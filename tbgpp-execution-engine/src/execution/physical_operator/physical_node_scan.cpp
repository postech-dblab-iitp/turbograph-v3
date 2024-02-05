#include "execution/physical_operator/physical_node_scan.hpp"
//#include "common/types/chunk_collection.hpp"

#include "storage/graph_store.hpp"
#include "extent/extent_iterator.hpp"

#include "planner/expression.hpp"
#include "icecream.hpp"

#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"

#include <cassert>
#include <queue>

namespace duckdb {
	
class NodeScanState : public LocalSourceState {
public:
	explicit NodeScanState() {
		iter_inited = false;
	}
public:
	bool iter_inited;
	bool iter_finished;
	std::queue<ExtentIterator *> ext_its;
	// TODO use for vectorized processing
	DataChunk extent_cache;
};

PhysicalNodeScan::PhysicalNodeScan(Schema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
								vector<vector<uint64_t>> scan_projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), filter_pushdown_key_idx(-1)	// without pushdown, two mappings are exactly same
{
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(sch.getStoredTypes());
	D_ASSERT(filter_pushdown_key_idx < 0);
}

PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<LogicalType> scan_types_, vector<vector<uint64_t>> scan_projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), filter_pushdown_key_idx(-1)
{ 
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(scan_types_);
	D_ASSERT(filter_pushdown_key_idx < 0);
}

PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<LogicalType> scan_types_, vector<vector<uint64_t>> scan_projection_mapping, 
	int64_t filterKeyIndex, duckdb::Value filterValue) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0),
		filter_pushdown_key_idx(filterKeyIndex), filter_pushdown_value(filterValue)
{ 
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(scan_types_);
	D_ASSERT(filter_pushdown_key_idx >= 0);
}
	
// TODO delete me!
PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue)
	: PhysicalNodeScan(sch, oids, projection_mapping, sch.getStoredTypes(), projection_mapping, filterKeyIndex, filterValue) { }

PhysicalNodeScan::PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<vector<uint64_t>> scan_projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), filter_pushdown_key_idx(-1)	// without pushdown, two mappings are exactly same
{
	num_schemas = sch.size();
	scan_types.resize(num_schemas);
	for (auto i = 0; i < num_schemas; i++) {
		scan_types[i] = std::move(sch[i].getStoredTypes());
	}
	D_ASSERT(filter_pushdown_key_idx < 0);
}

PhysicalNodeScan::PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<vector<uint64_t>> scan_projection_mapping, vector<int64_t>& filterKeyIndexes, vector<duckdb::Value>& filterValues) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), 
		filter_pushdown_key_idxs(filterKeyIndexes), filter_pushdown_values(filterValues) 	// without pushdown, two mappings are exactly same
{
	num_schemas = sch.size();
	scan_types.resize(num_schemas);
	for (auto i = 0; i < num_schemas; i++) {
		scan_types[i] = std::move(sch[i].getStoredTypes());
	}
}

PhysicalNodeScan::~PhysicalNodeScan() {}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalNodeScan::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<NodeScanState>();
}

void PhysicalNodeScan::GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;
	// If first time here, call doScan and get iterator from iTbgppGraphStore
	if (!state.iter_inited) {
		state.iter_inited = true;

		auto initializeAPIResult =
			context.client->graph_store->InitializeScan(state.ext_its, oids, scan_projection_mapping, scan_types);
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK);
	}
	D_ASSERT(state.ext_its.size() > 0);
	idx_t j = 0;
	// for (auto i = 0; i < chunk.ColumnCount(); i++) { // TODO
	// 	if (projection_mapping[current_schema_idx][j] == i) {
	// 		j++;
	// 	} else {
	// 		auto &validity = FlatVector::Validity(chunk.data[i]);
	// 		validity.EnsureWritable(STANDARD_VECTOR_SIZE);
	// 		validity.SetAllInvalid(STANDARD_VECTOR_SIZE);
	// 	}
	// }

	StoreAPIResult res;
	if (filter_pushdown_key_idx < 0 && filter_pushdown_key_idxs.empty()) {
		// no filter pushdown
		if (projection_mapping.size() == 1) {
			res = context.client->graph_store->doScan(state.ext_its, chunk, types);
		} else {
			res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, current_schema_idx, false);
		}
	} else {
		// filter pushdown applied
		res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, current_schema_idx,
												filter_pushdown_key_idxs[current_schema_idx], filter_pushdown_values[current_schema_idx]);
	}
	
	// current_schema_idx = 0; // TODO temporary logic!
	
	if (res == StoreAPIResult::DONE) {
		printf("current_schema_idx = %ld, num_schemas = %ld\n", current_schema_idx, num_schemas);
		current_schema_idx++;
		state.iter_finished = true;
		return;
		// if (++current_schema_idx == num_schemas) return;
		// idx_t j = 0;
		// for (auto i = 0; i < chunk.ColumnCount(); i++) {
		// 	if (projection_mapping[current_schema_idx][j] == i) {
		// 		j++;
		// 	} else {
		// 		auto &validity = FlatVector::Validity(chunk.data[i]);
		// 		validity.EnsureWritable(STANDARD_VECTOR_SIZE);
		// 		validity.SetAllInvalid(STANDARD_VECTOR_SIZE);
		// 	}
		// }
		// chunk.Destroy();
		// chunk.Initialize(scan_types[current_schema_idx]);
		// GetData(context, chunk, lstate);
		// return;
	} else {
		state.iter_finished = false;
	}

	chunk.SetSchemaIdx(current_schema_idx);
	
	/* GetData() should return empty chunk to indicate scan is finished. */
}

bool PhysicalNodeScan::IsSourceDataRemaining(LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;
	return !state.iter_finished;
}

std::string PhysicalNodeScan::ParamsToString() const {
	return "nodescan-params";
}

std::string PhysicalNodeScan::ToString() const {
	return "NodeScan";
}
}