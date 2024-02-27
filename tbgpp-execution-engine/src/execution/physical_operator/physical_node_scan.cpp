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
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0)	// without pushdown, two mappings are exactly same
{
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(sch.getStoredTypes());
}

PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<LogicalType> scan_types_, vector<vector<uint64_t>> scan_projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0)
{ 
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(scan_types_);
}

PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<LogicalType> scan_types_, vector<vector<uint64_t>> scan_projection_mapping, 
	int64_t filterKeyIndex, duckdb::Value filterValue) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0),
		filter_pushdown_type(FilterPushdownType::FP_EQ)
{ 
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(scan_types_);
	filter_pushdown_key_idxs.push_back(filterKeyIndex);
	filter_pushdown_values.push_back(filterValue);
}

PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<LogicalType> scan_types_, vector<vector<uint64_t>> scan_projection_mapping,
	int64_t filterKeyIndex, duckdb::Value l_filterValue,  duckdb::Value r_filterValue, bool l_inclusive, bool r_inclusive) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), filter_pushdown_type(FilterPushdownType::FP_RANGE),
		range_filter_pushdown_value{l_filterValue, r_filterValue, l_inclusive, r_inclusive}
{ 
	num_schemas = 1;
	scan_types.resize(num_schemas);
	scan_types[0] = std::move(scan_types_);
	filter_pushdown_key_idxs.push_back(filterKeyIndex);
	range_filter_pushdown_values.push_back(range_filter_pushdown_value);
}

	
PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue)
	: PhysicalNodeScan(sch, oids, projection_mapping, sch.getStoredTypes(), projection_mapping, filterKeyIndex, filterValue) { }

PhysicalNodeScan::PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<vector<uint64_t>> scan_projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0)	// without pushdown, two mappings are exactly same
{
	num_schemas = sch.size();
	scan_types.resize(num_schemas);
	for (auto i = 0; i < num_schemas; i++) {
		scan_types[i] = std::move(sch[i].getStoredTypes());
	}
}

/* Schemaless Equality Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<vector<uint64_t>> scan_projection_mapping, vector<int64_t>& filterKeyIndexes, vector<duckdb::Value>& filterValues) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), filter_pushdown_type(FilterPushdownType::FP_EQ),
		filter_pushdown_key_idxs(filterKeyIndexes), filter_pushdown_values(filterValues) 
{
	num_schemas = sch.size();
	scan_types.resize(num_schemas);
	for (auto i = 0; i < num_schemas; i++) {
		scan_types[i] = std::move(sch[i].getStoredTypes());
	}
}

/* Schemaless Range Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<vector<uint64_t>> scan_projection_mapping, vector<int64_t>& filterKeyIndexes, vector<RangeFilterValue>& rangeFilterValues) :
	CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema, sch), oids(oids), projection_mapping(projection_mapping),
	scan_projection_mapping(scan_projection_mapping), current_schema_idx(0), filter_pushdown_type(FilterPushdownType::FP_RANGE),
	filter_pushdown_key_idxs(filterKeyIndexes), range_filter_pushdown_values(rangeFilterValues)
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

	StoreAPIResult res;
	if (filter_pushdown_key_idxs.empty()) {
		// no filter pushdown
		if (projection_mapping.size() == 1) {
			res = context.client->graph_store->doScan(state.ext_its, chunk, types);
		} else {
			res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, current_schema_idx, false);
		}
	} else {
        /* TODO @jhha - Even if the minmax array does not exist,
		 * if the number of predicate application results is 0, 
		 * continue scanning the next area. 
		 */
        // filter pushdown applied
        if (filter_pushdown_type == FilterPushdownType::FP_RANGE) {
			res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, current_schema_idx,
													filter_pushdown_key_idxs[current_schema_idx], range_filter_pushdown_values[current_schema_idx]);
		} else {
			res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, current_schema_idx,
													filter_pushdown_key_idxs[current_schema_idx], filter_pushdown_values[current_schema_idx]);
		}
	}
	
	// current_schema_idx = 0; // TODO temporary logic!
	
	if (res == StoreAPIResult::DONE) {
#ifdef DEBUG_PRINT_PIPELINE
		printf("current_schema_idx = %ld, num_schemas = %ld\n", current_schema_idx, num_schemas);
#endif
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
	string params = "nodescan-params: oids {";
	for (auto i = 0; i < oids.size(); i++) {
		params += std::to_string(oids[i]);
		if (i < oids.size() - 1) {
			params += ", ";
		}
	}
	params += "}";
	return params;
}

std::string PhysicalNodeScan::ToString() const {
	return "NodeScan";
}
}