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
	std::queue<ExtentIterator *> ext_its;
	// TODO use for vectorized processing
	DataChunk extent_cache;
};

PhysicalNodeScan::PhysicalNodeScan(Schema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(projection_mapping), current_schema_idx(0), filter_pushdown_key_idx(-1)	// without pushdown, two mappings are exactly same
{
	num_schemas = 1;
	scan_types.resize(num_schemas);
	std::copy(sch.getStoredTypes().begin(), sch.getStoredTypes().end(), scan_types[0].begin());
	D_ASSERT(filter_pushdown_key_idx < 0);
}

PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	vector<LogicalType> scan_types_, vector<vector<uint64_t>> scan_projection_mapping, 
	int64_t filterKeyIndex, duckdb::Value filterValue) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(scan_projection_mapping), curcurrent_schema_idxrent_schema(0),
		filter_pushdown_key_idx(filterKeyIndex), filter_pushdown_value(filterValue)
{ 
	num_schemas = 1;
	scan_types.resize(num_schemas);
	std::copy(sch.getStoredTypes().begin(), sch.getStoredTypes().end(), scan_types[0].begin());
	D_ASSERT(filter_pushdown_key_idx >= 0);
}
	
// TODO delete me!
PhysicalNodeScan::PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue)
	: PhysicalNodeScan(sch, oids, projection_mapping, sch.getStoredTypes(), projection_mapping, filterKeyIndex, filterValue) { }

PhysicalNodeScan::PhysicalNodeScan(vector<Schema> &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_projection_mapping(projection_mapping), current_schema_idx(0), filter_pushdown_key_idx(-1)	// without pushdown, two mappings are exactly same
{
	num_schemas = sch.size();
	scan_types.resize(num_schemas);
	for (auto i = 0; i < num_schemas; i++) {
		std::copy(sch[i].getStoredTypes().begin(), sch[i].getStoredTypes().end(), scan_types[i].begin());
	}
	D_ASSERT(filter_pushdown_key_idx < 0);
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
			context.client->graph_store->InitializeScan(state.ext_its, oids, scan_projection_mapping, scan_types[current_schema_idx]);
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK); 

	}
	D_ASSERT(state.ext_its.size() > 0);

	StoreAPIResult res;
	if( filter_pushdown_key_idx < 0 ) {
		// no filter pushdown
		res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types);
	} else {
		// filter pushdown applied
		res = context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, filter_pushdown_key_idx, filter_pushdown_value);
	}

	if (res == StoreAPIResult::DONE) {
		if (++current_schema_idx == num_schemas) return;
		GetData(context, chunk, lstate);
	}
	/* GetData() should return empty chunk to indicate scan is finished. */
}

std::string PhysicalNodeScan::ParamsToString() const {
	return "nodescan-params";
}

std::string PhysicalNodeScan::ToString() const {
	return "NodeScan";
}
}