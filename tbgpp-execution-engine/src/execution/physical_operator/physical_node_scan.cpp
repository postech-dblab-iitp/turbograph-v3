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

PhysicalNodeScan::PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping), 
		scan_types(sch.getStoredTypes()), scan_projection_mapping(projection_mapping), filter_pushdown_key_idx(-1)	// without pushdown, two mappings are exactly same
		{ 
			D_ASSERT(filter_pushdown_key_idx < 0);
		}

PhysicalNodeScan::PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
	std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping, 
	int64_t filterKeyIndex, duckdb::Value filterValue) :
		CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch), oids(oids), projection_mapping(projection_mapping),
		scan_types(scan_types), scan_projection_mapping(scan_projection_mapping),
		filter_pushdown_key_idx(filterKeyIndex), filter_pushdown_value(filterValue)
		 { 
			D_ASSERT(filter_pushdown_key_idx >= 0);
		 }
	
// TODO delete me!
PhysicalNodeScan::PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue)
	: PhysicalNodeScan(sch, oids, projection_mapping, sch.getStoredTypes(), projection_mapping, filterKeyIndex, filterValue) { }

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

	if( filter_pushdown_key_idx < 0 ) {
		// no filter pushdown
		context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types);
	} else {
		// filter pushdown applied
		context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types, filter_pushdown_key_idx, filter_pushdown_value);
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