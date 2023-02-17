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
		CypherPhysicalOperator(sch), oids(oids), projection_mapping(projection_mapping), filter_pushdown_key("") { }

// TODO need to revive nodescan with pushdown

PhysicalNodeScan::~PhysicalNodeScan() {}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalNodeScan::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<NodeScanState>();
}

// StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> projection_mapping, std::vector<duckdb::LogicalType> scanSchema);

void PhysicalNodeScan::GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;
	// If first time here, call doScan and get iterator from iTbgppGraphStore
	if (!state.iter_inited) {
		state.iter_inited = true;

// TODO revive keys for filter pushdown
		// if( filter_pushdown_key.compare("") != 0 ) {
		// 	// check to add filterkey to access
		// 	bool isFilterKeyFound = false;
		// 	for( auto& key: propertyKeys ) {
		// 		if( key.compare(filter_pushdown_key) == 0 ) { isFilterKeyFound = true; break; }
		// 	}
		// 	// now add pushdown key to access keys
		// 	if( ! isFilterKeyFound ) {
		// 		access_property_keys.push_back( filter_pushdown_key );
		// 		access_schema.push_back( filter_pushdown_value.type() );
		// 	}
		// }

		auto initializeAPIResult =
			context.client->graph_store->InitializeScan(state.ext_its, oids, projection_mapping, types);
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK); 

	}
	D_ASSERT(state.ext_its.size() > 0);

	if( filter_pushdown_key.compare("") == 0 ) {
		context.client->graph_store->doScan(state.ext_its, chunk, projection_mapping, types);
	} else {
		D_ASSERT(false);
		// currently does not support filter pushdown
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