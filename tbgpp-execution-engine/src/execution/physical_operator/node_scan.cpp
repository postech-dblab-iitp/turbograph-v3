#include "execution/physical_operator/node_scan.hpp"
//#include "common/types/chunk_collection.hpp"

#include "storage/graph_store.hpp"
#include "extent/extent_iterator.hpp"

#include <cassert>
namespace duckdb {
class NodeScanState : public LocalSourceState {
public:
	explicit NodeScanState() {
		is_it_initialized = true;
	}
	
public:
	bool is_it_initialized;
	ExtentIterator *ext_it;

	// TODO use for vectorized processing
	DataChunk extent_cache;
};

unique_ptr<LocalSourceState> NodeScan::GetLocalSourceState() const {
	return make_unique<NodeScanState>();
}

void NodeScan::GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;

	auto itbgpp_graph = (iTbgppGraphStore*)graph;

	// If first time here, call doScan and get iterator from iTbgppGraphStore
	if (state.is_it_initialized) {
		state.is_it_initialized = false;

		auto initializeAPIResult =
			itbgpp_graph->InitializeScan(state.ext_it, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes());
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK); 
	}
	D_ASSERT(state.ext_it != nullptr);

	// TODO need to split chunk in units of EXEC_ENGINE_VECTOR_SIZE
	if(filterKey.compare("") == 0 ){
		auto scanAPIResult =
		itbgpp_graph->doScan(state.ext_it, chunk, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes());
	} else {
		auto scanAPIResult =
		itbgpp_graph->doScan(state.ext_it, chunk, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes(), filterKey, filterValue);
	}
	// GetData() should return empty chunk to indicate scan is finished.
}

std::string NodeScan::ParamsToString() const {
	return "nodescan-params";
}

std::string NodeScan::ToString() const {
	return "NodeScan";
}
}