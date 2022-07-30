#include "execution/physical_operator/node_scan.hpp"
//#include "common/types/chunk_collection.hpp"

#include "storage/graph_store.hpp"
#include "extent/extent_iterator.hpp"

#include <cassert>
namespace duckdb {
class NodeScanState : public LocalSourceState {
public:
	explicit NodeScanState() {
		first_time_here = true;
	}
	
public:
	bool first_time_here;
	ExtentIterator *ext_it;
};

unique_ptr<LocalSourceState> NodeScan::GetLocalSourceState() const {
	return make_unique<NodeScanState>();
}

void NodeScan::GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;

	// TODO change when using different storage.
	auto itbgpp_graph = (iTbgppGraphStore*)graph;

	// If first time here, call doScan and get iterator from iTbgppGraphStore
	if (state.first_time_here) {
		state.first_time_here = false;
		//fprintf(stdout, "A\n");
		auto initializeAPIResult =
			itbgpp_graph->InitializeScan(state.ext_it, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes());
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK); // ??zz
		//fprintf(stdout, "B\n");
		auto scanAPIResult =
			itbgpp_graph->doScan(state.ext_it, chunk, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes());
		//fprintf(stdout, "C\n");
	} else {
		D_ASSERT(state.ext_it != nullptr);
		auto scanAPIResult =
			itbgpp_graph->doScan(state.ext_it, chunk, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes());
	}
}

std::string NodeScan::ParamsToString() const {
	return "nodescan-param";
}

std::string NodeScan::ToString() const {
	return "NodeScan";
}
}