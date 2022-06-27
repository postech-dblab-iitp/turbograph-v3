#include "execution/physical_operator/node_scan.hpp"
//#include "common/types/chunk_collection.hpp"

#include "storage/graph_store.hpp"

#include <cassert>
namespace duckdb {
class NodeScanState : public LocalSourceState {
public:
	explicit NodeScanState() {
		chunkIdxToScan = -1;
		//chunks = duckdb::ChunkCollection();
	}
	
public:
	int chunkIdxToScan;
	//duckdb::ChunkCollection chunks; //XXX
};

unique_ptr<LocalSourceState> NodeScan::GetLocalSourceState() const {
	return make_unique<NodeScanState>();
}

void NodeScan::GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;

	// TODO change when using different storage.
	//auto livegraph = (LiveGraphStore*)graph;
	auto livegraph = graph;
	
	if( state.chunkIdxToScan == -1 ) {
		state.chunkIdxToScan +=1;
		//auto scanAPIResult =
		//	livegraph->doScan(state.chunks, labels, edgeLabelSet, loadAdjOpt, propertyKeys, schema.getTypes());

		//if( state.chunks.ChunkCount() == 0) { return; }	// return empty chunk
	}
	// search starts from idx 0
	//assert( state.chunks.ChunkCount() > 0 && "chunk empty");
	//if( state.chunkIdxToScan == state.chunks.ChunkCount()  ) { // no more chunk left
	//	return;
	//}
	//chunk.Reference(state.chunks.GetChunk(state.chunkIdxToScan));
	state.chunkIdxToScan += 1;
	
}

std::string NodeScan::ParamsToString() const {
	return "nodescan-param";
}

std::string NodeScan::ToString() const {
	return "NodeScan";
}
}