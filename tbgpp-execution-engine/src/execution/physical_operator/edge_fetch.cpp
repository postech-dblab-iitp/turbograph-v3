
#include "typedef.hpp"

#include "execution/physical_operator/edge_fetch.hpp"

#include <string>

namespace duckdb {

class EdgeFetchState : public OperatorState {
public:
	explicit EdgeFetchState() {}
public:
	ExtentIterator* ext_it; // TODO separate this
};

unique_ptr<OperatorState> EdgeFetch::GetOperatorState() const {
	return make_unique<EdgeFetchState>();
}

OperatorResultType EdgeFetch::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	// std::cout << "Start Edge Fetch\n";
	auto &state = (EdgeFetchState &)lstate;

	auto itbgpp_graph = (iTbgppGraphStore*)graph;
	
	int edgeColIdx = schema.getColIdxOfKey( name ); // idx 
	// std::cout << "A" << std::endl;
	DataChunk targetTupleChunk;
	auto targetTypes = schema.getTypesOfKey( name );
	targetTupleChunk.Initialize(targetTypes);
	std::cout << "B" << std::endl;
	std::vector<LabelSet> empty_els;
	
	int numProducedTuples = 0;
	// for new ones call api 
	for( u_int64_t srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		// fetch value
		uint64_t eid = UBigIntValue::Get(input.GetValue(edgeColIdx, srcIdx));
		// std::cout << "C" << std::endl;
		// pass value
		itbgpp_graph->doEdgeIndexSeek(state.ext_it, targetTupleChunk, eid, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes); 
		assert( targetTupleChunk.size() == 1 && "did not fetch well");
		
		// set value
		// std::cout << "D" << std::endl;
		for (idx_t colId = 0; colId < targetTupleChunk.ColumnCount(); colId++) {
			chunk.SetValue(colId+edgeColIdx, numProducedTuples, targetTupleChunk.GetValue(colId, 0) );
		}
		targetTupleChunk.Reset();
		numProducedTuples +=1;
	}
	// for original ones reference others
	// std::cout << "E" << std::endl;
	for(int i =0; i<edgeColIdx; i++) {
		chunk.data[i].Reference( input.data[i] );
	}
	// std::cout << "F" << std::endl;
	// 0 1 234
	// 0 12 345
	for(int i = edgeColIdx+targetTupleChunk.ColumnCount() ; i < chunk.ColumnCount() ; i++) {
		chunk.data[i].Reference( input.data[i-targetTupleChunk.ColumnCount()+1] );
	}

	chunk.SetCardinality( input.size() );
	// always return need_more_input
	// std::cout << "End EdgeFetch\n";
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string EdgeFetch::ParamsToString() const {
	return "edgefetch-param";
}

std::string EdgeFetch::ToString() const {
	return "EdgeFetch";
}

}