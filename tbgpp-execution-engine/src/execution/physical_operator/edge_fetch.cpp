
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
	auto &state = (EdgeFetchState &)lstate;

	auto itbgpp_graph = (iTbgppGraphStore*)graph;
	
	int edgeColIdx = schema.getColIdxOfKey( name ); // idx 
	DataChunk targetTupleChunk;
	auto targetTypes = schema.getTypesOfKey( name );
	targetTupleChunk.Initialize(targetTypes);
	std::vector<LabelSet> empty_els;
	
	int numProducedTuples = 0;
	// for new ones call api 
	for( u_int64_t srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		// fetch value
		uint64_t eid = UBigIntValue::Get(input.GetValue(edgeColIdx, srcIdx));
		// pass value
		itbgpp_graph->doIndexSeek(state.ext_it, targetTupleChunk, eid, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes); 
		assert( targetTupleChunk.size() == 1 && "did not fetch well");
		
		// set value
		for (idx_t colId = 0; colId < targetTupleChunk.ColumnCount(); colId++) {
			chunk.SetValue(colId+edgeColIdx, numProducedTuples, targetTupleChunk.GetValue(colId, 0) );
		}
		targetTupleChunk.Reset();
		numProducedTuples +=1;
	}
	// for original ones reference others
	for(int i =0; i<edgeColIdx; i++) {
		chunk.data[i].Reference( input.data[i] );
	}
	for(int i = edgeColIdx+targetTupleChunk.size() ; i < chunk.size() ; i++) {
		chunk.data[i].Reference( input.data[i-targetTupleChunk.size()] );
	}

	chunk.SetCardinality( input.size() );
	// always return need_more_input
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string EdgeFetch::ParamsToString() const {
	return "edgefetch-param";
}

std::string EdgeFetch::ToString() const {
	return "EdgeFetch";
}

}