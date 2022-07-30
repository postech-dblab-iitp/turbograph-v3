#include "typedef.hpp"
#include "execution/physical_operator/naive_expand.hpp"
#include <string>

#include <cassert>

namespace duckdb {

class NaiveExpandState : public OperatorState {
public:
	explicit NaiveExpandState() {
		pointToStartSeek.first = 0;
		pointToStartSeek.second = 0;
	}
public:
	std::pair<int,int> pointToStartSeek;
	ExtentIterator *ext_it;
};

unique_ptr<OperatorState> NaiveExpand::GetOperatorState() const {
	return make_unique<NaiveExpandState>();
}

OperatorResultType NaiveExpand::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (NaiveExpandState &)lstate;

	auto itbgpp_graph = (iTbgppGraphStore*)graph;
	
	// check directionality and access edgelist
	int nodeColIdx = schema.getNodeColIdx( srcName ); // idx 
	vector<LogicalType> input_datachunk_types = move(input.GetTypes());

	// target tuple chunk
	auto targetTypes = schema.getNodeTypes( std::get<0>(schema.attrs.back()) );
	bool fetchTarget = targetTypes.size() != 0;
	DataChunk targetTupleChunk;
	if( fetchTarget ) {
		targetTupleChunk.Initialize(targetTypes);
	}

	// progress state variables
	int numProducedTuples = 0;
	bool isHaveMoreOutput = false;
	bool isSeekPointReached = false;
	int srcIdx, adjIdx;

	// for( int srcIdx=0 ; srcIdx < input.size(); srcIdx++) {

	// 	// TODO get adjlist by calling API

	// 	// FIXME here
	// 	for( int adjIdx=0; adjID ) {
	// 		if( numProducedTuples == STANDARD_VECTOR_SIZE ) {
	// 			// output full, but we have more output.
	// 			// when current output size is exactly STANDARD_VECTOR_SIZE, this area is never accessed.
	// 			isHaveMoreOutput = true;
	// 			goto breakLoop;
	// 		}
	// 		// bypass until reaching pointToSeek;
	// 		if( !isSeekPointReached ) {
	// 			// once reached, this block is never accessed.
	// 			isSeekPointReached = (state.pointToStartSeek.first <= srcIdx) && (state.pointToStartSeek.second <= adjIdx);
	// 			if( !isSeekPointReached ) continue;
	// 		}
	// 		// produce
	// 		for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
	// 			chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, srcIdx) );
	// 		}
	// 		// fetch
	// 		// call API
	// 		// FIXME write here
	// 		itbgpp_graph->doIndexSeek(state.ext_it, targetTupleChunk, tgtId, tgtLabelSet, tgtEdgeLabelSets, tgtLoadAdjOpt, tgtPropertyKeys, targetTypes); // TODO
	// 		assert( targetTupleChunk.size() == 1 && "did not fetch well");
	// 		int columnOffset = input.ColumnCount();
	// 		for (idx_t colId = 0; colId < targetTupleChunk.ColumnCount(); colId++) {
	// 			chunk.SetValue(colId+columnOffset, numProducedTuples, targetTupleChunk.GetValue(colId, 0) );
	// 		}
			
	// 		// post-produce
	// 		numProducedTuples +=1;
	// 		targetTupleChunk.Reset();
	// 	}
	// }


breakLoop:
	// postprocess
	
	// set chunk cardinality
	chunk.SetCardinality(numProducedTuples);
	if( isHaveMoreOutput ) {
		// save state
		state.pointToStartSeek.first = srcIdx;
		state.pointToStartSeek.second = adjIdx;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	else {
		// re-initialize
		state.pointToStartSeek.first = 0;
		state.pointToStartSeek.second = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

}

std::string NaiveExpand::ParamsToString() const {
	return "NaiveExpand-params-TODO";
}

std::string NaiveExpand::ToString() const {
	return "NaiveExpand";
}

}
