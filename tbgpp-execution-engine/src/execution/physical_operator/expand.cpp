#include "typedef.hpp"
#include "execution/physical_operator/expand.hpp"
#include <string>

#include <cassert>

using namespace duckdb;
using namespace std;

class ExpandState : public OperatorState {
public:
	explicit ExpandState() {
		pointToStartSeek.first = 0;
		pointToStartSeek.second = 0;
	}
public:
	std::pair<int,int> pointToStartSeek;
};

unique_ptr<OperatorState> Expand::GetOperatorState() const {
	return make_unique<ExpandState>();
}

OperatorResultType Expand::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (ExpandState &)lstate;

	// TODO change when using different storage.
	auto livegraph = (LiveGraphStore*)graph; 
	
	// check directionality and access edgelist
	int nodeColIdx = schema.getNodeColIdx( srcName ); // idx 
	int adjColIdx = nodeColIdx + 1;

	// target tuple chunk
	auto targetTypes = schema.getNodeTypes( std::get<0>(schema.attrs.back()) );
	bool fetchTarget = targetTypes.size() != 0;
	DataChunk targetTupleChunk;
	if( fetchTarget ) {
		targetTupleChunk.Initialize(targetTypes);

	}

	int numProducedTuples = 0;
	bool isHaveMoreOutput = false;
	bool isSeekPointReached = false;
	int srcIdx; int adjIdx;
	for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		// TODO need to fix when traversing "BOTH"
		std::vector<duckdb::Value> adjList = duckdb::ListValue::GetChildren(input.GetValue(adjColIdx, srcIdx));
		int numAdjs = adjList.size();
		//std::cout << "srcidx " << srcIdx << " numADj " << numAdjs << std::endl;
		for( adjIdx = 0 ; adjIdx < numAdjs; adjIdx++ ){
			// check have more output
			if( numProducedTuples == 1024 ) {
				// output full, but we have more output.
				// when current output size is exactly 1024, this area is never accessed.
				isHaveMoreOutput = true;
				goto breakLoop;
			}
			// bypass until reaching pointToSeek;
			if( !isSeekPointReached ) {
				// once reached, this block is never accessed.
				isSeekPointReached = (state.pointToStartSeek.first <= srcIdx) && (state.pointToStartSeek.second <= adjIdx);
				if( !isSeekPointReached ) continue;
			}
			duckdb::Value v = adjList[adjIdx];
			uint64_t tgtId = duckdb::UBigIntValue::Get(v) ;
			// check target labelset predicate
			if( ! livegraph->isNodeInLabelset(tgtId, tgtLabelSet) ) { continue; }
			
			// now, produce
			// add source 
				// TODO note that src can also not be loaded when trying to exppand
			for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, srcIdx) );
			}
			// add target
				// TODO also rhs can not be loaded. based on the operator parameter.
			livegraph->doIndexSeek(targetTupleChunk, tgtId, tgtLabelSet, tgtEdgeLabelSets, tgtLoadAdjOpt, tgtPropertyKeys, targetTypes);
			assert( targetTupleChunk.size() == 1 && "did not fetch well");

			int columnOffset = input.ColumnCount();
			for (idx_t colId = 0; colId < targetTupleChunk.ColumnCount(); colId++) {
				chunk.SetValue(colId+columnOffset, numProducedTuples, targetTupleChunk.GetValue(colId, 0) );
			}
			// add edge I presume? ; TODO// not available since no edge id given.

			// post-produce
			numProducedTuples +=1;
			targetTupleChunk.Reset();
		}
	}
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

std::string Expand::ParamsToString() const {
	return "expand-params-TODO";
}