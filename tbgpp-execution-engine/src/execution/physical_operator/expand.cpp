#include "typedef.hpp"
#include "execution/physical_operator/expand.hpp"
#include <string>

#include <cassert>

namespace duckdb {

class ExpandState : public OperatorState {
public:
	explicit ExpandState() {
		pointToStartSeek.first = 0;
		pointToStartSeek.second = 0;
	}
public:
	std::pair<int,int> pointToStartSeek;
	ExtentIterator *ext_it;
};

unique_ptr<OperatorState> Expand::GetOperatorState() const {
	return make_unique<ExpandState>();
}

OperatorResultType Expand::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (ExpandState &)lstate;

	// TODO change when using different storage.
	auto itbgpp_graph = (iTbgppGraphStore*)graph;
	
	// check directionality and access edgelist
	int nodeColIdx = schema.getNodeColIdx( srcName ); // idx 
	vector<LogicalType> input_datachunk_types = move(input.GetTypes());
	vector<int> adjColIdxs;
	for (int i = 0; i < input_datachunk_types.size(); i++) {
		if (input_datachunk_types[i] == LogicalType::ADJLIST) adjColIdxs.push_back(i);
	}
	D_ASSERT(adjColIdxs.size() == 1); // TODO temporary
	//int adjColIdx = nodeColIdx + 1; // TODO assumption: adj col id = node id col id + 1

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
	idx_t *adjListBase = (idx_t *)input.data[adjColIdxs[0]].GetData();
	fprintf(stdout, "%s\n", input.ToString(2).c_str());
	for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		// TODO need to fix when traversing "BOTH"
		idx_t start_offset = srcIdx == 0 ? STANDARD_VECTOR_SIZE : adjListBase[srcIdx - 1];
		idx_t end_offset = adjListBase[srcIdx];
		D_ASSERT(end_offset >= start_offset);
		
		//std::vector<duckdb::Value> adjList = duckdb::ListValue::GetChildren(input.GetValue(adjColIdx, srcIdx)); // TODO
		//int numAdjs = adjList.size();
		idx_t *adjList = adjListBase + start_offset;
		int numAdjs = end_offset - start_offset;
		//std::cout << "srcidx " << srcIdx << " numADj " << numAdjs << std::endl;
		for( adjIdx = 0 ; adjIdx < numAdjs; adjIdx++ ){
			// check have more output
			if( numProducedTuples == STANDARD_VECTOR_SIZE ) {
				// output full, but we have more output.
				// when current output size is exactly STANDARD_VECTOR_SIZE, this area is never accessed.
				isHaveMoreOutput = true;
				goto breakLoop;
			}
			// bypass until reaching pointToSeek;
			if( !isSeekPointReached ) {
				// once reached, this block is never accessed.
				isSeekPointReached = (state.pointToStartSeek.first <= srcIdx) && (state.pointToStartSeek.second <= adjIdx);
				if( !isSeekPointReached ) continue;
			}
			uint64_t tgtId = adjList[adjIdx];
			// duckdb::Value v = adjList[adjIdx];
			// uint64_t tgtId = duckdb::UBigIntValue::Get(v) ;
			// check target labelset predicate
			if( ! itbgpp_graph->isNodeInLabelset(tgtId, tgtLabelSet) ) { continue; } // TODO
			
			// now, produce
			// add source 
				// TODO note that src can also not be loaded when trying to exppand
			for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, srcIdx) );
			}
			// add target
				// TODO also rhs can not be loaded. based on the operator parameter.
			itbgpp_graph->doIndexSeek(state.ext_it, targetTupleChunk, tgtId, tgtLabelSet, tgtEdgeLabelSets, tgtLoadAdjOpt, tgtPropertyKeys, targetTypes); // TODO
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

std::string Expand::ToString() const {
	return "Expand";
}
}