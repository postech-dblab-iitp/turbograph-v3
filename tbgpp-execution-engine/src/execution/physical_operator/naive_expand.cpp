#include "typedef.hpp"
#include "execution/physical_operator/naive_expand.hpp"

// TODO not good import ...  adjlistiterator should not be inited here
#include "extent/extent_iterator.hpp"
#include <string>

#include <cassert>

namespace duckdb {

class NaiveExpandState : public OperatorState {
public:
	explicit NaiveExpandState() {
		pointToStartSeek.first = 0;
		pointToStartSeek.second = 0;

		adj_it = new AdjacencyListIterator();
	}
public:
	std::pair<u_int64_t,u_int64_t> pointToStartSeek;
	AdjacencyListIterator *adj_it;
	ExtentIterator* ext_it; // TODO separate this
};

unique_ptr<OperatorState> NaiveExpand::GetOperatorState() const {
	return make_unique<NaiveExpandState>();
}

OperatorResultType NaiveExpand::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	// std::cout << "Start Expand\n";
	auto &state = (NaiveExpandState &)lstate;
	auto itbgpp_graph = (iTbgppGraphStore*)graph;
	
	// check directionality and access edgelist
	int nodeColIdx = schema.getColIdxOfKey( srcName ); // idx
	
	vector<LogicalType> input_datachunk_types = move(input.GetTypes());

	// target tuple chunk
	auto targetTypes = schema.getTypesOfKey( std::get<0>(schema.attrs.back()) );
	bool fetchTarget = targetTypes.size() != 0;
	DataChunk targetTupleChunk;
	if( fetchTarget ) {
		targetTupleChunk.Initialize(targetTypes);
	}

	// progress state variables
	int numProducedTuples = 0;
	bool isHaveMoreOutput = false;
	bool isSeekPointReached = false;
	u_int64_t srcIdx, adjPtr;
	uint64_t* adj_start;
	uint64_t* adj_end;

	// std::cout << input.ToString(10) << std::endl;
	vector<int> adjColIdxs;
	itbgpp_graph->getAdjColIdxs(srcLabelSet, adjColIdxs);

	uint64_t *src_column = (uint64_t *)input.data[nodeColIdx].GetData();
	if( edgeName.compare("") == 1 ) { // TODO seprate this to two operators
		for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
			uint64_t vid = src_column[srcIdx];
			// uint64_t vid = duckdb::UBigIntValue::Get(input.GetValue(nodeColIdx, srcIdx));
			
			itbgpp_graph->getAdjListFromVid(*state.adj_it, adjColIdxs[0], vid, adj_start, adj_end );
			//std::cout << "pass adjlist call" << std::endl;

			// std::cout << adj_end - adj_start << std::endl;
			size_t adjListSize = adj_end - adj_start;
			
			for (idx_t adj_idx = 0; adj_idx < adjListSize; adj_idx+=2) {
				//std::cout << "val" << std::endl;
				uint64_t tgtId = adj_start[adj_idx];
				uint64_t edgeId = adj_start[adj_idx+1];

				if( numProducedTuples == EXEC_ENGINE_VECTOR_SIZE ) {
					// output full, but we have more output.
					// when current output size is exactly EXEC_ENGINE_VECTOR_SIZE, this area is never accessed.
					isHaveMoreOutput = true;
					goto breakLoop;
				}
				// bypass until reaching pointToSeek;
				if( !isSeekPointReached ) {
					// once reached, this block is never accessed.
					isSeekPointReached = (state.pointToStartSeek.first <= srcIdx) && (state.pointToStartSeek.second <= adjPtr);
					if( !isSeekPointReached ) continue;
				}
				// produce
				//std::cout << input.ToString(0) << std::endl;
				//std::cout << chunk.ToString(0) << std::endl;
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					//std::cout << colId <<   std::endl;
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, srcIdx) );
				}
				// TODO optionally add edge id
				chunk.SetValue(input.ColumnCount(), numProducedTuples, Value::UBIGINT(edgeId) );

				// fetch
				// call API
				// FIXME write here
				itbgpp_graph->doIndexSeek(state.ext_it, targetTupleChunk, tgtId, tgtLabelSet, tgtEdgeLabelSets, tgtLoadAdjOpt, tgtPropertyKeys, targetTypes); // TODO
				assert( targetTupleChunk.size() == 1 && "did not fetch well");
				int columnOffset = input.ColumnCount();
				columnOffset+=1;
				
				for (idx_t colId = 0; colId < targetTupleChunk.ColumnCount(); colId++) {
					chunk.SetValue(colId+columnOffset, numProducedTuples, targetTupleChunk.GetValue(colId, 0) );
				}
				
				// post-produce
				// TODO here
				numProducedTuples +=1;
				targetTupleChunk.Reset();
			}
		}
	} else {
		for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
			uint64_t vid = src_column[srcIdx];
			// uint64_t vid = duckdb::UBigIntValue::Get(input.GetValue(nodeColIdx, srcIdx));
			
			itbgpp_graph->getAdjListFromVid(*state.adj_it, adjColIdxs[0], vid, adj_start, adj_end );
			//std::cout << "pass adjlist call" << std::endl;

			// std::cout << adj_end - adj_start << std::endl;
			size_t adjListSize = adj_end - adj_start;
			
			for (idx_t adj_idx = 0; adj_idx < adjListSize; adj_idx+=2) {
				//std::cout << "val" << std::endl;
				uint64_t tgtId = adj_start[adj_idx];

				if( numProducedTuples == EXEC_ENGINE_VECTOR_SIZE ) {
					// output full, but we have more output.
					// when current output size is exactly EXEC_ENGINE_VECTOR_SIZE, this area is never accessed.
					isHaveMoreOutput = true;
					goto breakLoop;
				}
				// bypass until reaching pointToSeek;
				if( !isSeekPointReached ) {
					// once reached, this block is never accessed.
					isSeekPointReached = (state.pointToStartSeek.first <= srcIdx) && (state.pointToStartSeek.second <= adjPtr);
					if( !isSeekPointReached ) continue;
				}
				// produce
				//std::cout << input.ToString(0) << std::endl;
				//std::cout << chunk.ToString(0) << std::endl;
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					//std::cout << colId <<   std::endl;
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, srcIdx) );
				}

				// fetch
				// call API
				// FIXME write here
				itbgpp_graph->doIndexSeek(state.ext_it, targetTupleChunk, tgtId, tgtLabelSet, tgtEdgeLabelSets, tgtLoadAdjOpt, tgtPropertyKeys, targetTypes); // TODO
				assert( targetTupleChunk.size() == 1 && "did not fetch well");
				int columnOffset = input.ColumnCount();

				for (idx_t colId = 0; colId < targetTupleChunk.ColumnCount(); colId++) {
					chunk.SetValue(colId+columnOffset, numProducedTuples, targetTupleChunk.GetValue(colId, 0) );
				}
				
				// post-produce
				// TODO here
				numProducedTuples +=1;
				targetTupleChunk.Reset();
			}
		}
	}


breakLoop:
	// postprocess
	// std::cout << "End Expand\n";
	// set chunk cardinality
	chunk.SetCardinality(numProducedTuples);
	if( isHaveMoreOutput ) {
		// save state
		state.pointToStartSeek.first = srcIdx;
		state.pointToStartSeek.second = adjPtr;
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
	return "AdjIdxJoinNaive-params-TODO";
}

std::string NaiveExpand::ToString() const {
	return "AdjIdxJoinNaive";
}

}
