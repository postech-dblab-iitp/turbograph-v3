
#include "typedef.hpp"

#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "extent/extent_iterator.hpp"

#include <string>
#include "icecream.hpp"


namespace duckdb{

class AdjIdxJoinState : public OperatorState {
public:
	explicit AdjIdxJoinState() {
		checkpoint.first = 0;
		checkpoint.second = 0;
		adj_it = new AdjacencyListIterator();
	}
public:

	// iterator state
		// first state on srcid
		// second state on adjacency
	std::pair<u_int64_t,u_int64_t> checkpoint;
	AdjacencyListIterator *adj_it;
	
	// TODO future get filter information for edge labelset and target labelset
};


unique_ptr<OperatorState> PhysicalAdjIdxJoin::GetOperatorState(ExecutionContext &context) const {

	return make_unique<AdjIdxJoinState>( );
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate;

	// init
	vector<LogicalType> input_datachunk_types = move(input.GetTypes());
	const idx_t srcColIdx = schema.getColIdxOfKey(srcName);	// first index of source node : where source node id is
	const idx_t edgeColIdx = input.ColumnCount();			// not using when load_eid = false
	const idx_t tgtColIdx = input.ColumnCount() + int(load_eid);	// first index of target node : where target node id should be OR start index of range

	// intra-chunk variables
	int numProducedTuples = 0;
	bool isHaveMoreOutput = false;
	bool isSeekPointReached = false;

	// get adjacency list columns in column
	vector<int> adjColIdxs;
	context.client->graph_store->getAdjColIdxs(srcLabelSet, adjColIdxs);
	D_ASSERT( adjColIdxs.size() > 0); // TODO need to remove this. what if srcLabelSet is all different?
	uint64_t* adj_start; uint64_t* adj_end;
IC();
	// fetch source column
	uint64_t *src_column = (uint64_t *)input.data[srcColIdx].GetData();
IC();
	// iterate source vids
	for( ; state.checkpoint.first < input.size() ; state.checkpoint.first++) {

		// check if output chunk full
		if( numProducedTuples == EXEC_ENGINE_VECTOR_SIZE ) { isHaveMoreOutput = true; goto breakLoop; }
IC();
		// check if vid is null
		if( state.checkpoint.second == 0 
			&&	FlatVector::IsNull(input.data[srcColIdx], state.checkpoint.first)
		) {
			if( join_type == JoinType::LEFT ) {
				// FIXME produce lsh join null
				// consider edge
				numProducedTuples +=1;
			} else { continue; }
		}
		// TODO actually there should be one more chunk full checking logic after producing 1 tuple in left join
IC();
		// vid is not null. now get source vid
		uint64_t vid = src_column[state.checkpoint.first];
IC(vid);
		context.client->graph_store->getAdjListFromVid(*state.adj_it, adjColIdxs[0], vid, adj_start, adj_end );
		size_t adjListSize = adj_end - adj_start;	
IC(adjListSize);
		size_t numTargets = adjListSize/2;			// adjListSize = 2 * target vertices
		// in anti/semijoin, the result is always smaller than input. Thus no overflow check required
		if ( join_type == JoinType::SEMI ) {
			if( adjListSize > 0 ) { 
				// FIXME produce lhs

				numProducedTuples +=1;
			 }
			continue;
		} else if (join_type == JoinType::ANTI ) {
			if( adjListSize == 0 ) { 
				// FIXME produce lhs
				numProducedTuples +=1;
			 }
			continue;
		} else if (join_type == JoinType::LEFT ) {
			if( adjListSize == 0 ) { 
				// FIXME produce lhs, null
				// consider edge
				numProducedTuples +=1;
			 }
			continue;
		}
IC();
		// TODO split range into subranges (apply filters)
			// TODO filter edge label
			// TODO filter target label

		// iterate subranges. currently TODO currently only one range
		if( enumerate ) {
			IC();
			// enumerate mode
				// choose smaller ones
			size_t numTuplesToProduce = ((EXEC_ENGINE_VECTOR_SIZE - numProducedTuples) > numTargets )
				? numTargets : (EXEC_ENGINE_VECTOR_SIZE - numProducedTuples);
			size_t finalCheckpoint = state.checkpoint.second + numTuplesToProduce;
			for( ; state.checkpoint.second < finalCheckpoint; state.checkpoint.second++ ) {	// TODO can parallelize this
				// produce lhs
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
				}
				// produce rhs : (edgeid), tid
				if( load_eid ) { chunk.SetValue(edgeColIdx, numProducedTuples, Value::UBIGINT( adj_start[state.checkpoint.second*2] )); }
				chunk.SetValue(tgtColIdx, numProducedTuples, Value::UBIGINT( adj_start[state.checkpoint.second*2 + 1] ));
				numProducedTuples +=1;
			}
			if( numTuplesToProduce == numTargets ) {
				// produce for this vertex done. init second
				state.checkpoint.second = 0;
			} else {
				isHaveMoreOutput = true;
				goto breakLoop;
			}
			IC();

		} else {
			// range mode
			D_ASSERT(false); // TODO needs logic recheck
			// call API ; reuse adj_start/adj_end
			context.client->graph_store->getAdjListRange(*state.adj_it, adjColIdxs[0], vid, adj_start, adj_end);
			// copy lhs
			for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
			}
			// copy rhs(range)
			chunk.SetValue(tgtColIdx, numProducedTuples, Value::UBIGINT(*adj_start));
			chunk.SetValue(tgtColIdx, numProducedTuples, Value::UBIGINT(*adj_end));
			numProducedTuples +=1;
		}
	}

breakLoop:
	// now produce finished. store state and exit
	D_ASSERT( numProducedTuples <= EXEC_ENGINE_VECTOR_SIZE );
	chunk.SetCardinality(numProducedTuples);
	if( isHaveMoreOutput ) {
		// state saved
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	else {
		// re-initialize
		state.checkpoint.first = 0;
		state.checkpoint.second = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

}

OperatorResultType PhysicalAdjIdxJoin::ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
	return OperatorResultType::FINISHED;
}

OperatorResultType PhysicalAdjIdxJoin::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {

	// check input type and run different function
	// TODO need logic change
		// if ranged input, unwrap range and provide per one vid.
	
	if( schema.getCypherType(srcName) == CypherValueType::RANGE ) {
		D_ASSERT( false && "currently not supporting when range is given as input");
		return ExecuteRangedInput(context, input, chunk, lstate);
	} else {
		return ExecuteNaiveInput(context, input, chunk, lstate);
	}
}

std::string PhysicalAdjIdxJoin::ParamsToString() const {
	return "AdjIdxJoin-params-TODO";
}

std::string PhysicalAdjIdxJoin::ToString() const {
	return "AdjIdxJoin";
}

}
