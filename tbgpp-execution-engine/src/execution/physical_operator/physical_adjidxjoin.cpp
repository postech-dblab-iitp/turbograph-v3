
#include "typedef.hpp"

#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "extent/extent_iterator.hpp"

#include <string>
#include "icecream.hpp"

#include <boost/timer/timer.hpp>

namespace duckdb {


PhysicalAdjIdxJoin::PhysicalAdjIdxJoin(CypherSchema& sch,
	std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, bool load_eid, bool enumerate)
	: CypherPhysicalOperator(sch), srcName(srcName), srcLabelSet(srcLabelSet), edgeLabelSet(edgeLabelSet), expandDir(expandDir), tgtLabelSet(tgtLabelSet), join_type(join_type), load_eid(load_eid), enumerate(enumerate) {
		
	// init timers
	adjfetch_timer_started = false;

	// operator rules
	bool check = (enumerate) ? true : (!load_eid);
	assert( check && "load_eid should be set to false(=not returning edge ids) when `enumerate` set to `false` (=range)");

	// TODO assert
	//assert( expandDir == ExpandDirection::OUTGOING && "currently supports outgoing index. how to do for both direction or incoming?"); // TODO needs support from the storage
	assert(enumerate == true && "always enumerate for now");
	assert( srcLabelSet.size() == 1 && "src label shuld be assigned and be only one for now");
	assert( tgtLabelSet.size() <= 1 && "no multiple targets"); // TODO needs support from the storage
	assert( edgeLabelSet.size() <= 1 && "no multiple edges Storage API support needed"); // TODO needs support from the storage
	assert( enumerate && "need careful debugging on range mode"); // TODO needs support from the storage
	// assert( join_type == JoinType::INNER && "write all fixmes"); // TODO needs support from the storage
		
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
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
	
	// TODO future get filter information for target labelset

};

unique_ptr<OperatorState> PhysicalAdjIdxJoin::GetOperatorState(ExecutionContext &context) const {

	return make_unique<AdjIdxJoinState>( );
}


OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate;

// icecream::ic.enable();
// IC(input.size());
// if (input.size() != 0)
// 	IC( input.ToString(std::min(10, (int)input.size())) );
// icecream::ic.disable();

//icecream::ic.enable();
	// init
	vector<LogicalType> input_datachunk_types = move(input.GetTypes());
	const idx_t srcColIdx = schema.getColIdxOfKey(srcName);	// first index of source node : where source node id is
	const idx_t edgeColIdx = input.ColumnCount();			// not using when load_eid = false
	const idx_t tgtColIdx = input.ColumnCount() + int(load_eid);	// first index of target node : where target node id should be OR start index of range
// IC(srcColIdx);
// IC(edgeColIdx);
// IC(tgtColIdx);
	// intra-chunk variables
	int numProducedTuples = 0;
	bool isHaveMoreOutput = false;
	bool isSeekPointReached = false;

	// get adjacency list columns in column
	vector<int> adjColIdxs;
	vector<LogicalType> adjColTypes;
// IC();
// for( auto& k: edgeLabelSet.data ) { IC(k); }
	context.client->graph_store->getAdjColIdxs(srcLabelSet, edgeLabelSet, expandDir, adjColIdxs, adjColTypes);
	D_ASSERT( adjColIdxs.size() > 0);
	D_ASSERT( adjColIdxs.size() == adjColTypes.size() );
// IC(adjColIdxs.size());
// IC(adjColIdxs[0]);

	uint64_t* adj_start; uint64_t* adj_end;

	// iterate source vids
	for( ; state.checkpoint.first < input.size() ; state.checkpoint.first++) {

		// check if output chunk full
		if( numProducedTuples == EXEC_ENGINE_VECTOR_SIZE ) { isHaveMoreOutput = true; goto breakLoop; }

		// check if vid is null
		if( state.checkpoint.second == 0 && input.GetValue(srcColIdx, state.checkpoint.first).IsNull() ) {
			// when left join produce left
			if( join_type == JoinType::LEFT ) {
				// produce lhs
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
				}
				// produce rhs (null)
				chunk.SetValue(tgtColIdx, numProducedTuples, Value( LogicalType::UBIGINT ));
				if( load_eid ) { chunk.SetValue(edgeColIdx, numProducedTuples, Value( LogicalType::UBIGINT )); }
				numProducedTuples +=1;
			}
			continue;
		}
		// TODO actually there should be one more chunk full checking logic after producing 1 tuple in left join
		// vid is not null. now get source vid
		uint64_t vid = input.data[srcColIdx].GetValue(state.checkpoint.first).GetValue<uint64_t>();
// WARNING adjfetch timer very slow
// if( ! adjfetch_timer_started ) {
// 	adjfetch_timer.start();
// 	adjfetch_timer_started = true;
// } else {
// 	adjfetch_timer.resume();
// }
		context.client->graph_store->getAdjListFromVid(*state.adj_it, adjColIdxs[0], vid, adj_start, adj_end, expandDir);
// adjfetch_timer.stop();

		size_t adjListSize = adj_end - adj_start;
		D_ASSERT( adjListSize % 2 == 0 );

		size_t numTargets = (adjListSize / 2) - state.checkpoint.second;			// adjListSize = 2 * target vertices

		// in anti/semijoin, the result is always smaller than input. Thus no overflow check required
		if ( join_type == JoinType::SEMI ) {
			if( adjListSize > 0 ) { 
				// produce lhs only
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
				}
				numProducedTuples +=1;
			}
			continue;
		} else if (join_type == JoinType::ANTI ) {
			if( adjListSize == 0 ) { 
				// produce lhs only
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
				}
				numProducedTuples +=1;
			}
			continue;
		}
		// for left join, consider no adjacency case
		if ( join_type == JoinType::LEFT ) {
			if( adjListSize == 0 ) { 
				// produce lhs
				for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
					chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
				}
				// produce rhs (null)
				chunk.SetValue(tgtColIdx, numProducedTuples, Value( LogicalType::UBIGINT ));
				if( load_eid ) { chunk.SetValue(edgeColIdx, numProducedTuples, Value( LogicalType::UBIGINT )); }
				numProducedTuples +=1;
			}
			continue;
		}

		// enumerate mode
		// choose smaller one
		const size_t numTuplesToProduce = ((EXEC_ENGINE_VECTOR_SIZE - numProducedTuples) > numTargets )
			? numTargets : (EXEC_ENGINE_VECTOR_SIZE - numProducedTuples);
		const size_t finalCheckpoint = state.checkpoint.second + numTuplesToProduce;
		for( ; state.checkpoint.second < finalCheckpoint; state.checkpoint.second++ ) {
			// PRODUCE
			// produce lhs
			for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
			}
			// produce rhs : tid, (edgeid)
			chunk.SetValue(tgtColIdx, numProducedTuples, Value::UBIGINT( adj_start[(idx_t)(state.checkpoint.second*2)] ));
			if( load_eid ) { chunk.SetValue(edgeColIdx, numProducedTuples, Value::UBIGINT( adj_start[(idx_t)(state.checkpoint.second*2 + 1)] )); }

			numProducedTuples +=1;
		}

		if( numTuplesToProduce == numTargets ) {
			// produce for this vertex done. init second
			state.checkpoint.second = 0;
		} else {
			isHaveMoreOutput = true;
			goto breakLoop;
		}
	
	}

breakLoop:

	// now produce finished. store state and exit
	D_ASSERT( numProducedTuples <= EXEC_ENGINE_VECTOR_SIZE );
	chunk.SetCardinality(numProducedTuples);

// icecream::ic.enable();
// IC(chunk.size());
// if (chunk.size() != 0)
// 	IC( chunk.ToString(std::min(10, (int)chunk.size())) );
// icecream::ic.disable();

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
