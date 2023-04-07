
#include "typedef.hpp"

#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "extent/extent_iterator.hpp"
#include "common/types/selection_vector.hpp"
#include "planner/joinside.hpp"

#include <string>
#include "icecream.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//

unique_ptr<OperatorState> PhysicalAdjIdxJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<AdjIdxJoinState>( );
}

// helper functions
inline ExpandDirection adjListLogicalTypeToExpandDir(LogicalType adjType) {
	return adjType == LogicalType::FORWARD_ADJLIST ? ExpandDirection::OUTGOING : ExpandDirection::INCOMING;
}

inline uint64_t& getIdRefFromVector(Vector& vector, idx_t index) {
	switch( vector.GetVectorType() ) {
		case VectorType::DICTIONARY_VECTOR: {
			return ((uint64_t *)vector.GetData())[DictionaryVector::SelVector(vector).get_index(index)];
		}
		case VectorType::FLAT_VECTOR: {
			return ((uint64_t *)vector.GetData())[index];
		}
		default: {
			D_ASSERT(false);
		}
	}
}

void PhysicalAdjIdxJoin::ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
	
	D_ASSERT(join_type == JoinType::SEMI || join_type == JoinType::ANTI);

	uint64_t* adj_start;
	uint64_t* adj_end;
	uint64_t src_vid;
	Vector& src_vid_column_vector = input.data[state.srcColIdx];	// can be dictionaryvector
	size_t adjlist_size;

	for( ; state.lhs_idx < input.size(); state.lhs_idx++ ) {
		
		uint64_t& src_vid = getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], src_vid, adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]));
		// FIXME debug calculate size directly here
		int adj_size_debug = (adj_end - adj_start)/2;
		const bool predicate_satisfied = (join_type == JoinType::SEMI)
							? ( !state.src_nullity[state.lhs_idx] && adj_size_debug > 0 ) 		// semi join
							: ( state.src_nullity[state.lhs_idx] || adj_size_debug == 0 );		// anti join
		if( predicate_satisfied ) {
			state.rhs_sel.set_index(state.output_idx++, state.lhs_idx);
		}
	}
	// use sel vector and filter lhs to chunk
	// chunk determined. now fill in lhs using slice operation
	for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
		chunk.data[colId].Slice(input.data[colId], state.rhs_sel, state.output_idx);
	}
	D_ASSERT( state.output_idx <= STANDARD_VECTOR_SIZE );
	chunk.SetCardinality(state.output_idx);


}

void PhysicalAdjIdxJoin::GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
	
	// check nullity (debug note : this part has no burden in execution time. verified!)
	switch( input.data[state.srcColIdx].GetVectorType() ) {
		case VectorType::DICTIONARY_VECTOR: {
			const auto &sel_vector = DictionaryVector::SelVector(input.data[state.srcColIdx]);
			const auto &child = DictionaryVector::Child(input.data[state.srcColIdx]);
			for(idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = FlatVector::IsNull( child, sel_vector.get_index(i) );
			}
			break;
		}
		default: {
			for(idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = FlatVector::IsNull(input.data[state.srcColIdx], i);
			}
			break;
		}
	}

	// fill in adjacency col info
// DEBUG 
	if( state.adj_col_idxs.size() == 0 ) {
		context.client->graph_store->getAdjColIdxs((idx_t)adjidx_obj_id, state.adj_col_idxs, state.adj_col_types);
		// 230303 changed api
		//context.client->graph_store->getAdjColIdxs(srcLabelSet, edgeLabelSet, expandDir, state.adj_col_idxs, state.adj_col_types);	
	}
	
	// resize join sizes using adj_col_idxs
	auto adjColCnt = state.adj_col_idxs.size();
	for( auto& v: state.join_sizes) {
		v.resize(adjColCnt);
	}
	
	// ask for sizes foreach srcvid
// FIXME commented until replaced by lightweight storage API
	// Vector& src_vid_column_vector = input.data[state.srcColIdx];
	// uint64_t *adj_start; uint64_t *adj_end;
	// size_t adjListSize = adj_end - adj_start;

	// for(idx_t a_i = 0; a_i < state.adj_col_idxs.size(); a_i++) {
	// 	for(idx_t v_i = 0; v_i < input.size(); v_i++) {
	// 		// FIXME need to use different APIs : that only returns size not whoe adjacency
	// 		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[a_i], getIdRefFromVector(src_vid_column_vector, v_i), adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[a_i]));
	// 		adjListSize = (adj_end - adj_start)/2;
	// 		state.join_sizes[v_i][a_i] = adjListSize;
	// 		state.total_join_size[v_i] += adjListSize;
	// 	}
	// }

}

// TODO function handlepredicates
	// x has label blabla
	// x == x
	// x != x
	// x in y

void PhysicalAdjIdxJoin::ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate, bool is_left_join) const {
	auto &state = (AdjIdxJoinState &)lstate;

// icecream::ic.enable();
// IC(input.size(), (uint8_t)join_type, state.srcColIdx, state.tgtColIdx, state.edgeColIdx);
// if (input.size() > 0) {
// 	IC(input.ToString(std::min((idx_t)10, input.size())));
// }
// icecream::ic.disable();
	
	uint64_t *adj_start, *adj_end;
	uint64_t *tgt_adj_column = nullptr;
	uint64_t *eid_adj_column = nullptr;
	uint64_t src_vid;
	size_t adjlist_size;
	
	D_ASSERT(state.srcColIdx < input.ColumnCount());
	Vector& src_vid_column_vector = input.data[state.srcColIdx];	// can be dictionaryvector

	D_ASSERT(discard_tgt || state.tgtColIdx < chunk.ColumnCount());
	if (load_eid) {
		D_ASSERT(discard_edge || (state.edgeColIdx >= 0 && state.edgeColIdx < chunk.ColumnCount()));
		if (!discard_edge) eid_adj_column = (uint64_t *)chunk.data[state.edgeColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	}
	if (!discard_tgt) tgt_adj_column = (uint64_t *)chunk.data[state.tgtColIdx].GetData();	// always flatvector[ID]. so ok to access directly

	// iterate source vids
	while( state.output_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size() ) {
		// bypass null src or empty adjs
	// FIXME debug need this!! when src null!!
		// if( state.total_join_size[state.lhs_idx] == 0 ) {
		// 	state.lhs_idx++;
		// 	continue;
		// }
		uint64_t& src_vid = getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], src_vid, adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]));

		// calculate size
		int adj_size = (adj_end - adj_start)/2;
		size_t num_rhs_left = adj_size - state.rhs_idx;
		size_t num_rhs_to_try_fetch = ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left )
										? num_rhs_left : (EXEC_ENGINE_VECTOR_SIZE - state.output_idx);
		// TODO apply filter predicates
		if (adj_size == 0) {
			D_ASSERT(num_rhs_to_try_fetch == 0);
		} else {
			state.all_adjs_null = false;
			// fillOutputChunk(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, false);
			// set sel vector on lhs	// TODO apply filter predicates
			auto tmp_output_idx = state.output_idx;	// do not alter output_idx here
			for( ; tmp_output_idx < state.output_idx + num_rhs_to_try_fetch ; tmp_output_idx++ ) {
				state.rhs_sel.set_index(tmp_output_idx, state.lhs_idx);
			}

			// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
			fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, false);
			// auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
			// for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
			// 	tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
			// 	if (load_eid) {
			// 		D_ASSERT(eid_adj_column != nullptr);
			// 		eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
			// 	}
			// 	state.output_idx++;
			// }
			D_ASSERT(tmp_output_idx == state.output_idx);
		}
		
		// update lhs_idx and adj_idx for next iteration
		if (state.rhs_idx >= adj_size) {
			// for this (lhs_idx, adj_idx), equi join is done
			if (state.adj_idx == state.adj_col_idxs.size() - 1) {
				if (state.all_adjs_null && (join_type == JoinType::LEFT)) {
					// fillOutputChunk(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, true);
					// set sel vector on lhs	// TODO apply filter predicates
					auto tmp_output_idx = state.output_idx;	// do not alter output_idx here
					for( ; tmp_output_idx < state.output_idx + 1 ; tmp_output_idx++ ) {
						state.rhs_sel.set_index(tmp_output_idx, state.lhs_idx);
					}

					// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
					fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, 1, true);
					// auto tmp_rhs_idx_end = state.rhs_idx + 1;
					// for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
					// 	tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
					// 	if (load_eid) {
					// 		D_ASSERT(eid_adj_column != nullptr);
					// 		eid_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
					// 	}
					// 	state.output_idx++;
					// }
					D_ASSERT(tmp_output_idx == state.output_idx);
				}
				state.all_adjs_null = true;
				state.lhs_idx++;
				state.adj_idx = 0;
			}
			else { state.adj_idx++; }
			state.rhs_idx = 0;
		}
	}
	// chunk determined. now fill in lhs using slice operation
	D_ASSERT(input.ColumnCount() == state.outer_col_map.size());
	for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
		if( state.outer_col_map[colId] != std::numeric_limits<uint32_t>::max() ) {
			// when outer col map marked uint32_max, do not return
			D_ASSERT(state.outer_col_map[colId] < chunk.ColumnCount());
			chunk.data[state.outer_col_map[colId]].Slice(input.data[colId], state.rhs_sel, state.output_idx);
		}
	}
	D_ASSERT( state.output_idx <= STANDARD_VECTOR_SIZE );
	chunk.SetCardinality(state.output_idx);

// icecream::ic.enable();
// IC(chunk.size());
// if (chunk.size() != 0)
// 	IC(chunk.ToString(std::min(10, (int)chunk.size())));
// icecream::ic.disable();
}

void PhysicalAdjIdxJoin::ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false); // 0405 deprecated
	// TODO later
	// (here we do not apply predicates as rhs is null)
	// however this logic needs further verification
	// if nonempty predicate,
		// mark leftjoin finished

	// use left_lhs_idx
	// for empty lefts, produce lhs, null
}


OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
 

	if (!state.first_fetch) {
		// values used while processing
		//state.srcColIdx = schema.getColIdxOfKey(srcName);
		state.srcColIdx = sid_col_idx;
		// state.edgeColIdx = input.ColumnCount();
		// state.tgtColIdx = input.ColumnCount() + int(load_eid);
		//D_ASSERT(inner_col_map.size() == 2);
		if (load_eid) {
			state.tgtColIdx = inner_col_map[0];
			state.edgeColIdx = inner_col_map[1];
		} else {
			state.tgtColIdx = inner_col_map[0];
			state.edgeColIdx = -1;
		}
		
		state.outer_col_map = move(outer_col_map);
		state.inner_col_map = move(inner_col_map);
		// Get join matches (sizes) for the LHS. Initialized one time per LHS
		GetJoinMatches(context, input, lstate);
		state.first_fetch = true;
	}

	if( join_type == JoinType::SEMI || join_type == JoinType::ANTI ) {
		// these joins can be processed in single call. explicitly process them and return
		ProcessSemiAntiJoin(context, input, chunk, lstate);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	const bool isProcessingTerminated = state.join_finished;
	if ( isProcessingTerminated ) {	
		// initialize state for next chunk
		state.resetForNewInput();

		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		ProcessEquiJoin(context, input, chunk, lstate, join_type == JoinType::LEFT);
		// update states
		state.resetForMoreOutput();
		if( state.lhs_idx >= input.size() ) {
			state.join_finished = true;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalAdjIdxJoin::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {

	// 230303 
	return ExecuteNaiveInput(context, input, chunk, lstate);


// icecream::ic.enable();
// 	if( schema.getCypherType(srcName) == CypherValueType::RANGE ) {
// 		D_ASSERT( false && "currently not supporting when range is given as input");
// 		return ExecuteRangedInput(context, input, chunk, lstate);
// 	} else {
// 		return ExecuteNaiveInput(context, input, chunk, lstate);
// 	}
// icecream::ic.disable();
}

std::string PhysicalAdjIdxJoin::ParamsToString() const {
	std::string result = "";
	result += "adjidx_obj_id=" + std::to_string(adjidx_obj_id) + ", ";
	result += "sid_col_idx=" + std::to_string(sid_col_idx) + ", ";
	result += "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
	result += "inner_col_map.size()=" + std::to_string(inner_col_map.size()) + ", ";
	return result;
}

std::string PhysicalAdjIdxJoin::ToString() const {
	return "AdjIdxJoin";
}


}
