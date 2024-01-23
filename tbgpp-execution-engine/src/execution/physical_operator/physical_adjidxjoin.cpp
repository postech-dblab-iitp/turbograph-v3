
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
inline ExpandDirection adjListLogicalTypeToExpandDir(LogicalType &adjType) {
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
		case VectorType::CONSTANT_VECTOR: {
			return ((uint64_t *)ConstantVector::GetData<uintptr_t>(vector))[0];
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
		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
			src_vid, adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]));
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
		case VectorType::CONSTANT_VECTOR: {
			auto is_null = ConstantVector::IsNull(input.data[state.srcColIdx]);
			for (idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = is_null;
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

// TODO
void PhysicalAdjIdxJoin::GetAdjListAndFillOutput() {
	
}

// TODO function handlepredicates
	// x has label blabla
	// x == x
	// x != x
	// x in y

void PhysicalAdjIdxJoin::ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate, bool is_left_join) const {
	auto &state = (AdjIdxJoinState &)lstate;

// icecream::ic.enable();
// std::cout << "[PhysicalAdjIdxJoin] input" << std::endl;
// IC(input.size(), (uint8_t)join_type, state.srcColIdx, state.tgtColIdx, state.edgeColIdx);
// if (input.size() > 0) {
// 	IC(input.ToString(std::min((idx_t)20, input.size())));
// }
// icecream::ic.disable();
	
	uint64_t *adj_start, *adj_end;
	uint64_t *tgt_adj_column = nullptr;
	uint64_t *eid_adj_column = nullptr;
	uint64_t src_vid;
	size_t adjlist_size;
	
	D_ASSERT(state.srcColIdx < input.ColumnCount());
	Vector &src_vid_column_vector = input.data[state.srcColIdx];	// can be dictionaryvector

	D_ASSERT(discard_tgt || state.tgtColIdx < chunk.ColumnCount());
	if (load_eid) {
		D_ASSERT(discard_edge || (state.edgeColIdx >= 0 && state.edgeColIdx < chunk.ColumnCount()));
		if (!discard_edge) eid_adj_column = (uint64_t *)chunk.data[state.edgeColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	}
	if (!discard_tgt) tgt_adj_column = (uint64_t *)chunk.data[state.tgtColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	Vector &outer_vec = input.data[outer_pos];
	// inner_vec = adj_start; // TODO
	ExpandDirection cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
	D_ASSERT(state.adj_col_idxs.size() == 1); // TODO we currently do not support iterate more than one edge types

	// iterate source vids
	switch (src_vid_column_vector.GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			while (state.output_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size()) {
				uint64_t src_vid = ((uint64_t *)src_vid_column_vector.GetData())[DictionaryVector::SelVector(src_vid_column_vector).get_index(state.lhs_idx)];
				context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
					src_vid, adj_start, adj_end, cur_direction);

				// calculate size
				int adj_size = (adj_end - adj_start) / 2;
				size_t num_rhs_left = adj_size - state.rhs_idx;
				size_t num_rhs_to_try_fetch = ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left)
												? num_rhs_left : (STANDARD_VECTOR_SIZE - state.output_idx);
				// TODO apply filter predicates
				if (adj_size == 0) {
					D_ASSERT(num_rhs_to_try_fetch == 0);
				} else {
					state.all_adjs_null = false;

					// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
					fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, false, outer_vec);
				}
				
				// update lhs_idx and adj_idx for next iteration
				if (state.rhs_idx >= adj_size) {
					// for this (lhs_idx, adj_idx), equi join is done
					if (state.adj_idx == state.adj_col_idxs.size() - 1) {
						if (state.all_adjs_null && (join_type == JoinType::LEFT)) {
							// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
							fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, 1, true, outer_vec);
						}
						state.all_adjs_null = true;
						state.lhs_idx++;
						state.adj_idx = 0;
						// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
					} else {
						state.adj_idx++;
						// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
					}
					state.rhs_idx = 0;
				}
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			while (state.output_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size()) {
				uint64_t src_vid = ((uint64_t *)src_vid_column_vector.GetData())[state.lhs_idx];
				context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
					src_vid, adj_start, adj_end, cur_direction);

				// calculate size
				int adj_size = (adj_end - adj_start) / 2;
				size_t num_rhs_left = adj_size - state.rhs_idx;
				size_t num_rhs_to_try_fetch = ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left)
												? num_rhs_left : (STANDARD_VECTOR_SIZE - state.output_idx);
				// TODO apply filter predicates
				if (adj_size == 0) {
					D_ASSERT(num_rhs_to_try_fetch == 0);
				} else {
					state.all_adjs_null = false;

					// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
					fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, false, outer_vec);
				}
				
				// update lhs_idx and adj_idx for next iteration
				if (state.rhs_idx >= adj_size) {
					// for this (lhs_idx, adj_idx), equi join is done
					if (state.adj_idx == state.adj_col_idxs.size() - 1) {
						if (state.all_adjs_null && (join_type == JoinType::LEFT)) {
							// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
							fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, 1, true, outer_vec);
						}
						state.all_adjs_null = true;
						state.lhs_idx++;
						state.adj_idx = 0;
						// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
					} else {
						state.adj_idx++;
						// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
					}
					state.rhs_idx = 0;
				}
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			while (state.output_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size()) {
				uint64_t src_vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(src_vid_column_vector))[0];
				context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
					src_vid, adj_start, adj_end, cur_direction);

				// calculate size
				int adj_size = (adj_end - adj_start) / 2;
				size_t num_rhs_left = adj_size - state.rhs_idx;
				size_t num_rhs_to_try_fetch = ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left)
												? num_rhs_left : (STANDARD_VECTOR_SIZE - state.output_idx);
				// TODO apply filter predicates
				if (adj_size == 0) {
					D_ASSERT(num_rhs_to_try_fetch == 0);
				} else {
					state.all_adjs_null = false;

					// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
					fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, false, outer_vec);
				}
				
				// update lhs_idx and adj_idx for next iteration
				if (state.rhs_idx >= adj_size) {
					// for this (lhs_idx, adj_idx), equi join is done
					if (state.adj_idx == state.adj_col_idxs.size() - 1) {
						if (state.all_adjs_null && (join_type == JoinType::LEFT)) {
							// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
							fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, 1, true, outer_vec);
						}
						state.all_adjs_null = true;
						state.lhs_idx++;
						state.adj_idx = 0;
						// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
					} else {
						state.adj_idx++;
						// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
					}
					state.rhs_idx = 0;
				}
			}
			break;
		}
		default: {
			D_ASSERT(false);
		}
	}
	// while (state.output_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size()) {
	// 	uint64_t &src_vid = getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
	// 	context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx],
	// 		src_vid, adj_start, adj_end, cur_direction);

	// 	// calculate size
	// 	int adj_size = (adj_end - adj_start) / 2;
	// 	size_t num_rhs_left = adj_size - state.rhs_idx;
	// 	size_t num_rhs_to_try_fetch = ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left)
	// 									? num_rhs_left : (STANDARD_VECTOR_SIZE - state.output_idx);
	// 	// TODO apply filter predicates
	// 	if (adj_size == 0) {
	// 		D_ASSERT(num_rhs_to_try_fetch == 0);
	// 	} else {
	// 		state.all_adjs_null = false;

	// 		// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
	// 		fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, num_rhs_to_try_fetch, false, outer_vec);
	// 	}
		
	// 	// update lhs_idx and adj_idx for next iteration
	// 	if (state.rhs_idx >= adj_size) {
	// 		// for this (lhs_idx, adj_idx), equi join is done
	// 		if (state.adj_idx == state.adj_col_idxs.size() - 1) {
	// 			if (state.all_adjs_null && (join_type == JoinType::LEFT)) {
	// 				// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
	// 				fillFunc(state, adj_start, tgt_adj_column, eid_adj_column, 1, true, outer_vec);
	// 			}
	// 			state.all_adjs_null = true;
	// 			state.lhs_idx++;
	// 			state.adj_idx = 0;
	// 			// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
	// 		} else {
	// 			state.adj_idx++;
	// 			// cur_direction = adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
	// 		}
	// 		state.rhs_idx = 0;
	// 	}
	// }
	// chunk determined. now fill in lhs using slice operation
	idx_t schema_idx = input.GetSchemaIdx();
	schema_idx = 0; // TODO 240117 tslee maybe we don't need
	D_ASSERT(schema_idx < state.outer_col_maps.size());
	D_ASSERT(input.ColumnCount() == state.outer_col_maps[schema_idx].size());
	for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
		if (state.outer_col_maps[schema_idx][colId] != std::numeric_limits<uint32_t>::max()) {
			// when outer col map marked uint32_max, do not return
			D_ASSERT(state.outer_col_maps[schema_idx][colId] < chunk.ColumnCount());
			chunk.data[state.outer_col_maps[schema_idx][colId]].Slice(input.data[colId], state.rhs_sel, state.output_idx);
		}
	}
	D_ASSERT( state.output_idx <= STANDARD_VECTOR_SIZE );
	chunk.SetCardinality(state.output_idx);
}

void PhysicalAdjIdxJoin::ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false); // 0405 deprecated
}


OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
 
	if (!state.first_fetch) {
		// values used while processing
		state.srcColIdx = sid_col_idx;
		if (load_eid) {
			if (load_eid_temporarily) {
				state.tgtColIdx = inner_col_map[0];
				state.edgeColIdx = inner_col_map[1];
			} else {
				state.edgeColIdx = inner_col_map[0];
				state.tgtColIdx = inner_col_map[1];
			}
		} else {
			state.tgtColIdx = inner_col_map[0];
			state.edgeColIdx = -1;
		}
		
		state.outer_col_map = move(outer_col_map);
		state.outer_col_maps = move(outer_col_maps);
		state.inner_col_map = move(inner_col_map);
		// Get join matches (sizes) for the LHS. Initialized one time per LHS
		GetJoinMatches(context, input, lstate);
		state.first_fetch = true;
	}

	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// these joins can be processed in single call. explicitly process them and return
		ProcessSemiAntiJoin(context, input, chunk, lstate);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	const bool isProcessingTerminated = state.join_finished;
	if (isProcessingTerminated) {	
		// initialize state for next chunk
		state.resetForNewInput();

		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		ProcessEquiJoin(context, input, chunk, lstate, join_type == JoinType::LEFT);
		// update states
		
		// TODO correctness check
		if (state.lhs_idx >= input.size()) {
			state.join_finished = true;
			state.resetForNewInput();
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			state.resetForMoreOutput();
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalAdjIdxJoin::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	return ExecuteNaiveInput(context, input, chunk, lstate);
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
