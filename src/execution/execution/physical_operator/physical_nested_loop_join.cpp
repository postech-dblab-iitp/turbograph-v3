// #include "execution/operator/join/physical_nested_loop_join.hpp"
// #include "parallel/thread_context.hpp"
// #include "common/operator/comparison_operators.hpp"
// #include "common/vector_operations/vector_operations.hpp"
// #include "execution/expression_executor.hpp"
//#include "execution/nested_loop_join.hpp"
// #include "main/client_context.hpp"

#include "execution/physical_operator/physical_join.hpp"

namespace duckdb {

// PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
//                                                unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
//                                                JoinType join_type, idx_t estimated_cardinality)
//     : PhysicalComparisonJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond), join_type, estimated_cardinality) {
// 	children.push_back(move(left));
// 	children.push_back(move(right));
// }

// static bool HasNullValues(DataChunk &chunk) {
// 	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
// 		VectorData vdata;
// 		chunk.data[col_idx].Orrify(chunk.size(), vdata);

// 		if (vdata.validity.AllValid()) {
// 			continue;
// 		}
// 		for (idx_t i = 0; i < chunk.size(); i++) {
// 			auto idx = vdata.sel->get_index(i);
// 			if (!vdata.validity.RowIsValid(idx)) {
// 				return true;
// 			}
// 		}
// 	}
// 	return false;
// }

template <bool MATCH>
static void ConstructSemiOrAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	// create the selection vector from the matches that were found
	idx_t result_count = 0;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < left.size(); i++) {
		if (found_match[i] == MATCH) {
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// project them using the result selection vector
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		result.SetCardinality(0);
	}
}

void PhysicalJoin::ConstructSemiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<true>(left, result, found_match);
}

void PhysicalJoin::ConstructAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<false>(left, result, found_match);
}

void PhysicalJoin::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
                                           bool has_null) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(left);
	for (idx_t i = 0; i < left.ColumnCount(); i++) {
		result.data[i].Reference(left.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		VectorData jdata;
		join_keys.data[col_idx].Orrify(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValid(jidx));
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < left.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * left.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (has_null) {
		for (idx_t i = 0; i < left.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

// //===--------------------------------------------------------------------===//
// // Sink
// //===--------------------------------------------------------------------===//
// class NestedLoopJoinLocalState : public LocalSinkState {
// public:
// 	explicit NestedLoopJoinLocalState(const vector<JoinCondition> &conditions) {
// 		vector<LogicalType> condition_types;
// 		for (auto &cond : conditions) {
// 			rhs_executor.AddExpression(*cond.right);
// 			condition_types.push_back(cond.right->return_type);
// 		}
// 		right_condition.Initialize(condition_types);
// 	}

// 	//! The chunk holding the right condition
// 	DataChunk right_condition;
// 	//! The executor of the RHS condition
// 	ExpressionExecutor rhs_executor;
// };

// class NestedLoopJoinGlobalState : public GlobalSinkState {
// public:
// 	NestedLoopJoinGlobalState() : has_null(false) {
// 	}

// 	mutex nj_lock;
// 	//! Materialized data of the RHS
// 	ChunkCollection right_data;
// 	//! Materialized join condition of the RHS
// 	ChunkCollection right_chunks;
// 	//! Whether or not the RHS of the nested loop join has NULL values
// 	bool has_null;
// 	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
// 	unique_ptr<bool[]> right_found_match;
// };

// SinkResultType PhysicalNestedLoopJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
//                                             DataChunk &input) const {
// 	auto &gstate = (NestedLoopJoinGlobalState &)state;
// 	auto &nlj_state = (NestedLoopJoinLocalState &)lstate;

// 	// resolve the join expression of the right side
// 	nlj_state.right_condition.Reset();
// 	nlj_state.rhs_executor.Execute(input, nlj_state.right_condition);

// 	// if we have not seen any NULL values yet, and we are performing a MARK join, check if there are NULL values in
// 	// this chunk
// 	if (join_type == JoinType::MARK && !gstate.has_null) {
// 		if (HasNullValues(nlj_state.right_condition)) {
// 			gstate.has_null = true;
// 		}
// 	}

// 	// append the data and the
// 	lock_guard<mutex> nj_guard(gstate.nj_lock);
// 	gstate.right_data.Append(input);
// 	gstate.right_chunks.Append(nlj_state.right_condition);
// 	return SinkResultType::NEED_MORE_INPUT;
// }

// void PhysicalNestedLoopJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
// 	auto &state = (NestedLoopJoinLocalState &)lstate;
// 	auto &client_profiler = QueryProfiler::Get(context.client);

// 	context.thread.profiler.Flush(this, &state.rhs_executor, "rhs_executor", 1);
// 	client_profiler.Flush(context.thread.profiler);
// }

// SinkFinalizeType PhysicalNestedLoopJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
//                                                   GlobalSinkState &gstate_p) const {
// 	auto &gstate = (NestedLoopJoinGlobalState &)gstate_p;
// 	if (join_type == JoinType::OUTER || join_type == JoinType::RIGHT) {
// 		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
// 		gstate.right_found_match = unique_ptr<bool[]>(new bool[gstate.right_data.Count()]);
// 		memset(gstate.right_found_match.get(), 0, sizeof(bool) * gstate.right_data.Count());
// 	}
// 	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
// 		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
// 	}
// 	return SinkFinalizeType::READY;
// }

// unique_ptr<GlobalSinkState> PhysicalNestedLoopJoin::GetGlobalSinkState(ClientContext &context) const {
// 	return make_unique<NestedLoopJoinGlobalState>();
// }

// unique_ptr<LocalSinkState> PhysicalNestedLoopJoin::GetLocalSinkState(ExecutionContext &context) const {
// 	return make_unique<NestedLoopJoinLocalState>(conditions);
// }

// //===--------------------------------------------------------------------===//
// // Operator
// //===--------------------------------------------------------------------===//
// class PhysicalNestedLoopJoinState : public OperatorState {
// public:
// 	PhysicalNestedLoopJoinState(const PhysicalNestedLoopJoin &op, const vector<JoinCondition> &conditions)
// 	    : fetch_next_left(true), fetch_next_right(false), right_chunk(0), left_tuple(0), right_tuple(0) {
// 		vector<LogicalType> condition_types;
// 		for (auto &cond : conditions) {
// 			lhs_executor.AddExpression(*cond.left);
// 			condition_types.push_back(cond.left->return_type);
// 		}
// 		left_condition.Initialize(condition_types);
// 		if (IsLeftOuterJoin(op.join_type)) {
// 			left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
// 			memset(left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
// 		}
// 	}

// 	bool fetch_next_left;
// 	bool fetch_next_right;
// 	idx_t right_chunk;
// 	DataChunk left_condition;
// 	//! The executor of the LHS condition
// 	ExpressionExecutor lhs_executor;

// 	idx_t left_tuple;
// 	idx_t right_tuple;

// 	unique_ptr<bool[]> left_found_match;

// public:
// 	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
// 		context.thread.profiler.Flush(op, &lhs_executor, "lhs_executor", 0);
// 	}
// };

// unique_ptr<OperatorState> PhysicalNestedLoopJoin::GetOperatorState(ClientContext &context) const {
// 	return make_unique<PhysicalNestedLoopJoinState>(*this, conditions);
// }

// OperatorResultType PhysicalNestedLoopJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
//                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
// 	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

// 	if (gstate.right_chunks.Count() == 0) {
// 		// empty RHS
// 		if (!EmptyResultIfRHSIsEmpty()) {
// 			ConstructEmptyJoinResult(join_type, gstate.has_null, input, chunk);
// 			return OperatorResultType::NEED_MORE_INPUT;
// 		} else {
// 			return OperatorResultType::FINISHED;
// 		}
// 	}

// 	switch (join_type) {
// 	case JoinType::SEMI:
// 	case JoinType::ANTI:
// 	case JoinType::MARK:
// 		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
// 		ResolveSimpleJoin(context, input, chunk, state_p);
// 		return OperatorResultType::NEED_MORE_INPUT;
// 	case JoinType::LEFT:
// 	case JoinType::INNER:
// 	case JoinType::OUTER:
// 	case JoinType::RIGHT:
// 		return ResolveComplexJoin(context, input, chunk, state_p);
// 	default:
// 		throw NotImplementedException("Unimplemented type for nested loop join!");
// 	}
// }

// void PhysicalNestedLoopJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
//                                                OperatorState &state_p) const {
// 	auto &state = (PhysicalNestedLoopJoinState &)state_p;
// 	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

// 	// resolve the left join condition for the current chunk
// 	state.lhs_executor.Execute(input, state.left_condition);

// 	bool found_match[STANDARD_VECTOR_SIZE] = {false};
// 	NestedLoopJoinMark::Perform(state.left_condition, gstate.right_chunks, found_match, conditions);
// 	switch (join_type) {
// 	case JoinType::MARK:
// 		// now construct the mark join result from the found matches
// 		PhysicalJoin::ConstructMarkJoinResult(state.left_condition, input, chunk, found_match, gstate.has_null);
// 		break;
// 	case JoinType::SEMI:
// 		// construct the semi join result from the found matches
// 		PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
// 		break;
// 	case JoinType::ANTI:
// 		// construct the anti join result from the found matches
// 		PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
// 		break;
// 	default:
// 		throw NotImplementedException("Unimplemented type for simple nested loop join!");
// 	}
// }

void PhysicalJoin::ConstructLeftJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	idx_t remaining_count = 0;
	for (idx_t i = 0; i < left.size(); i++) {
		if (!found_match[i]) {
			remaining_sel.set_index(remaining_count++, i);
		}
	}
	if (remaining_count > 0) {
		result.Slice(left, remaining_sel, remaining_count);
		for (idx_t idx = left.ColumnCount(); idx < result.ColumnCount(); idx++) {
			result.data[idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[idx], true);
		}
	}
}

void PhysicalJoin::ConstructLeftJoinResult(DataChunk &left, DataChunk &result, bool found_match[], const vector<uint32_t>& right_col_map) {
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	idx_t remaining_count = 0;
	for (idx_t i = 0; i < left.size(); i++) {
		if (!found_match[i]) {
			remaining_sel.set_index(remaining_count++, i);
		}
	}
	if (remaining_count > 0) {
		result.Slice(left, remaining_sel, remaining_count);
		for (idx_t idx = left.ColumnCount(); idx < result.ColumnCount(); idx++) {
			if( right_col_map[idx] != std::numeric_limits<uint32_t>::max() ) {
				result.data[right_col_map[idx]].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result.data[right_col_map[idx]], true);
			}
		}
	}
}

// OperatorResultType PhysicalNestedLoopJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input,
//                                                               DataChunk &chunk, OperatorState &state_p) const {
// 	auto &state = (PhysicalNestedLoopJoinState &)state_p;
// 	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

// 	idx_t match_count;
// 	do {
// 		if (state.fetch_next_right) {
// 			// we exhausted the chunk on the right: move to the next chunk on the right
// 			state.right_chunk++;
// 			state.left_tuple = 0;
// 			state.right_tuple = 0;
// 			state.fetch_next_right = false;
// 			// check if we exhausted all chunks on the RHS
// 			if (state.right_chunk >= gstate.right_chunks.ChunkCount()) {
// 				state.fetch_next_left = true;
// 				// we exhausted all chunks on the right: move to the next chunk on the left
// 				if (IsLeftOuterJoin(join_type)) {
// 					// left join: before we move to the next chunk, see if we need to output any vectors that didn't
// 					// have a match found
// 					PhysicalJoin::ConstructLeftJoinResult(input, chunk, state.left_found_match.get());
// 					memset(state.left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
// 				}
// 				return OperatorResultType::NEED_MORE_INPUT;
// 			}
// 		}
// 		if (state.fetch_next_left) {
// 			// resolve the left join condition for the current chunk
// 			state.left_condition.Reset();
// 			state.lhs_executor.Execute(input, state.left_condition);

// 			state.left_tuple = 0;
// 			state.right_tuple = 0;
// 			state.right_chunk = 0;
// 			state.fetch_next_left = false;
// 		}
// 		// now we have a left and a right chunk that we can join together
// 		// note that we only get here in the case of a LEFT, INNER or FULL join
// 		auto &left_chunk = input;
// 		auto &right_chunk = gstate.right_chunks.GetChunk(state.right_chunk);
// 		auto &right_data = gstate.right_data.GetChunk(state.right_chunk);

// 		// sanity check
// 		left_chunk.Verify();
// 		right_chunk.Verify();
// 		right_data.Verify();

// 		// now perform the join
// 		SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
// 		match_count = NestedLoopJoinInner::Perform(state.left_tuple, state.right_tuple, state.left_condition,
// 		                                           right_chunk, lvector, rvector, conditions);
// 		// we have finished resolving the join conditions
// 		if (match_count > 0) {
// 			// we have matching tuples!
// 			// construct the result
// 			if (state.left_found_match) {
// 				for (idx_t i = 0; i < match_count; i++) {
// 					state.left_found_match[lvector.get_index(i)] = true;
// 				}
// 			}
// 			if (gstate.right_found_match) {
// 				for (idx_t i = 0; i < match_count; i++) {
// 					gstate.right_found_match[state.right_chunk * STANDARD_VECTOR_SIZE + rvector.get_index(i)] = true;
// 				}
// 			}
// 			chunk.Slice(input, lvector, match_count);
// 			chunk.Slice(right_data, rvector, match_count, input.ColumnCount());
// 		}

// 		// check if we exhausted the RHS, if we did we need to move to the next right chunk in the next iteration
// 		if (state.right_tuple >= right_chunk.size()) {
// 			state.fetch_next_right = true;
// 		}
// 	} while (match_count == 0);
// 	return OperatorResultType::HAVE_MORE_OUTPUT;
// }

// //===--------------------------------------------------------------------===//
// // Source
// //===--------------------------------------------------------------------===//
// class NestedLoopJoinScanState : public GlobalSourceState {
// public:
// 	explicit NestedLoopJoinScanState(const PhysicalNestedLoopJoin &op) : op(op), right_outer_position(0) {
// 	}

// 	mutex lock;
// 	const PhysicalNestedLoopJoin &op;
// 	idx_t right_outer_position;

// public:
// 	idx_t MaxThreads() override {
// 		auto &sink = (NestedLoopJoinGlobalState &)*op.sink_state;
// 		return sink.right_chunks.Count() / (STANDARD_VECTOR_SIZE * 10);
// 	}
// };

// unique_ptr<GlobalSourceState> PhysicalNestedLoopJoin::GetGlobalSourceState(ClientContext &context) const {
// 	return make_unique<NestedLoopJoinScanState>(*this);
// }

// void PhysicalNestedLoopJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
//                                      LocalSourceState &lstate) const {
// 	D_ASSERT(IsRightOuterJoin(join_type));
// 	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
// 	auto &sink = (NestedLoopJoinGlobalState &)*sink_state;
// 	auto &state = (NestedLoopJoinScanState &)gstate;

// 	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
// 	// still need to output
// 	lock_guard<mutex> l(state.lock);
// 	ConstructFullOuterJoinResult(sink.right_found_match.get(), sink.right_data, chunk, state.right_outer_position);
// }

} // namespace duckdb
