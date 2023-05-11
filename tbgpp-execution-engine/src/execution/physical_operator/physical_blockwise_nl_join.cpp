#include "execution/physical_operator/physical_blockwise_nl_join.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "execution/physical_operator/physical_comparison_join.hpp"

namespace duckdb {

PhysicalBlockwiseNLJoin::PhysicalBlockwiseNLJoin(Schema& sch, unique_ptr<Expression> condition, JoinType join_type, vector<uint32_t> &outer_col_map_p, vector<uint32_t> &inner_col_map_p)
    : PhysicalJoin(sch, PhysicalOperatorType::BLOCKWISE_NL_JOIN, join_type),
      condition(move(condition)), outer_col_map(move(outer_col_map_p)), inner_col_map(move(inner_col_map_p)) {
	// MARK and SINGLE joins not handled
	D_ASSERT(join_type != JoinType::MARK);
	D_ASSERT(join_type != JoinType::SINGLE);

	D_ASSERT(join_type != JoinType::RIGHT); // s62 currently on right join
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinLocalState : public LocalSinkState {
public:
	ChunkCollection right_chunks;
	//! Whether or not a tuple on the RHS has found a match, only used for FULL OUTER joins
	unique_ptr<bool[]> rhs_found_match;
	BlockwiseNLJoinLocalState() {
	}
};

// class BlockwiseNLJoinGlobalState : public GlobalSinkState {
// public:
// 	mutex lock;
// 	ChunkCollection right_chunks;
// 	//! Whether or not a tuple on the RHS has found a match, only used for FULL OUTER joins
// 	unique_ptr<bool[]> rhs_found_match;
// };

unique_ptr<LocalSinkState> PhysicalBlockwiseNLJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BlockwiseNLJoinLocalState>();
}

SinkResultType PhysicalBlockwiseNLJoin::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &state) const {
	auto& lstate = (BlockwiseNLJoinLocalState &)state;
	lstate.right_chunks.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

// SinkFinalizeType PhysicalBlockwiseNLJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
//                                                    GlobalSinkState &gstate_p) const {
// 	auto &gstate = (BlockwiseNLJoinGlobalState &)gstate_p;
// 	if (IsRightOuterJoin(join_type)) {
// 		gstate.rhs_found_match = unique_ptr<bool[]>(new bool[gstate.right_chunks.Count()]);
// 		memset(gstate.rhs_found_match.get(), 0, sizeof(bool) * gstate.right_chunks.Count());
// 	}
// 	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
// 		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
// 	}
// 	return SinkFinalizeType::READY;
// }

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinState : public OperatorState {
public:
	explicit BlockwiseNLJoinState(const PhysicalBlockwiseNLJoin &op)
	    : left_position(0), right_position(0), executor(*op.condition) {
		if (IsLeftOuterJoin(op.join_type)) {
			left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
			memset(left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		}
	}

	//! Whether or not a tuple on the LHS has found a match, only used for LEFT OUTER and FULL OUTER joins
	unique_ptr<bool[]> left_found_match;
	idx_t left_position;
	idx_t right_position;
	ExpressionExecutor executor;
};

unique_ptr<OperatorState> PhysicalBlockwiseNLJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<BlockwiseNLJoinState>(*this);
}

OperatorResultType PhysicalBlockwiseNLJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    OperatorState &state_p, LocalSinkState &sink_state) const {
	D_ASSERT(input.size() > 0);
	auto &state = (BlockwiseNLJoinState &)state_p;
	auto &gstate = (BlockwiseNLJoinLocalState &)sink_state;

	if (gstate.right_chunks.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			//PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk);
			PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk, inner_col_map, outer_col_map);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	// now perform the actual join
	// we construct a combined DataChunk by referencing the LHS and the RHS
	// every step that we do not have output results we shift the vectors of the RHS one up or down
	// this creates a new "alignment" between the tuples, exhausting all possible O(n^2) combinations
	// while allowing us to use vectorized execution for every step
	idx_t result_count = 0;
	do {
		if (state.left_position >= input.size()) {
			// exhausted LHS, have to pull new LHS chunk
			if (state.left_found_match) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				PhysicalJoin::ConstructLeftJoinResult(input, chunk, state.left_found_match.get(), inner_col_map);
				memset(state.left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			state.left_position = 0;
			state.right_position = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		}
		auto &lchunk = input;
		auto &rchunk = gstate.right_chunks.GetChunk(state.right_position);

		// fill in the current element of the LHS into the chunk
		D_ASSERT(chunk.ColumnCount() == lchunk.ColumnCount() + rchunk.ColumnCount());
		for (idx_t i = 0; i < lchunk.ColumnCount(); i++) {
			ConstantVector::Reference(chunk.data[outer_col_map[i]], lchunk.data[i], state.left_position, lchunk.size());
		}
		// for the RHS we just reference the entire vector
		for (idx_t i = 0; i < rchunk.ColumnCount(); i++) {
			chunk.data[inner_col_map[i]].Reference(rchunk.data[i]);
		}
		chunk.SetCardinality(rchunk.size());

		// now perform the computation
		SelectionVector match_sel(STANDARD_VECTOR_SIZE);
		result_count = state.executor.SelectExpression(chunk, match_sel);
		if (result_count > 0) {
			// found a match!
			// set the match flags in the LHS
			if (state.left_found_match) {
				state.left_found_match[state.left_position] = true;
			}
			// set the match flags in the RHS
			if (gstate.rhs_found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					auto idx = match_sel.get_index(i);
					gstate.rhs_found_match[state.right_position * STANDARD_VECTOR_SIZE + idx] = true;
				}
			}
			chunk.Slice(match_sel, result_count);
		} else {
			// no result: reset the chunk
			chunk.Reset();
		}
		// move to the next tuple on the LHS
		state.left_position++;
		if (state.left_position >= input.size()) {
			// exhausted the current chunk, move to the next RHS chunk
			state.right_position++;
			if (state.right_position < gstate.right_chunks.ChunkCount()) {
				// we still have chunks left! start over on the LHS
				state.left_position = 0;
			}
		}
	} while (result_count == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

string PhysicalBlockwiseNLJoin::ParamsToString() const {
	string extra_info = JoinTypeToString(join_type) + "\n";
	extra_info += condition->GetName();
	return extra_info;
}

// //===--------------------------------------------------------------------===//
// // Source - only for right outer join
// //===--------------------------------------------------------------------===//
// class BlockwiseNLJoinScanState : public GlobalSourceState {
// public:
// 	explicit BlockwiseNLJoinScanState(const PhysicalBlockwiseNLJoin &op) : op(op), right_outer_position(0) {
// 	}

// 	mutex lock;
// 	const PhysicalBlockwiseNLJoin &op;
// 	//! The position in the RHS in the final scan of the FULL OUTER JOIN
// 	idx_t right_outer_position;

// public:
// 	idx_t MaxThreads() override {
// 		auto &sink = (BlockwiseNLJoinGlobalState &)*op.sink_state;
// 		return sink.right_chunks.Count() / (STANDARD_VECTOR_SIZE * 10);
// 	}
// };

// unique_ptr<GlobalSourceState> PhysicalBlockwiseNLJoin::GetGlobalSourceState(ClientContext &context) const {
// 	return make_unique<BlockwiseNLJoinScanState>(*this);
// }

// void PhysicalBlockwiseNLJoin::GetData(ExecutionContext &context, DataChunk &chunk, 
//                                       LocalSourceState &lstate, LocalSinkState &sink_state) const {
// 	D_ASSERT(IsRightOuterJoin(join_type));
// 	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
// 	auto &sink = (BlockwiseNLJoinGlobalState &)*sink_state;
// 	auto &state = (BlockwiseNLJoinScanState &)gstate;

// 	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
// 	// still need to output
// 	lock_guard<mutex> l(state.lock);
// 	PhysicalComparisonJoin::ConstructFullOuterJoinResult(sink.rhs_found_match.get(), sink.right_chunks, chunk,
// 	                                                     state.right_outer_position);
// }

} // namespace duckdb
