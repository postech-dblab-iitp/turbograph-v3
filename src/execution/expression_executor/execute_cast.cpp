#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_cast_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.child.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// resolve the child
	state->intermediate_chunk.Reset();

	auto &child = state->intermediate_chunk.data[0];
	auto child_state = state->child_states[0].get();

	Execute(*expr.child, child_state, sel, count, child);
	if (expr.try_cast) {
		string error_message;
		VectorOperations::TryCast(child, result, count, &error_message);
	} else {
		// cast it to the type specified by the cast expression
		D_ASSERT(result.GetType() == expr.return_type);
		VectorOperations::Cast(child, result, count);
	}
}

} // namespace duckdb
