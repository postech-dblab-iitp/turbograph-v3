#include "execution/expression_executor_state.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression.hpp"

namespace s62 {

void ExpressionState::AddChild(Expression *expr) {
	types.push_back(expr->return_type);
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}

void ExpressionState::Finalize() {
	if (!types.empty()) {
		intermediate_chunk.Initialize(types);
	}
}
ExpressionState::ExpressionState(const Expression &expr, ExpressionExecutorState &root)
    : expr(expr), root(root), name(expr.ToString()) {
}

ExpressionExecutorState::ExpressionExecutorState(const string &name) : profiler(), name(name) {
}
} // namespace s62
