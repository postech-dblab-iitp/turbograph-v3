#include "execution/expression_executor.hpp"
#include "planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundReferenceExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.index != DConstants::INVALID_INDEX);
	D_ASSERT(expr.index < chunk->ColumnCount());
	// if (chunk->HasRowChunk()) {
	// 	D_ASSERT(false); // TODO disable SchemalessDataChunk
	// 	// SchemalessDataChunk *sch_chunk = (SchemalessDataChunk *)chunk;
	// 	// Vector &vec = sch_chunk->GetIthCol(expr.index);
	// 	// if (sel) {
	// 	// 	result.Slice(vec, *sel, count);
	// 	// } else {
	// 	// 	result.Reference(vec);
	// 	// }
	// } else {
		if (sel) {
			result.Slice(chunk->data[expr.index], *sel, count);
		} else {
			result.Reference(chunk->data[expr.index]);
		}
	// }
}

} // namespace duckdb
