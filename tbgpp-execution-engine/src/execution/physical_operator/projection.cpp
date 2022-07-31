// #include "typedef.hpp"

// #include "execution/physical_operator/projection.hpp"
// #include "duckdb/execution/expression_executor.hpp"

// #include <cassert>

// namespace duckdb {

// class ProjectionState : public OperatorState {
// public:
// 	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
// 	    : executor(Allocator::Get(context.client), expressions) {
// 	}
// public:

// 	ExpressionExecutor executor;

// };

// unique_ptr<OperatorState> Projection::GetOperatorState() const {
// 	return make_unique<ProjectionState>(context, select_list);
// }

// OperatorResultType Projection::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
// 	auto &state = (ProjectionState &)state;
// 	// state not necessary

// 	// TODO write here

// 	// always return need_more_input
// 	return OperatorResultType::NEED_MORE_INPUT;
// }

// std::string Projection::ParamsToString() const {
// 	return "projection-param";
// }

// std::string Projection::ToString() const {
// 	return "Projection";
// }


// } // namespace duckdb