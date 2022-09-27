
#include "typedef.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/expression_executor.hpp"
#include <string>

#include "icecream.hpp"


namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(const vector<unique_ptr<Expression>> &expressions)
	    : executor(expressions) { }
public:
	ExpressionExecutor executor;
};

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_unique<ProjectionState>(expressions);
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (ProjectionState &)lstate;

// icecream::ic.enable();
// for( auto& i: input.GetTypes()) {
// 	IC(i.ToString());
// }
// for( auto& j: chunk.GetTypes()) {
// 	IC(j.ToString());
// }
// icecream::ic.disable();


	// state not necessary
	// IC();
	state.executor.Execute(input, chunk);
	// IC();
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalProjection::ParamsToString() const {
	return "projection-param";
}

std::string PhysicalProjection::ToString() const {
	return "Projection";
}








}