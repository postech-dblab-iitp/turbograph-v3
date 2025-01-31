
#include "common/typedef.hpp"

#include "execution/physical_operator/physical_filter.hpp"

#include "execution/expression_executor.hpp"

#include <string>

#include "common/allocator.hpp"

#include "icecream.hpp"

namespace duckdb {

class FilterState : public OperatorState {
public:
	explicit FilterState(Expression& expr):
		executor(expr), sel(STANDARD_VECTOR_SIZE) {
	}
public:
	SelectionVector sel;
	ExpressionExecutor executor;
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_unique<FilterState>(*expression);
}

OperatorResultType PhysicalFilter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (FilterState &)lstate;
	D_ASSERT( input.size() <= STANDARD_VECTOR_SIZE ); // TODO release me
	
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalFilter::ParamsToString() const {
	return "filter-param: " + expression->ToString();
}

std::string PhysicalFilter::ToString() const {
	return "Filter";
}

}