#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "function/aggregate/regression_functions.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "function/aggregate/regression/regr_count.hpp"
#include "function/function_set.hpp"

namespace s62 {

void RegrCountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("regr_count");
	corr.AddFunction(AggregateFunction::BinaryAggregate<size_t, double, double, uint32_t, RegrCountFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::UINTEGER));
	set.AddFunction(corr);
}

} // namespace s62
