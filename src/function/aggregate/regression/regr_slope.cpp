// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double

#include "function/aggregate/regression/regr_slope.hpp"
#include "function/function_set.hpp"
#include "function/aggregate/regression_functions.hpp"

namespace duckdb {

void RegrSlopeFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_slope");
	fun.AddFunction(AggregateFunction::BinaryAggregate<RegrSlopeState, double, double, double, RegrSlopeOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

} // namespace duckdb
