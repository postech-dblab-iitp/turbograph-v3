#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "function/aggregate/regression_functions.hpp"

namespace s62 {

void BuiltinFunctions::RegisterRegressiveAggregates() {
	Register<RegrAvgxFun>();
	Register<RegrAvgyFun>();
	Register<RegrCountFun>();
	Register<RegrSlopeFun>();
	Register<RegrR2Fun>();
	Register<RegrSYYFun>();
	Register<RegrSXXFun>();
	Register<RegrSXYFun>();
	Register<RegrInterceptFun>();
}

} // namespace s62
