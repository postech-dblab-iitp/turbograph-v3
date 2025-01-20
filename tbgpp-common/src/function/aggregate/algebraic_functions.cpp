#include "function/aggregate/algebraic_functions.hpp"
#include "function/aggregate_function.hpp"

namespace s62 {

void BuiltinFunctions::RegisterAlgebraicAggregates() {
	Register<AvgFun>();

	Register<CovarSampFun>();
	Register<CovarPopFun>();

	Register<StdDevSampFun>();
	Register<StdDevPopFun>();
	Register<VarPopFun>();
	Register<VarSampFun>();
	Register<VarianceFun>();
	Register<StandardErrorOfTheMeanFun>();
	Register<Corr>();
}

} // namespace s62
