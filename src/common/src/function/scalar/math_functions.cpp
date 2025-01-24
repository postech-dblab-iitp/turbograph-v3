#include "function/scalar/math_functions.hpp"
#include "common/exception.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterMathFunctions() {
	Register<AbsFun>();
	Register<SignFun>();

	Register<CeilFun>();
	Register<FloorFun>();
	Register<RoundFun>();

	Register<DegreesFun>();
	Register<RadiansFun>();

	Register<CbrtFun>();
	Register<ExpFun>();
	Register<Log2Fun>();
	Register<Log10Fun>();
	Register<LnFun>();
	Register<PowFun>();
	Register<RandomFun>();
	Register<SetseedFun>();
	Register<SqrtFun>();

	Register<PiFun>();

	Register<BitCountFun>();

	Register<GammaFun>();
	Register<LogGammaFun>();

	Register<FactorialFun>();

	Register<NextAfterFun>();

	Register<EvenFun>();

	Register<IsNanFun>();
	Register<IsInfiniteFun>();
	Register<IsFiniteFun>();
}

} // namespace duckdb
