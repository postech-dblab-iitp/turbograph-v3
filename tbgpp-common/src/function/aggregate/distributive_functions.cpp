#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "function/aggregate_function.hpp"

namespace s62 {

void BuiltinFunctions::RegisterDistributiveAggregates() {
	Register<BitAndFun>();
	Register<BitOrFun>();
	Register<BitXorFun>();
	Register<CountStarFun>();
	Register<CountFun>();
	Register<FirstFun>();
	Register<MaxFun>();
	Register<MinFun>();
	Register<SumFun>();
	Register<StringAggFun>();
	// Register<ApproxCountDistinctFun>();
	Register<ProductFun>();
	Register<BoolOrFun>();
	Register<BoolAndFun>();
	Register<ArgMinFun>();
	Register<ArgMaxFun>();
	Register<SkewFun>();
	Register<KurtosisFun>();
	Register<EntropyFun>();
}

} // namespace s62
