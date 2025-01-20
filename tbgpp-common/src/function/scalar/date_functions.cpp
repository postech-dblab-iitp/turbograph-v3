#include "function/scalar/date_functions.hpp"

namespace s62 {

void BuiltinFunctions::RegisterDateFunctions() {
	Register<AgeFun>();
	Register<DateDiffFun>();
	Register<DatePartFun>();
	Register<DateSubFun>();
	Register<DateTruncFun>();
	// Register<CurrentTimeFun>();
	// Register<CurrentDateFun>();
	// Register<CurrentTimestampFun>();
	Register<EpochFun>();
	Register<MakeDateFun>();
	Register<StrfTimeFun>();
	Register<StrpTimeFun>();
	Register<ToIntervalFun>();
}

} // namespace s62
