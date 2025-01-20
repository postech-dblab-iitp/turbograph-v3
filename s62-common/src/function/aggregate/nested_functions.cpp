#include "function/aggregate/nested_functions.hpp"

namespace s62 {

void BuiltinFunctions::RegisterNestedAggregates() {
	Register<ListFun>();
	// Register<HistogramFun>();
}

} // namespace s62
