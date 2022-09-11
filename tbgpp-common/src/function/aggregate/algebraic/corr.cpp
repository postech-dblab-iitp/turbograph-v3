#include "function/aggregate/algebraic_functions.hpp"
#include "function/aggregate/algebraic/covar.hpp"
#include "function/aggregate/algebraic/stddev.hpp"
#include "function/aggregate/algebraic/corr.hpp"
#include "function/function_set.hpp"

namespace duckdb {
void Corr::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("corr");
	corr.AddFunction(AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(corr);
}
} // namespace duckdb
