#include "function/scalar/date_functions.hpp"
#include "common/types/interval.hpp"
#include "common/types/time.hpp"
#include "common/types/timestamp.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/vector_operations/unary_executor.hpp"
#include "common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void AgeFunctionStandard(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	auto current_timestamp = Timestamp::GetCurrentTimestamp();

	UnaryExecutor::Execute<timestamp_t, interval_t>(input.data[0], result, input.size(), [&](timestamp_t input) {
		return Interval::GetAge(current_timestamp, input);
	});
}

static void AgeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 2);

	BinaryExecutor::Execute<timestamp_t, timestamp_t, interval_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [&](timestamp_t input1, timestamp_t input2) { return Interval::GetAge(input1, input2); });
}

void AgeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet age("age");
	age.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::INTERVAL, AgeFunctionStandard));
	age.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP}, LogicalType::INTERVAL, AgeFunction));
	set.AddFunction(age);
}

} // namespace duckdb
