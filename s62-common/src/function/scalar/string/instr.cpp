#include "function/scalar/string_functions.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "storage/statistics/string_statistics.hpp"
#include "third_party/utf8proc/utf8proc.hpp"

namespace s62 {

struct InstrOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA haystack, TB needle) {
		int64_t string_position = 0;

		auto location = ContainsFun::Find(haystack, needle);
		if (location != DConstants::INVALID_INDEX) {
			auto len = (duckdb::utf8proc_ssize_t)location;
			auto str = reinterpret_cast<const duckdb::utf8proc_uint8_t *>(haystack.GetDataUnsafe());
			D_ASSERT(len <= (duckdb::utf8proc_ssize_t)haystack.GetSize());
			for (++string_position; len > 0; ++string_position) {
				duckdb::utf8proc_int32_t codepoint;
				auto bytes = duckdb::utf8proc_iterate(str, len, &codepoint);
				str += bytes;
				len -= bytes;
			}
		}
		return string_position;
	}
};

struct InstrAsciiOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA haystack, TB needle) {
		auto location = ContainsFun::Find(haystack, needle);
		return location == DConstants::INVALID_INDEX ? 0 : location + 1;
	}
};

static unique_ptr<BaseStatistics> InStrPropagateStats(ClientContext &context, BoundFunctionExpression &expr,
                                                      FunctionData *bind_data,
                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
	D_ASSERT(child_stats.size() == 2);
	// can only propagate stats if the children have stats
	if (!child_stats[0]) {
		return nullptr;
	}
	// for strpos, we only care if the FIRST string has unicode or not
	auto &sstats = (StringStatistics &)*child_stats[0];
	if (!sstats.has_unicode) {
		expr.function.function = ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrAsciiOperator>;
	}
	return nullptr;
}

void InstrFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction instr("instr",                                      // name of the function
	                     {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                     LogicalType::BIGINT,                          // return type
	                     ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator>, false, nullptr,
	                     nullptr, InStrPropagateStats);
	set.AddFunction(instr);
	instr.name = "strpos";
	set.AddFunction(instr);
	instr.name = "position";
	set.AddFunction(instr);
}

} // namespace s62
