#include "function/scalar/string_functions.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "storage/statistics/string_statistics.hpp"
#include "utf8proc.hpp"

namespace duckdb {

// length returns the size in characters
struct StringLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return LengthFun::Length<TA, TR>(input);
	}
};

struct ArrayLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input.length;
	}
};

struct ArrayLengthBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB dimension) {
		if (dimension != 1) {
			throw NotImplementedException("array_length for dimensions other than 1 not implemented");
		}
		return input.length;
	}
};

struct PathLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (input.length - 1)/2;
	}
};

// strlen returns the size in bytes
struct StrLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input.GetSize();
	}
};

// bitlen returns the size in bits
struct BitLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 8 * input.GetSize();
	}
};

static unique_ptr<BaseStatistics> LengthPropagateStats(ClientContext &context, BoundFunctionExpression &expr,
                                                       FunctionData *bind_data,
                                                       vector<unique_ptr<BaseStatistics>> &child_stats) {
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	if (!child_stats[0]) {
		return nullptr;
	}
	auto &sstats = (StringStatistics &)*child_stats[0];
	if (!sstats.has_unicode) {
		expr.function.function = ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>;
	}
	return nullptr;
}

static unique_ptr<FunctionData> ListLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction array_length_unary = ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY)}, LogicalType::BIGINT,
	    ScalarFunction::UnaryFunction<list_entry_t, int64_t, ArrayLengthOperator>, false, false, ListLengthBind);
	ScalarFunctionSet length("length");
	length.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                  ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator>, false,
	                                  false, nullptr, nullptr, LengthPropagateStats));
	length.AddFunction(array_length_unary);
	set.AddFunction(length);
	length.name = "len";
	set.AddFunction(length);

	ScalarFunctionSet array_length("array_length");
	array_length.AddFunction(array_length_unary);
	array_length.AddFunction(
	    ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<list_entry_t, int64_t, int64_t, ArrayLengthBinaryOperator>, false,
	                   false, ListLengthBind));
	set.AddFunction(array_length);

	ScalarFunctionSet path_length("path_length");
	ScalarFunction path_length_unary = ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY)}, LogicalType::UBIGINT,
	    ScalarFunction::UnaryFunction<list_entry_t, uint64_t, PathLengthOperator>, false, false, ListLengthBind);
	path_length.AddFunction(path_length_unary);
	path_length.AddFunction(
		ScalarFunction({LogicalType::PATH(LogicalType::ANY)}, LogicalType::UBIGINT,
					   ScalarFunction::UnaryFunction<list_entry_t, uint64_t, PathLengthOperator>, false,
					   false, ListLengthBind));
	set.AddFunction(path_length);

	set.AddFunction(ScalarFunction("strlen", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>));
	set.AddFunction(ScalarFunction("bit_length", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, BitLenOperator>));
	// length for BLOB type
	set.AddFunction(ScalarFunction("octet_length", {LogicalType::BLOB}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator>));
}

struct UnicodeOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		auto str = reinterpret_cast<const utf8proc_uint8_t *>(input.GetDataUnsafe());
		auto len = input.GetSize();
		utf8proc_int32_t codepoint;
		(void)utf8proc_iterate(str, len, &codepoint);
		return codepoint;
	}
};

void UnicodeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction unicode("unicode", {LogicalType::VARCHAR}, LogicalType::INTEGER,
	                       ScalarFunction::UnaryFunction<string_t, int32_t, UnicodeOperator>);
	set.AddFunction(unicode);
	unicode.name = "ord";
	set.AddFunction(unicode);
}

} // namespace duckdb
