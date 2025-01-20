//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/function_set.hpp"
#include "function/scalar_function.hpp"

namespace s62 {

struct AddFun {
	static ScalarFunction GetFunction(const LogicalType &type);
	static ScalarFunction GetFunction(const LogicalType &left_type, const LogicalType &right_type);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SubtractFun {
	static ScalarFunction GetFunction(const LogicalType &type);
	static ScalarFunction GetFunction(const LogicalType &left_type, const LogicalType &right_type);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MultiplyFun {
	static ScalarFunction GetFunction(const LogicalType &type);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DivideFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ModFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LeftShiftFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RightShiftFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseAndFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseOrFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseXorFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitwiseNotFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace s62
