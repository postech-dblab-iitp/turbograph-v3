//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/regression_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"
#include "function/function_set.hpp"
#include "common/types/null_value.hpp"

namespace s62 {
struct RegrAvgxFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrAvgyFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSlopeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrR2Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSXXFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSYYFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrSXYFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegrInterceptFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace s62
