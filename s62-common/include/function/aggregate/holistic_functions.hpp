//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/holistic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"
#include "function/function_set.hpp"

namespace s62 {

struct QuantileFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ModeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ApproximateQuantileFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReservoirQuantileFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace s62
