//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"
#include "function/function_set.hpp"
#include "common/types/null_value.hpp"

namespace duckdb {

struct BitAndFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitOrFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct BitXorFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CountStarFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct CountFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct BoolAndFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct BoolOrFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct ProductFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct ApproxCountDistinctFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ArgMinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ArgMaxFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct FirstFun {
	static AggregateFunction GetFunction(const LogicalType &type);

	static void RegisterFunction(BuiltinFunctions &set);
};

struct MaxFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MaxByFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MinByFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SumFun {
	static AggregateFunction GetSumAggregate(PhysicalType type);
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SkewFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct KurtosisFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EntropyFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringAggFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
