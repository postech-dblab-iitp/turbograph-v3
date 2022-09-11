//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct ListBindData : public FunctionData {
	ListBindData() {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ListBindData>();
	}
};

struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
struct HistogramFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
} // namespace duckdb
