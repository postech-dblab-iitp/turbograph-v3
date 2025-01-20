//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_aggregate_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_function_info.hpp"
#include "function/function_set.hpp"

namespace s62 {

struct CreateAggregateFunctionInfo : public CreateFunctionInfo {
	// deprecated
	// explicit CreateAggregateFunctionInfo(AggregateFunction function)
	//     : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(function.name) {
	// 	this->name = function.name;
	// 	functions.AddFunction(move(function));
	// }

	explicit CreateAggregateFunctionInfo(unique_ptr<AggregateFunctionSet> set_ptr)
	    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(move(set_ptr)) {
		this->name = functions->name;
		for (auto &func : functions->functions) {
			func.name = functions->name;
		}
	}

	unique_ptr<AggregateFunctionSet> functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		// auto result = make_unique<CreateAggregateFunctionInfo>(functions);
		// CopyProperties(*result);
		// return move(result);
	}
};

} // namespace s62
