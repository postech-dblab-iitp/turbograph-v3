//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_function_info.hpp"
#include "function/scalar_function.hpp"
#include "function/function_set.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	// deprecated
	// explicit CreateScalarFunctionInfo(ScalarFunction function)
	//     : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY) {
	// 	this->name = function.name;
	// 	functions.push_back(function);
	// }
	explicit CreateScalarFunctionInfo(unique_ptr<ScalarFunctionSet> set_ptr)
	    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY), functions(move(set_ptr)) {
		this->name = functions->name;
		for (auto &func : functions->functions) {
			func.name = functions->name;
		}
	}

	unique_ptr<ScalarFunctionSet> functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		// ScalarFunctionSet set(name);
		// // set.functions = functions;
		// auto result = make_unique<CreateScalarFunctionInfo>(move(set));
		// CopyProperties(*result);
		// return move(result);
	}
};

} // namespace duckdb
