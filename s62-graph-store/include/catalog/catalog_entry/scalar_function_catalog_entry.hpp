//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/standard_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "common/boost_typedefs.hpp"

namespace s62 {

//! A table function in the catalog
class ScalarFunctionCatalogEntry : public StandardEntry {
	// typedef boost::interprocess::allocator<ScalarFunction, segment_manager_t> scalarfunction_allocator;
	// typedef boost::interprocess::vector<ScalarFunction, scalarfunction_allocator> ScalarFunction_vector;
public:
	ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateScalarFunctionInfo *info, const void_allocator &void_alloc)
	    : StandardEntry(CatalogType::SCALAR_FUNCTION_ENTRY, schema, catalog, info->name, void_alloc), functions(move(info->functions)) {
		// for (int i = 0; i < info->functions.size(); i++) {
		// 	functions.push_back(info->functions[i]);
		// }
	}

	//! The scalar functions
	// vector<ScalarFunction> functions;
	unique_ptr<ScalarFunctionSet> functions;

	void SetFunctions(unique_ptr<ScalarFunctionSet> set_ptr) {
		functions.release(); // TODO this is tricky code.. not preferred
		functions = move(set_ptr);
	}
};
} // namespace s62
