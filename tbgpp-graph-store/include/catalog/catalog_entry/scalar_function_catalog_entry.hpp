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

namespace duckdb {

//! A table function in the catalog
class ScalarFunctionCatalogEntry : public StandardEntry {
public:
	ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateScalarFunctionInfo *info, const void_allocator &void_alloc)
	    : StandardEntry(CatalogType::SCALAR_FUNCTION_ENTRY, schema, catalog, info->name, void_alloc), functions(info->functions) {
	}

	//! The scalar functions
	vector<ScalarFunction> functions;
};
} // namespace duckdb
