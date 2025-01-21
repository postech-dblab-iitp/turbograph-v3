#pragma once

#include "catalog/standard_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "common/boost_typedefs.hpp"

namespace s62 {

//! A table function in the catalog
class ScalarFunctionCatalogEntry : public StandardEntry {
public:
	ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateScalarFunctionInfo *info, const void_allocator &void_alloc)
	    : StandardEntry(CatalogType::SCALAR_FUNCTION_ENTRY, schema, catalog, info->name, void_alloc), functions(move(info->functions)) {
	}

	//! The scalar functions
	unique_ptr<ScalarFunctionSet> functions;

	void SetFunctions(unique_ptr<ScalarFunctionSet> set_ptr) {
		functions.release();
		functions = move(set_ptr);
	}
};
} // namespace s62
