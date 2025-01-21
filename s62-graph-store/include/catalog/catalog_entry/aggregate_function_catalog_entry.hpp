#pragma once

#include "catalog/standard_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"

namespace s62 {

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public StandardEntry {
public:
	AggregateFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateAggregateFunctionInfo *info, const void_allocator &void_alloc)
	    : StandardEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, schema, catalog, info->name, void_alloc),
	      functions(move(info->functions)) {
	}

	unique_ptr<AggregateFunctionSet> functions;

	void SetFunctions(unique_ptr<AggregateFunctionSet> set_ptr) {
		functions.release();
		functions = move(set_ptr);
	}
};
} // namespace s62
