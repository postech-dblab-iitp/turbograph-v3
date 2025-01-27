//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/catalog/standard_entry.hpp"
#include "storage/catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"

namespace duckdb {

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public StandardEntry {
public:
	AggregateFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateAggregateFunctionInfo *info, const void_allocator &void_alloc)
	    : StandardEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, schema, catalog, info->name, void_alloc),
	      functions(move(info->functions)) {
	}

	unique_ptr<AggregateFunctionSet> functions; // TODO how..
	//! The aggregate functions
	// vector<AggregateFunction> *functions;

	void SetFunctions(unique_ptr<AggregateFunctionSet> set_ptr) {
		functions.release(); // TODO this is tricky code.. not preferred
		functions = move(set_ptr);
	}
};
} // namespace duckdb
