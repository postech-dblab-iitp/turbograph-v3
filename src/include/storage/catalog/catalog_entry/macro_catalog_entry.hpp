//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
/*
#include "storage/catalog/catalog_set.hpp"
#include "storage/catalog/standard_entry.hpp"
#include "function/macro_function.hpp"
#include "parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

//! A macro function in the catalog
class MacroCatalogEntry : public StandardEntry {
public:
	MacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info);
	//! The macro function
	unique_ptr<MacroFunction> function;

public:
	//! Serialize the meta information
	virtual void Serialize(Serializer &serializer) = 0;
};

} // namespace duckdb
*/