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
#include "parser/parsed_data/create_macro_info.hpp"
#include "storage/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {

//! A macro function in the catalog
class TableMacroCatalogEntry : public MacroCatalogEntry {
public:
	TableMacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info);

public:
	//! Serialize the meta information of the ScalarMacroCatalogEntry
	void Serialize(Serializer &serializer) override;
	//! Deserializes to a CreateMacroInfo
	static unique_ptr<CreateMacroInfo> Deserialize(Deserializer &source);
};

} // namespace duckdb
*/