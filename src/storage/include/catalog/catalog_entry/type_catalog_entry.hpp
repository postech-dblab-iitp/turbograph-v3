//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/type_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
/*
#include "catalog/standard_entry.hpp"
#include "common/mutex.hpp"
#include "parser/parsed_data/create_type_info.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//! A type catalog entry
class TypeCatalogEntry : public StandardEntry {
public:
	//! Create a TypeCatalogEntry and initialize storage for it
	TypeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTypeInfo *info);

	LogicalType user_type;

public:
	//! Serialize the meta information of the TypeCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a TypeCatalogEntry
	static unique_ptr<CreateTypeInfo> Deserialize(Deserializer &source);

	string ToSQL() override;
};
} // namespace duckdb
*/