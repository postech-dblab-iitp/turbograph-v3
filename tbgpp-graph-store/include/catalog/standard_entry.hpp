//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/standard_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog_entry.hpp"

namespace s62 {
class SchemaCatalogEntry;

//! A StandardEntry is a catalog entry that is a member of a schema
class StandardEntry : public CatalogEntry {
public:
	StandardEntry(CatalogType type, SchemaCatalogEntry *schema, Catalog *catalog, string name, const void_allocator &void_alloc)
	    : CatalogEntry(type, catalog, name, void_alloc), schema(schema) {
	}
	~StandardEntry() override {
	}

	//! The schema the entry belongs to
	SchemaCatalogEntry *schema;
};
} // namespace s62
