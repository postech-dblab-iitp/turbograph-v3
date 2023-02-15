//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/index_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/standard_entry.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "common/boost_typedefs.hpp"

namespace duckdb {

// struct DataTableInfo;
class Index;

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info, const void_allocator &void_alloc);
	~IndexCatalogEntry() override;

	Index *index;
	// shared_ptr<DataTableInfo> info;
	// string sql;

public:
	string ToSQL() override;
};

} // namespace duckdb
