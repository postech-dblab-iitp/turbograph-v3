//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/standard_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog_entry.hpp"

namespace duckdb {
class SchemaCatalogEntry;

//! A StandardEntry is a catalog entry that is a member of a schema
class StandardEntry : public CatalogEntry {
protected:
	// typedefs for shared memory object
	typedef boost::interprocess::managed_shared_memory::segment_manager segment_manager_t;
	typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
	typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
	typedef boost::interprocess::allocator<idx_t, segment_manager_t> idx_t_allocator;
	typedef boost::interprocess::allocator<char, segment_manager_t> char_allocator;
	typedef boost::interprocess::allocator<transaction_t, segment_manager_t> transaction_t_allocator;
	typedef boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> char_string;

public:
	StandardEntry(CatalogType type, SchemaCatalogEntry *schema, Catalog *catalog, string name)
	    : CatalogEntry(type, catalog, name), schema(schema) {
	}
	~StandardEntry() override {
	}

	//! The schema the entry belongs to
	SchemaCatalogEntry *schema;
};
} // namespace duckdb
