//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/enums/catalog_type.hpp"
#include "common/exception.hpp"
#include "common/atomic.hpp"
#include "common/boost.hpp"
#include "common/boost_typedefs.hpp"

#include <memory>

namespace duckdb {
struct AlterInfo;
class Catalog;
class CatalogSet;
class ClientContext;

//! Abstract base class of an entry in the catalog
class CatalogEntry {
public:
	// CatalogEntry(CatalogType type, Catalog *catalog, string name);
	CatalogEntry(CatalogType type, Catalog *catalog, string name, const void_allocator &void_alloc);
	virtual ~CatalogEntry();

	//! The oid of the entry
	idx_t oid;
	//! The type of this catalog entry
	CatalogType type;
	//! Reference to the catalog this entry belongs to
	Catalog *catalog;
	//! Reference to the catalog set this entry is stored in
	CatalogSet *set;
	//! The name of the entry
	char_string name;
	//! Whether or not the object is deleted
	bool deleted;
	//! Whether or not the object is temporary and should not be added to the WAL
	bool temporary;
	//! Whether or not the entry is an internal entry (cannot be deleted, not dumped, etc)
	bool internal;
	//! Timestamp at which the catalog entry was created
	atomic<transaction_t> timestamp;
	//! Child entry
	//unique_ptr<CatalogEntry> child;
	boost::interprocess::offset_ptr<CatalogEntry> child;
	//! Parent entry (the node that dependents_map this node)
	//CatalogEntry *parent;
	boost::interprocess::offset_ptr<CatalogEntry> parent;

public:
	idx_t GetOid() { return oid; }
	string GetName() { return string(name); }

	virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info);

	virtual unique_ptr<CatalogEntry> Copy(ClientContext &context);

	//! Sets the CatalogEntry as the new root entry (i.e. the newest entry)
	// this is called on a rollback to an AlterEntry
	virtual void SetAsRoot();

	//! Convert the catalog entry to a SQL string that can be used to re-construct the catalog entry
	virtual string ToSQL();

	void SetCatalog(Catalog *catalog) { this->catalog = catalog; }
};
} // namespace duckdb
