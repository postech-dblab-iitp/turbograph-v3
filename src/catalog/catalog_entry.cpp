#include "catalog/catalog_entry.hpp"
#include "catalog/catalog.hpp"

namespace duckdb {

// CatalogEntry::CatalogEntry(CatalogType type, Catalog *catalog_p, string name_p) {
// 	// Deprecated
// 	D_ASSERT(false);
// }

CatalogEntry::CatalogEntry(CatalogType type, Catalog *catalog_p, string name_p, const void_allocator &void_alloc)
    : oid(catalog_p->ModifyCatalog()), type(type), catalog(catalog_p), set(nullptr), name(name_p.c_str(), void_alloc), deleted(false),
      temporary(false), internal(false), parent(nullptr) {
}

CatalogEntry::~CatalogEntry() {
}

void CatalogEntry::SetAsRoot() {
}

// LCOV_EXCL_START
unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	throw InternalException("Unsupported alter type for catalog entry!");
}

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext &context) {
	throw InternalException("Unsupported copy type for catalog entry!");
}

string CatalogEntry::ToSQL() {
	throw InternalException("Unsupported catalog type for ToSQL()");
}
// LCOV_EXCL_STOP

} // namespace duckdb
