#include "catalog/catalog_entry/index_catalog_entry.hpp"
// #include "duckdb/storage/data_table.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name, void_alloc), index(nullptr) {
}

IndexCatalogEntry::~IndexCatalogEntry() {
	// remove the associated index from the info
	// if (!info || !index) {
	// 	return;
	// }
	// info->indexes.RemoveIndex(index);
}

string IndexCatalogEntry::ToSQL() {
}

} // namespace duckdb
