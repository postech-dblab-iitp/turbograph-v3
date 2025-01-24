#include "catalog/catalog_entry/index_catalog_entry.hpp"
// #include "duckdb/storage/data_table.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name, void_alloc), index(nullptr), index_type(info->index_type),
	pid(info->partition_oid), psid(info->propertyschema_oid), adj_col_idx(info->adj_col_idx), index_key_columns(void_alloc) {
	for (idx_t i = 0; i < info->column_ids.size(); i++)
		index_key_columns.push_back(info->column_ids[i]);
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

idx_t IndexCatalogEntry::GetPartitionID() {
	return pid;
}

idx_t IndexCatalogEntry::GetPropertySchemaID() {
	return psid;
}

int64_t_vector *IndexCatalogEntry::GetIndexKeyColumns() {
	return &index_key_columns;
}

IndexType IndexCatalogEntry::GetIndexType() {
	return index_type;
}

idx_t IndexCatalogEntry::GetAdjColIdx() {
	return adj_col_idx;
}

} // namespace duckdb
