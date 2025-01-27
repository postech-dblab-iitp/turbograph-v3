//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once


#include "storage/catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
#include "common/case_insensitive_map.hpp"
#include "common/boost_typedefs.hpp"

namespace duckdb {

class ColumnStatistics;
class DataTable;
struct CreateTableInfo;
struct BoundCreateTableInfo;
struct CreateChunkDefinitionInfo;

struct RenameColumnInfo;
struct AddColumnInfo;
struct RemoveColumnInfo;
struct SetDefaultInfo;
struct ChangeColumnTypeInfo;
struct AlterForeignKeyInfo;

//! A chunk definition catalog entry
class ChunkDefinitionCatalogEntry : public StandardEntry {
public:
	//! Create a real GraphCatalogEntry
	ChunkDefinitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info, const void_allocator &void_alloc);

	LogicalTypeId data_type_id; // TODO SHM
	CompressionType compression_type = CompressionType::COMPRESSION_AUTO; // TODO SHM
	bool is_min_max_array_exist = false; // TODO SHM
	size_t num_entries_in_column; // TODO SHM

	/**
	 * Currently, min max array is supported for numeric types only, which can be represented by idx_t.
	 * Therefore, float and double are not supported.
	 * To do this, we need complex data type define (for example, union).
	 * For now, we just use idx_t.
	 */

	minmax_t_vector min_max_array;
public:

	void SetNumEntriesInColumn(size_t num_entries_in_column_) {
		num_entries_in_column = num_entries_in_column_;
	}
	size_t GetNumEntriesInColumn() {
		return num_entries_in_column;
	}
	void CreateMinMaxArray(Vector &column, size_t input_size);
	bool IsMinMaxArrayExist() {
		return is_min_max_array_exist;
	}
	vector<minmax_t> GetMinMaxArray();

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;
};
} // namespace duckdb
