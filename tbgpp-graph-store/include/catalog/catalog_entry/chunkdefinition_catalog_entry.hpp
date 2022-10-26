//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once


#include "catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
//#include "parser/constraint.hpp"
//#include "planner/bound_constraint.hpp"
//#include "planner/expression.hpp"
#include "common/case_insensitive_map.hpp"

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

#define MIN_MAX_ARRAY_SIZE 1024

struct minmax_t {
	idx_t min;
	idx_t max;
};

//! A chunk definition catalog entry
class ChunkDefinitionCatalogEntry : public StandardEntry {
	typedef boost::interprocess::allocator<minmax_t, segment_manager_t> minmax_allocator;
	typedef boost::interprocess::vector<minmax_t, minmax_allocator> minmax_t_vector;
public:
	//! Create a real GraphCatalogEntry
	ChunkDefinitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info, const void_allocator &void_alloc);

	LogicalTypeId data_type_id; // TODO SHM
	CompressionType compression_type = CompressionType::COMPRESSION_AUTO; // TODO SHM
	bool is_min_max_array_exist = false; // TODO SHM
	size_t num_entries_in_column; // TODO SHM
	minmax_t_vector min_max_array;
public:

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);
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

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_exists is true, returns DConstants::INVALID_INDEX
	//! If if_exists is false, throws an exception
	//idx_t GetColumnIndex(string &name, bool if_exists = false);

};
} // namespace duckdb
