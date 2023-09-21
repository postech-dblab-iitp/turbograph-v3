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
#include "common/boost.hpp"
#include "common/boost_typedefs.hpp"

namespace duckdb {

class ColumnStatistics;
class DataTable;
struct CreatePropertySchemaInfo;
struct BoundCreateTableInfo;
struct ExtentCatalogEntry;

struct RenameColumnInfo;
struct AddColumnInfo;
struct RemoveColumnInfo;
struct SetDefaultInfo;
struct ChangeColumnTypeInfo;
struct AlterForeignKeyInfo;

//! A property schema catalog entry
class PropertySchemaCatalogEntry : public StandardEntry {

public:
	//! Create a real GraphCatalogEntry
	PropertySchemaCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info, const void_allocator &void_alloc);

	PartitionID pid;
	idx_t partition_oid; // oid of parent partition
	PropertyKeyID_vector property_keys;
	idx_t_vector extent_ids;
	idx_t_vector key_column_idxs;
	LogicalTypeId_vector property_typesid;
	uint16_t_vector extra_typeinfo_vec;
	string_vector property_key_names;
	LogicalTypeId_vector adjlist_typesid;
	string_vector adjlist_names;
	idx_t num_columns;
	idx_t last_extent_num_tuples;
	
public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	
	void SetTypes(vector<LogicalType> &types);
	void SetKeys(ClientContext &context, vector<string> &key_names);
	void SetKeyColumnIdxs(vector<idx_t> &key_column_idxs_);
	string_vector *GetKeys();
	vector<string> GetKeysWithCopy();
	string GetPropertyKeyName(idx_t i);
	void AppendType(LogicalType type);
	idx_t AppendKey(ClientContext &context, string key_name);
	void AppendAdjListType(LogicalType type);
	idx_t AppendAdjListKey(ClientContext &context, string key_name);
	//! Returns a list of types of the table
	LogicalTypeId_vector *GetTypes();
	uint16_t_vector *GetExtraTypeInfos();
	LogicalTypeId GetType(idx_t i);
	vector<LogicalType> GetTypesWithCopy();
	uint64_t GetTypeSize(idx_t i);
	vector<idx_t> GetColumnIdxs(vector<string> &property_keys);
	vector<idx_t> GetKeyColumnIdxs();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	void AddExtent(ExtentCatalogEntry* extent_cat);
	void AddExtent(ExtentID eid, size_t num_tuples_in_extent = 0);
	PartitionID GetPartitionID();
	idx_t GetPartitionOID();

	uint64_t GetNumberOfColumns();
	uint64_t GetNumberOfRowsApproximately();
	uint64_t GetNumberOfExtents();

	/* histogram */
	void InitializeAccumulators();
	void AccumulateExtent(DataChunk &chunk);
	void FinalizeAccumulators();

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_exists is true, returns DConstants::INVALID_INDEX
	//! If if_exists is false, throws an exception
	//idx_t GetColumnIndex(string &name, bool if_exists = false);

};
} // namespace duckdb
