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

	PartitionID pid; // foreign key
	PropertyKeyID_vector property_keys;
	idx_t_vector extent_ids;
	atomic<ExtentID> local_extent_id_version;
	
public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	
	//! Returns a list of types of the table
	vector<LogicalType> GetTypes();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	void AddExtent(ExtentCatalogEntry* extent_cat);
	void AddExtent(ExtentID eid);
	ExtentID GetNewExtentID();

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_exists is true, returns DConstants::INVALID_INDEX
	//! If if_exists is false, throws an exception
	//idx_t GetColumnIndex(string &name, bool if_exists = false);

};
} // namespace duckdb
