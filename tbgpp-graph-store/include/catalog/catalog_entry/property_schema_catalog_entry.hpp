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
	PropertySchemaCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema);

	vector<PropertyKeyID> property_keys;
	vector<ExtentID> extent_ids;
	
public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	
	//! Returns a list of types of the table
	vector<LogicalType> GetTypes();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_exists is true, returns DConstants::INVALID_INDEX
	//! If if_exists is false, throws an exception
	//idx_t GetColumnIndex(string &name, bool if_exists = false);

};
} // namespace duckdb
