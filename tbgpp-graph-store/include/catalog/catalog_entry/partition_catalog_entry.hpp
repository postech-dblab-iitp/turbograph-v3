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
//#include "parser/column_definition.hpp"
//#include "parser/constraint.hpp"
//#include "planner/bound_constraint.hpp"
//#include "planner/expression.hpp"
#include "common/case_insensitive_map.hpp"
#include "catalog/inverted_index.hpp"

namespace duckdb {

class ColumnStatistics;
class DataTable;
struct CreateTableInfo;
struct CreatePartitionInfo;

struct RenameColumnInfo;
struct AddColumnInfo;
struct RemoveColumnInfo;
struct SetDefaultInfo;
struct ChangeColumnTypeInfo;
struct AlterForeignKeyInfo;

//! A partition catalog entry
class PartitionCatalogEntry : public StandardEntry {
public:
	//! Create a real PartitionCatalogEntry
	PartitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePartitionInfo *info);

	inverted_index_t<PropertyKeyID, PropertySchemaID> property_schema_index;
	//vector<Constraints> constraints;

public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	void AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> property_schemas);

	//! Returns a list of types of the table
	//vector<LogicalType> GetTypes();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;


};
} // namespace duckdb
