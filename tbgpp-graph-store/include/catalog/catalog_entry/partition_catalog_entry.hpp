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
	typedef boost::unordered_map< PropertyKeyID, PropertySchemaID_vector
       	, boost::hash<PropertyKeyID>, std::equal_to<PropertyKeyID>
		, property_to_propertyschemavec_map_value_type_allocator>
	PropertyToPropertySchemaVecUnorderedMap;
public:
	//! Create a real PartitionCatalogEntry
	PartitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePartitionInfo *info, const void_allocator &void_alloc);

	PropertyToPropertySchemaVecUnorderedMap property_schema_index;
	//vector<Constraints> constraints;
	PartitionID pid;

public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	void AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> &property_schemas);

	//! Returns a list of types of the table
	//vector<LogicalType> GetTypes();
	PartitionID GetPartitionID();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;


};
} // namespace duckdb
