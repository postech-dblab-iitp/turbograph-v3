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
	PropertySchemaID_vector property_schema_array;
	//vector<Constraints> constraints;
	PartitionID pid;
	idx_t physical_id_index;
	idx_t_vector adjlist_indexes;
	idx_t_vector property_indexes;
	LogicalTypeId_vector global_property_typesid;
	uint16_t_vector extra_typeinfo_vec;
	string_vector global_property_key_names;
	idx_t num_columns;
	atomic<ExtentID> local_extent_id_version;

public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	void AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> &property_schemas);
	void SetPhysicalIDIndex(idx_t index_oid);
	void AddAdjIndex(idx_t index_oid);
	void AddPropertyIndex(idx_t index_oid);
	void SetTypes(vector<LogicalType> &types);
	void SetKeys(ClientContext &context, vector<string> &key_names);
	void SetPartitionID(PartitionID pid);

	//! Get Property Schema IDs
	void GetPropertySchemaIDs(vector<idx_t> &psids);

	//! Get Property Schema IDs w/o copy
	PropertySchemaID_vector *GetPropertySchemaIDs();
	
	//! Get Catalog OID of Physical ID Index
	idx_t GetPhysicalIDIndexOid();

	//! Get Catalog OIDs of Adjacency Index
	idx_t_vector *GetAdjIndexOidVec();

	//! Get Catalog OIDs of Property Index
	idx_t_vector *GetPropertyIndexOidVec();

	//! Get Number of columns in the universal schema
	uint64_t GetNumberOfColumns() const;

	//! Returns a list of types of the table
	vector<LogicalType> GetTypes();
	
	PartitionID GetPartitionID();
	ExtentID GetNewExtentID();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;
};
} // namespace duckdb
