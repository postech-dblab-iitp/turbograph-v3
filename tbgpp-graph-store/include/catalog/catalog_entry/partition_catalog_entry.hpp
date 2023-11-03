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
	idx_t src_part_oid;
	idx_t dst_part_oid;
	idx_t_vector adjlist_indexes;
	idx_t_vector property_indexes;
	LogicalTypeId_vector global_property_typesid;
	uint16_t_vector extra_typeinfo_vec;
	string_vector global_property_key_names;
	idx_t num_columns;
	atomic<ExtentID> local_extent_id_version;

public:
	void AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> &property_schemas);
	void AddAdjIndex(idx_t index_oid);
	void AddPropertyIndex(idx_t index_oid);
	void SetPhysicalIDIndex(idx_t index_oid);
	void SetTypes(vector<LogicalType> &types);
	void SetKeys(ClientContext &context, vector<string> &key_names);
	void SetSrcDstPartOid(idx_t src_part_oid, idx_t dst_part_oid);

	void GetPropertySchemaIDs(vector<idx_t> &psids);
	uint64_t GetNumberOfColumns() const;
	idx_t GetPhysicalIDIndexOid();
	idx_t GetSrcPartOid();
	idx_t GetDstPartOid();
	idx_t_vector *GetAdjIndexOidVec();
	idx_t_vector *GetPropertyIndexOidVec();

	//! Returns a list of types of the table
	void SetPartitionID(PartitionID pid);
	PartitionID GetPartitionID();
	ExtentID GetNewExtentID();

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;
};
} // namespace duckdb
