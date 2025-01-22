#pragma once


#include "catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "common/boost_typedefs.hpp"
#include "common/case_insensitive_map.hpp"
#include "catalog/inverted_index.hpp"

namespace s62 {

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

typedef float StdDev;

//! A partition catalog entry
class PartitionCatalogEntry : public StandardEntry {
public:
	//! Create a real PartitionCatalogEntry
	PartitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePartitionInfo *info, const void_allocator &void_alloc);

	//! PropertyKeyID -> Property schema catalog entries those contains the property
	PropertyToPropertySchemaPairVecUnorderedMap property_schema_index;

	//! OIDs of property schema catalog entries
	PropertySchemaID_vector property_schema_array;
	
	//! Logical partition ID
	PartitionID pid;

	//! OID of the physical ID index (_id)
	idx_t physical_id_index;

	//! OID of the src partition
	idx_t src_part_oid;

	//! OID of the dst partition
	idx_t dst_part_oid;

	//! OID of the universal property schema 
	idx_t univ_ps_oid;

	//! OIDs of the adjacency list indexes
	idx_t_vector adjlist_indexes;

	//! OIDs of the property indexes
	idx_t_vector property_indexes;

	//! Universal schema property Key IDs -> location map
	PropertyToIdxUnorderedMap global_property_key_to_location;

	//! universal schema logical type IDs
	LogicalTypeId_vector global_property_typesid;

	//! extra info vector of universal schema
	uint16_t_vector extra_typeinfo_vec;

	//! universal schema names
	string_vector global_property_key_names;

	//! universal schema property key ids
	idx_t_vector global_property_key_ids;

	//! # of columns in universal schema
	idx_t num_columns;

	//! variable for the local extent ID generator
	atomic<ExtentID> local_extent_id_version;

	//! variable for the local temporal ID generator
	idx_t local_temporal_id_version; 

	//! offset infos
	idx_t_vector offset_infos;

	//! boundary values of the histogram
	idx_t_vector boundary_values;

	//! number of groups for each column
	idx_t_vector num_groups_for_each_column;

	//! precomputed base values for each column
	idx_t_vector multipliers_for_each_column;

	//! which group each table belongs to by column 
	idx_t_vector group_info_for_each_table;

	//! min max value for each column (assume only numeric)
	minmax_t_vector min_max_array;

	//! std dev value for each column (assume only numeric)
	welford_t_vector welford_array;

public:
	void AddPropertySchema(ClientContext &context, idx_t ps_oid, vector<PropertyKeyID> &property_schemas);
	void SetUnivPropertySchema(idx_t psid);
	void AddAdjIndex(idx_t index_oid);
	void AddPropertyIndex(idx_t index_oid);
	void SetPhysicalIDIndex(idx_t index_oid);
	void SetTypes(vector<LogicalType> &types);
	void SetKeys(ClientContext &context, vector<string> &key_names);

	//! Set Universal Schema Info
	void SetSchema(ClientContext &context, vector<string> &key_names, vector<LogicalType> &types, vector<PropertyKeyID> &univ_prop_key_ids);

	void SetPartitionID(PartitionID pid);
	void SetSrcDstPartOid(idx_t src_part_oid, idx_t dst_part_oid);

	idx_t GetUnivPSOid()
	{
		return univ_ps_oid;
	}

	idx_t GetSrcPartOid()
	{
		return src_part_oid;
	}

	idx_t GetDstPartOid()
	{
		return dst_part_oid;
	}

	PropertyToPropertySchemaPairVecUnorderedMap *GetPropertySchemaIndex()
	{
		return &property_schema_index;
	}

	//! Get Property Schema IDs
	void GetPropertySchemaIDs(vector<idx_t> &psids);

	//! Get Property Schema IDs w/o copy
	PropertySchemaID_vector *GetPropertySchemaIDs()
	{
		return &property_schema_array;
	}
	
	//! Get Catalog OID of Physical ID Index
	idx_t GetPhysicalIDIndexOid();

	//! Get Catalog OIDs of Adjacency Index
	idx_t_vector *GetAdjIndexOidVec()
	{
		return &adjlist_indexes;
	}

	//! Get Catalog OIDs of Property Index
	idx_t_vector *GetPropertyIndexOidVec()
	{
		return &property_indexes;
	}

	//! Get Number of columns in the universal schema
	uint64_t GetNumberOfColumns() const;

	//! Returns a list of types of the table
	vector<LogicalType> GetTypes();

	//! Returns a key id -> location map
	PropertyToIdxUnorderedMap *GetPropertyToIdxMap()
	{
		return &global_property_key_to_location;
	}

	//! Get universal schema names
	string_vector *GetUniversalPropertyKeyNames()
	{
		return &global_property_key_names;
	}

	//! Get universal schema key ids
	idx_t_vector *GetUniversalPropertyKeyIds()
	{
		return &global_property_key_ids;
	}

	//! Get universal schema key ids
	LogicalTypeId_vector *GetUniversalPropertyTypeIds()
	{
		return &global_property_typesid;
	}

	//! Get offset infos member variable
	idx_t_vector *GetOffsetInfos()
	{
		return &offset_infos;
	}

	//! Get boundary values member variable
	idx_t_vector *GetBoundaryValues()
	{
		return &boundary_values;
	}

	//! get number of groups
	idx_t_vector *GetNumberOfGroups()
	{
		return &num_groups_for_each_column;
	}

	//! get number of groups
	idx_t_vector *GetMultipliers()
	{
		return &multipliers_for_each_column;
	}

	//! get group info
	idx_t_vector *GetGroupInfo()
	{
		return &group_info_for_each_table;
	}
	
	PartitionID GetPartitionID();
	ExtentID GetNewExtentID();
	ExtentID GetCurrentExtentID();
	ExtentID GetLocalExtentID();

	idx_t GetNewTemporalID()
	{
		return local_temporal_id_version++;
	}

	// Statistics
	void UpdateMinMaxArray(PropertyKeyID key_id, int64_t min, int64_t max);
	void UpdateWelfordStdDevArray(PropertyKeyID key_id, Vector& data, size_t size);
	StdDev GetStdDev(PropertyKeyID key_id);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;
};
} // namespace s62
