#pragma once


#include "catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
#include "common/case_insensitive_map.hpp"
#include "common/boost.hpp"
#include "common/boost_typedefs.hpp"

namespace s62 {

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
	PropertySchemaCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info, const void_allocator &void_alloc);

	//! Logical partition ID of parent partition
	PartitionID pid;

	//! OID of parent partition
	idx_t partition_oid;

	//! OID of the physical ID index (_id)
	idx_t physical_id_index;

	//! Property Key ID array
	PropertyKeyID_vector property_keys;

	//! Extent (vertex/edgelet) IDs
	idx_t_vector extent_ids;

	idx_t_vector key_column_idxs;

	//! Logical type id array of properties
	LogicalTypeId_vector property_typesid;

	//! Extra info vector
	uint16_t_vector extra_typeinfo_vec;

	//! Property key names
	string_vector property_key_names;

	LogicalTypeId_vector adjlist_typesid;
	string_vector adjlist_names;

	//! # of columns
	idx_t num_columns;

	//! # of tuples in the last extent
	idx_t last_extent_num_tuples;
	
	bool is_fake = false;

	//! Offset infos for frequency values
	idx_t_vector offset_infos;

	//! Frequency values of the histogram corresponding to each bucket
	idx_t_vector frequency_values;

	//! Number of distinct values of each property
	uint64_t_vector ndvs;
	
public:
	void SetTypes(vector<LogicalType> &types);
	void SetKeys(ClientContext &context, vector<string> &key_names);

    //! Set Schema Info
    void SetSchema(ClientContext &context, vector<string> &key_names,
                   vector<LogicalType> &types,
                   vector<PropertyKeyID> &prop_key_ids);

    //! Set Schema Info
    void SetSchema(ClientContext &context, vector<LogicalType> &types,
                   vector<PropertyKeyID> &prop_key_ids);

    //! Set Schema Info with key names, types and key IDs
    void SetSchema(ClientContext &context, vector<string> &key_names,
                   LogicalTypeId_vector &types,
                   PropertyKeyID_vector &prop_key_ids);

    void SetKeyColumnIdxs(vector<idx_t> &key_column_idxs_);
	void SetFake() {
		is_fake = true;
	}

	//! Check if the property schema is fake
	bool IsFake() {
		return is_fake;
	}

	//! Set physical ID index
	void SetPhysicalIDIndex(idx_t index_oid) {
		physical_id_index = index_oid;
	}

	string_vector *GetKeys();
	PropertyKeyID_vector *GetKeyIDs();
	vector<string> GetKeysWithCopy();
	string GetPropertyKeyName(idx_t i);
	void AppendType(LogicalType type);
	idx_t AppendKey(ClientContext &context, string key_name);
	void AppendAdjListType(LogicalType type);
	idx_t AppendAdjListKey(ClientContext &context, string key_name);

	//! Returns a list of types of the table
	LogicalTypeId_vector *GetTypes() {
		return &this->property_typesid;
	}

	//! Get Extra type info vector
	uint16_t_vector *GetExtraTypeInfos() {
		return &this->extra_typeinfo_vec;
	}

	//! Get i-th type id
	LogicalTypeId GetType(idx_t i) {
		return property_typesid[i];
	}

	//! Get i-th extra type info
	uint16_t GetExtraTypeInfo(idx_t i) {
		return extra_typeinfo_vec[i];
	}

	//! Get PropKeyID vector
	PropertyKeyID_vector *GetPropKeyIDs() {
		return &property_keys;
	}

	//! Get frequency offset infos member variable
	idx_t_vector *GetOffsetInfos() {
		return &offset_infos;
	}

	//! Get histogram frequency values member variable
	idx_t_vector *GetFrequencyValues() {
		return &frequency_values;
	}

	//! Get OID of physical id index
	idx_t GetPhysicalIDIndex(){
		return physical_id_index;
	}

	//! Get the ndvs
	uint64_t_vector *GetNDVs() {
		// ID column + other columns
		return &ndvs;
	}

	vector<LogicalType> GetTypesWithCopy();
	uint64_t GetTypeSize(idx_t i);
	vector<idx_t> GetColumnIdxs(vector<string> &property_keys);
	vector<idx_t> GetKeyColumnIdxs();

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	void AddExtent(ExtentCatalogEntry* extent_cat);
	void AddExtent(ExtentID eid, size_t num_tuples_in_extent = 0);
	PartitionID GetPartitionID();
	idx_t GetPartitionOID();

	uint64_t GetNumberOfColumns();
	uint64_t GetNumberOfRowsApproximately();
	uint64_t GetNumberOfExtents();

	//! Set the number of tuples in the last extent (used for temporal catalog only)
	void SetNumberOfLastExtentNumTuples(uint64_t num_tuples)
	{
		last_extent_num_tuples = num_tuples;
	}
};
} // namespace s62
