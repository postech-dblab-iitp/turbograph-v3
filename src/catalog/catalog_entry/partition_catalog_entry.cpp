#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_partition_info.hpp"

#include <algorithm>
#include <memory>

namespace duckdb {

PartitionCatalogEntry::PartitionCatalogEntry(Catalog *catalog,
                                             SchemaCatalogEntry *schema,
                                             CreatePartitionInfo *info,
                                             const void_allocator &void_alloc)
    : StandardEntry(CatalogType::PARTITION_ENTRY, schema, catalog,
                    info->partition, void_alloc),
      property_schema_index(void_alloc),
      property_schema_array(void_alloc),
      adjlist_indexes(void_alloc),
      property_indexes(void_alloc),
      global_property_typesid(void_alloc),
      global_property_key_names(void_alloc),
      id_key_column_idxs(void_alloc),
	  global_property_key_ids(void_alloc),
      extra_typeinfo_vec(void_alloc),
      offset_infos(void_alloc),
      boundary_values(void_alloc),
      global_property_key_to_location(void_alloc),
      num_groups_for_each_column(void_alloc),
      group_info_for_each_table(void_alloc),
      multipliers_for_each_column(void_alloc),
      min_max_array(void_alloc),
      welford_array(void_alloc)
{
    this->temporary = info->temporary;
    this->pid = info->pid;
    this->num_columns = 0;
    this->local_temporal_id_version = 0;
    this->local_extent_id_version = 0;
    this->physical_id_index = INVALID_OID;
}

void PartitionCatalogEntry::AddPropertySchema(
    ClientContext &context, idx_t ps_oid,
    vector<PropertyKeyID> &property_schemas)
{
    property_schema_array.push_back(ps_oid);
    for (int i = 0; i < property_schemas.size(); i++) {
        auto target_partitions =
            property_schema_index.find(property_schemas[i]);
        if (target_partitions != property_schema_index.end()) {
            // found
            property_schema_index.at(property_schemas[i]).push_back({ps_oid, i});
        }
        else {
            // not found
            void_allocator void_alloc(
                context.db->GetCatalog()
                    .catalog_segment->get_segment_manager());
            idx_t_pair_vector tmp_vec(void_alloc);
            tmp_vec.push_back({ps_oid, i});
            property_schema_index.insert({property_schemas[i], tmp_vec});
        }
    }
}

void PartitionCatalogEntry::SetUnivPropertySchema(idx_t psid)
{
    univ_ps_oid = psid;
}

void PartitionCatalogEntry::SetIdKeyColumnIdxs(vector<idx_t> &key_column_idxs)
{
    for (auto &it : key_column_idxs) {
        id_key_column_idxs.push_back(it);
    }
}

unique_ptr<CatalogEntry> PartitionCatalogEntry::Copy(ClientContext &context)
{
    D_ASSERT(false);
}

void PartitionCatalogEntry::SetPartitionID(PartitionID pid)
{
    this->pid = pid;
}

PartitionID PartitionCatalogEntry::GetPartitionID()
{
    return pid;
}

ExtentID PartitionCatalogEntry::GetLocalExtentID()
{
    return local_extent_id_version;
}

ExtentID PartitionCatalogEntry::GetCurrentExtentID()
{
    ExtentID eid = pid;
    eid = eid << 16;
    return eid + local_extent_id_version;
}

ExtentID PartitionCatalogEntry::GetNewExtentID()
{
    ExtentID new_eid = pid;
    new_eid = new_eid << 16;
    return new_eid + local_extent_id_version++;
}

void PartitionCatalogEntry::GetPropertySchemaIDs(vector<idx_t> &psids)
{
    for (auto &psid : property_schema_array) {
        psids.push_back(psid);
    }
}

void PartitionCatalogEntry::SetPhysicalIDIndex(idx_t index_oid)
{
    physical_id_index = index_oid;
}

void PartitionCatalogEntry::SetSrcDstPartOid(idx_t src_part_oid,
                                             idx_t dst_part_oid)
{
    this->src_part_oid = src_part_oid;
    this->dst_part_oid = dst_part_oid;
}

void PartitionCatalogEntry::AddAdjIndex(idx_t index_oid)
{
    adjlist_indexes.push_back(index_oid);
}

void PartitionCatalogEntry::AddPropertyIndex(idx_t index_oid)
{
    property_indexes.push_back(index_oid);
}

idx_t PartitionCatalogEntry::GetPhysicalIDIndexOid()
{
    D_ASSERT(physical_id_index != INVALID_OID);
    return physical_id_index;
}

void PartitionCatalogEntry::SetSchema(ClientContext &context,
                                      vector<string> &key_names,
                                      vector<LogicalType> &types,
                                      vector<PropertyKeyID> &univ_prop_key_ids)
{
    char_allocator temp_charallocator(
        context.GetCatalogSHM()->get_segment_manager());
    D_ASSERT(global_property_typesid.empty());
    D_ASSERT(global_property_key_names.empty());

    // Set type info
    for (auto &it : types) {
        if (it != LogicalType::FORWARD_ADJLIST &&
            it != LogicalType::BACKWARD_ADJLIST)
            num_columns++;
        global_property_typesid.push_back(it.id());
        if (it.id() == LogicalTypeId::DECIMAL) {
            uint16_t width_scale = DecimalType::GetWidth(it);
            width_scale = width_scale << 8 | DecimalType::GetScale(it);
            extra_typeinfo_vec.push_back(width_scale);
        }
        else {
            extra_typeinfo_vec.push_back(0);
        }
    }

    // Set key names
    for (auto &it : key_names) {
        char_string key_(temp_charallocator);
        key_ = it.c_str();
        global_property_key_names.push_back(move(key_));
    }

    // Set key id -> location info
    for (auto i = 0; i < univ_prop_key_ids.size(); i++) {
        global_property_key_to_location.insert({univ_prop_key_ids[i], i});
		global_property_key_ids.push_back(univ_prop_key_ids[i]);
    }

    // Set min_max_array
    min_max_array.resize(types.size());

    // Set welford_array
    welford_array.resize(types.size());
}

void PartitionCatalogEntry::SetTypes(vector<LogicalType> &types)
{
    D_ASSERT(global_property_typesid.empty());
    for (auto &it : types) {
        if (it != LogicalType::FORWARD_ADJLIST &&
            it != LogicalType::BACKWARD_ADJLIST)
            num_columns++;
        global_property_typesid.push_back(it.id());
        if (it.id() == LogicalTypeId::DECIMAL) {
            uint16_t width_scale = DecimalType::GetWidth(it);
            width_scale = width_scale << 8 | DecimalType::GetScale(it);
            extra_typeinfo_vec.push_back(width_scale);
        }
        else {
            extra_typeinfo_vec.push_back(0);
        }
    }
}

// TODO we need to create universal schema in memory only once & reuse
// avoid serialize in-memory DS
vector<LogicalType> PartitionCatalogEntry::GetTypes()
{
    vector<LogicalType> universal_schema;
    for (auto i = 0; i < global_property_typesid.size(); i++) {
        if (extra_typeinfo_vec[i] == 0) {
            universal_schema.push_back(LogicalType(global_property_typesid[i]));
        }
        else {
            // decimal type case
            uint8_t width = (uint8_t)(extra_typeinfo_vec[i] >> 8);
            uint8_t scale = (uint8_t)(extra_typeinfo_vec[i] & 0xFF);
            universal_schema.push_back(LogicalType::DECIMAL(width, scale));
        }
    }

    return universal_schema;
}

uint64_t PartitionCatalogEntry::GetNumberOfColumns() const
{
    return num_columns;
}

/**
 * Note that idx_t is uint64_t, which assumes positive number
 * If we want to support negative number, we need to change this
 */

void PartitionCatalogEntry::UpdateMinMaxArray(PropertyKeyID key_id, int64_t min,
                                              int64_t max)
{
    auto location = global_property_key_to_location.find(key_id);
    if (location != global_property_key_to_location.end()) {
        auto idx = location->second;
        auto minmax = min_max_array[idx];
        if (minmax.min > min)
            minmax.min = min;
        if (minmax.max < max)
            minmax.max = max;
        min_max_array[idx] = minmax;
    }
}

void PartitionCatalogEntry::UpdateWelfordStdDevArray(PropertyKeyID key_id,
                                                     Vector &data, size_t size)
{
    D_ASSERT(data.GetType().IsNumeric());

    auto location = global_property_key_to_location.find(key_id);
    if (location != global_property_key_to_location.end()) {
        for (int i = 0; i < size; i++) {
            auto original_value = data.GetValue(i);
            auto value = original_value.GetValue<idx_t>();
            auto &welford_v = welford_array[location->second];
            welford_v.n++;
            auto delta = value - welford_v.mean;
            welford_v.mean += delta / welford_v.n;
            auto delta2 = value - welford_v.mean;
            welford_v.M2 += delta * delta2;
        }
    }
}

StdDev PartitionCatalogEntry::GetStdDev(PropertyKeyID key_id)
{
    StdDev std_dev = 0.0;
    auto location = global_property_key_to_location.find(key_id);
    if (location != global_property_key_to_location.end()) {
        auto welford_v = welford_array[location->second];
        if (welford_v.n > 1) {
            std_dev = sqrt(welford_v.M2 / (welford_v.n));
        }
    }
    return std_dev;
}

}  // namespace duckdb
