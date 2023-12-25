#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "common/enums/graph_component_type.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

PartitionCatalogEntry::PartitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePartitionInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::PARTITION_ENTRY, schema, catalog, info->partition, void_alloc),
	  property_schema_index(void_alloc), property_schema_array(void_alloc), adjlist_indexes(void_alloc),
	  property_indexes(void_alloc), global_property_typesid(void_alloc), global_property_key_names(void_alloc),
	  extra_typeinfo_vec(void_alloc), offset_infos(void_alloc), boundary_values(void_alloc),
	  global_property_key_to_location(void_alloc) {
	this->temporary = info->temporary;
	this->pid = info->pid;
	this->num_columns = 0;
	this->physical_id_index = INVALID_OID;
}

// TODO psid is now oid.. do we need PropertySchemaID?
void PartitionCatalogEntry::AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> &property_schemas) {
	property_schema_array.push_back(psid);
	for (int i = 0; i < property_schemas.size(); i++) {
		auto target_partitions = property_schema_index.find(property_schemas[i]);
		if (target_partitions != property_schema_index.end()) {
			// found
			property_schema_index.at(property_schemas[i]).push_back(psid);
		} else {
			// not found
			void_allocator void_alloc (context.db->GetCatalog().catalog_segment->get_segment_manager());
			PropertyKeyID_vector tmp_vec(void_alloc);
			tmp_vec.push_back(psid);
			property_schema_index.insert({property_schemas[i], tmp_vec});
		}
	}
}

unique_ptr<CatalogEntry> PartitionCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreatePartitionInfo>(schema->name, name);
	//return make_unique<PartitionCatalogEntry>(catalog, schema, create_info.get());
}

void PartitionCatalogEntry::SetPartitionID(PartitionID pid) {
	this->pid = pid;
}

PartitionID PartitionCatalogEntry::GetPartitionID() {
	return pid;
}

ExtentID PartitionCatalogEntry::GetNewExtentID() {
	ExtentID new_eid = pid;
	new_eid = new_eid << 16;
	return new_eid + local_extent_id_version++;
}

void PartitionCatalogEntry::GetPropertySchemaIDs(vector<idx_t> &psids) {
	for (auto &psid : property_schema_array) {
		psids.push_back(psid);
	}
}

void PartitionCatalogEntry::SetPhysicalIDIndex(idx_t index_oid) {
	physical_id_index = index_oid;
}

void PartitionCatalogEntry::AddAdjIndex(idx_t index_oid) {
	adjlist_indexes.push_back(index_oid);
}

void PartitionCatalogEntry::AddPropertyIndex(idx_t index_oid) {
	property_indexes.push_back(index_oid);
}

PropertySchemaID_vector *PartitionCatalogEntry::GetPropertySchemaIDs() {
	return &property_schema_array;
}

idx_t PartitionCatalogEntry::GetPhysicalIDIndexOid() {
	D_ASSERT(physical_id_index != INVALID_OID);
	return physical_id_index;
}

idx_t_vector *PartitionCatalogEntry::GetAdjIndexOidVec() {
	return &adjlist_indexes;
}

idx_t_vector *PartitionCatalogEntry::GetPropertyIndexOidVec() {
	return &property_indexes;
}

PropertyToIdxUnorderedMap *PartitionCatalogEntry::GetPropertyToIdxMap() {
	return &global_property_key_to_location;
}

idx_t_vector *PartitionCatalogEntry::GetOffsetInfos() {
	return &offset_infos;
}

idx_t_vector *PartitionCatalogEntry::GetBoundaryValues() {
	return &boundary_values;
}

void PartitionCatalogEntry::SetSchema(ClientContext &context, vector<string> &key_names, vector<LogicalType> &types, vector<PropertyKeyID> &univ_prop_key_ids) {
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	D_ASSERT(global_property_typesid.empty());
	D_ASSERT(global_property_key_names.empty());

	// Set type info
	for (auto &it : types) {
		if (it != LogicalType::FORWARD_ADJLIST && it != LogicalType::BACKWARD_ADJLIST) num_columns++;
		global_property_typesid.push_back(it.id());
		if (it.id() == LogicalTypeId::DECIMAL) {
			uint16_t width_scale = DecimalType::GetWidth(it);
			width_scale = width_scale << 8 | DecimalType::GetScale(it);
			extra_typeinfo_vec.push_back(width_scale);
		} else {
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
	}
}

void PartitionCatalogEntry::SetTypes(vector<LogicalType> &types) {
	D_ASSERT(global_property_typesid.empty());
	for (auto &it : types) {
		if (it != LogicalType::FORWARD_ADJLIST && it != LogicalType::BACKWARD_ADJLIST) num_columns++;
		global_property_typesid.push_back(it.id());
		if (it.id() == LogicalTypeId::DECIMAL) {
			uint16_t width_scale = DecimalType::GetWidth(it);
			width_scale = width_scale << 8 | DecimalType::GetScale(it);
			extra_typeinfo_vec.push_back(width_scale);
		} else {
			extra_typeinfo_vec.push_back(0);
		}
	}
}

// TODO we need to create universal schema in memory only once & reuse
// avoid serialize in-memory DS
vector<LogicalType> PartitionCatalogEntry::GetTypes() {
	vector<LogicalType> universal_schema;
	for (auto i = 0; i < global_property_typesid.size(); i++) {
		if (extra_typeinfo_vec[i] == 0) {
			universal_schema.push_back(LogicalType(global_property_typesid[i]));
		} else {
			// decimal type case
			uint8_t width = (uint8_t)((extra_typeinfo_vec[i] | 0xFF00) >> 8);
			uint8_t scale = (uint8_t)(extra_typeinfo_vec[i] | 0xFF);
			universal_schema.push_back(LogicalType::DECIMAL(width, scale));
		}
	}

	return universal_schema;
}

void PartitionCatalogEntry::SetKeys(ClientContext &context, vector<string> &key_names) {
	// TODO add logic to generate global schema (ex. if size = 0, just insert, not, insert only new things
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	D_ASSERT(global_property_key_names.empty());
	for (auto &it : key_names) {
		char_string key_(temp_charallocator);
		key_ = it.c_str();
		global_property_key_names.push_back(move(key_));
	}
}

uint64_t PartitionCatalogEntry::GetNumberOfColumns() const {
	return num_columns;
}

} // namespace duckdb
