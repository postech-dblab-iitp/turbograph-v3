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
	  extra_typeinfo_vec(void_alloc) {
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
