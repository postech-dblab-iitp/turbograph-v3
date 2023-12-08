#include "main/client_context.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"

#include <memory>
#include <iostream>
#include <algorithm>

namespace duckdb {

PropertySchemaCatalogEntry::PropertySchemaCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::PROPERTY_SCHEMA_ENTRY, schema, catalog, info->propertyschema, void_alloc)
	, property_keys(void_alloc), extent_ids(void_alloc), key_column_idxs(void_alloc), property_typesid(void_alloc),
	property_key_names(void_alloc), adjlist_typesid(void_alloc), adjlist_names(void_alloc), num_columns(0),
	extra_typeinfo_vec(void_alloc) {
	this->temporary = info->temporary;
	this->pid = info->pid;
	this->partition_oid = info->partition_oid;
}

unique_ptr<CatalogEntry> PropertySchemaCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreatePropertySchemaInfo>(schema->name, name);
	//return make_unique<PropertySchemaCatalogEntry>(catalog, schema, create_info.get());
}

void PropertySchemaCatalogEntry::AddExtent(ExtentCatalogEntry* extent_cat) {
	D_ASSERT(false);
	extent_ids.push_back(extent_cat->oid);
}

void PropertySchemaCatalogEntry::AddExtent(ExtentID eid, size_t num_tuples_in_extent) {
	extent_ids.push_back(eid);
	last_extent_num_tuples = num_tuples_in_extent;
}

LogicalTypeId_vector *PropertySchemaCatalogEntry::GetTypes() {
	return &this->property_typesid;
}

uint16_t_vector *PropertySchemaCatalogEntry::GetExtraTypeInfos() {
	return &this->extra_typeinfo_vec;
}

LogicalTypeId PropertySchemaCatalogEntry::GetType(idx_t i) {
	return property_typesid[i];
}

uint16_t PropertySchemaCatalogEntry::GetExtraTypeInfo(idx_t i) {
	return extra_typeinfo_vec[i];
}

vector<LogicalType> PropertySchemaCatalogEntry::GetTypesWithCopy() {
	vector<LogicalType> types;
	for (auto &it : this->property_typesid) {
		LogicalType type(it);
		types.push_back(type);
	}
	return types;
}

vector<idx_t> PropertySchemaCatalogEntry::GetColumnIdxs(vector<string> &property_keys) {
	vector<idx_t> column_idxs;
	for (auto &it : property_keys) {
		auto idx = std::find(this->property_key_names.begin(), this->property_key_names.end(), it);
		if (idx == this->property_key_names.end()) throw InvalidInputException("");
		column_idxs.push_back(idx - this->property_key_names.begin());
	}
	return column_idxs;
}

void PropertySchemaCatalogEntry::SetTypes(vector<LogicalType> &types) {
	D_ASSERT(property_typesid.empty());
	for (auto &it : types) {
		if (it != LogicalType::FORWARD_ADJLIST && it != LogicalType::BACKWARD_ADJLIST) num_columns++;
		property_typesid.push_back(it.id());
		if (it.id() == LogicalTypeId::DECIMAL) {
			uint16_t width_scale = DecimalType::GetWidth(it);
			width_scale = width_scale << 8 | DecimalType::GetScale(it);
			extra_typeinfo_vec.push_back(width_scale);
		} else {
			extra_typeinfo_vec.push_back(0);
		}
	}
}

void PropertySchemaCatalogEntry::SetKeys(ClientContext &context, vector<string> &key_names) {
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	D_ASSERT(property_key_names.empty());
	for (auto &it : key_names) {
		char_string key_(temp_charallocator);
		key_ = it.c_str();
		property_key_names.push_back(move(key_));
	}
}

void PropertySchemaCatalogEntry::SetKeyIDs(ClientContext &context, vector<PropertyKeyID> &key_ids) {
	D_ASSERT(property_keys.empty());
	for (auto i = 0; i < key_ids.size(); i++) {
		property_keys.push_back(key_ids[i]);
	}
}

void PropertySchemaCatalogEntry::SetKeyColumnIdxs(vector<idx_t> &key_column_idxs_) {
	for (auto &it : key_column_idxs_) {
		key_column_idxs.push_back(it);
	}
}

string_vector *PropertySchemaCatalogEntry::GetKeys() {
	return &property_key_names;
}

PropertyKeyID_vector *PropertySchemaCatalogEntry::GetKeyIDs() {
	return &property_keys;
}

vector<string> PropertySchemaCatalogEntry::GetKeysWithCopy() {
	vector<string> output;
	for (auto &it : property_key_names) {
		output.push_back(std::string(it));
	}
	return output;
}

vector<idx_t> PropertySchemaCatalogEntry::GetKeyColumnIdxs() {
	vector<idx_t> output;
	for (auto &it : key_column_idxs) {
		output.push_back(it);
	}
	return output;
}

void PropertySchemaCatalogEntry::AppendType(LogicalType type) {
	D_ASSERT((type != LogicalType::FORWARD_ADJLIST) && (type != LogicalType::BACKWARD_ADJLIST));
	num_columns++;
	property_typesid.push_back(move(type.id()));
}

idx_t PropertySchemaCatalogEntry::AppendKey(ClientContext &context, string key) {
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	char_string key_(temp_charallocator);
	key_ = key.c_str();
	property_key_names.push_back(move(key_));
	return property_key_names.size() - 1;
}

void PropertySchemaCatalogEntry::AppendAdjListType(LogicalType type) {
	D_ASSERT((type == LogicalType::FORWARD_ADJLIST) || (type == LogicalType::BACKWARD_ADJLIST));
	adjlist_typesid.push_back(move(type.id()));
}

idx_t PropertySchemaCatalogEntry::AppendAdjListKey(ClientContext &context, string key) {
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	char_string key_(temp_charallocator);
	key_ = key.c_str();
	adjlist_names.push_back(move(key_));
	return adjlist_names.size() - 1;
}

PartitionID PropertySchemaCatalogEntry::GetPartitionID() {
	return pid;
}

uint64_t PropertySchemaCatalogEntry::GetNumberOfColumns() {
	return num_columns;
}

string PropertySchemaCatalogEntry::GetPropertyKeyName(idx_t i) {
	return string(property_key_names[i]);
}

uint64_t PropertySchemaCatalogEntry::GetTypeSize(idx_t i) {
	if (property_typesid[i] == LogicalTypeId::DECIMAL) {
		uint8_t width = extra_typeinfo_vec[i] & 0xFF00;
		uint8_t scale = extra_typeinfo_vec[i] & 0x00FF;
		return GetTypeIdSize(LogicalType::DECIMAL(width, scale).InternalType());
	}
	return GetTypeIdSize(LogicalType(property_typesid[i]).InternalType());
}

idx_t PropertySchemaCatalogEntry::GetPartitionOID() {
	return partition_oid;
}

uint64_t PropertySchemaCatalogEntry::GetNumberOfRowsApproximately() {
	// # of extents * # rows per extent
	// D_ASSERT(extent_ids.size() >= 1); // TODO 
	// uint64_t num_tuples_except_last_extent = (extent_ids.size() - 1) * STORAGE_STANDARD_VECTOR_SIZE;

	// return num_tuples_except_last_extent + last_extent_num_tuples;
	return 1; // TODO extend for fake
}

uint64_t PropertySchemaCatalogEntry::GetNumberOfExtents() {
	return extent_ids.size();
}

void PropertySchemaCatalogEntry::InitializeAccumulators() {
	
}

void PropertySchemaCatalogEntry::AccumulateExtent(DataChunk &chunk) {

}

void PropertySchemaCatalogEntry::FinalizeAccumulators() {

}

} // namespace duckdb
