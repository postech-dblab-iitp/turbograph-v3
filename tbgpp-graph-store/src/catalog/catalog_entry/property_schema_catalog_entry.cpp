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
	, property_keys(void_alloc), extent_ids(void_alloc), local_extent_id_version(0), property_key_names(void_alloc), property_typesid(void_alloc) {
	this->temporary = info->temporary;
	this->pid = info->pid;
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

void PropertySchemaCatalogEntry::AddExtent(ExtentID eid) {
	// Can we perform this logic in GetNewExtentID??
	extent_ids.push_back(eid);
}

ExtentID PropertySchemaCatalogEntry::GetNewExtentID() {
	ExtentID new_eid = pid;
	new_eid = new_eid << 16;
	return new_eid + local_extent_id_version++;
}

vector<LogicalType> PropertySchemaCatalogEntry::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : this->property_typesid) {
		types.emplace_back(it);
	}
	return types;
}

vector<idx_t> PropertySchemaCatalogEntry::GetColumnIdxs(vector<string> &property_keys) {
	for (auto &it : property_key_names) {
		std::cout << "pk: " << it << std::endl;
	}
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
		property_types.push_back(it.id());
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

vector<string> PropertySchemaCatalogEntry::GetKeys() {
	vector<string> output;
	for (auto &it : property_key_names) {
		output.push_back(std::string(it));
	}
	return output;
}

void PropertySchemaCatalogEntry::AppendType(LogicalType type) {
	property_types.push_back(move(type.id()));
}

void PropertySchemaCatalogEntry::AppendKey(ClientContext &context, string key) {
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	char_string key_(temp_charallocator);
	key_ = key.c_str();
	property_key_names.push_back(move(key_));
}

PartitionID PropertySchemaCatalogEntry::GetPartitionID() {
	return pid;
}

} // namespace duckdb
