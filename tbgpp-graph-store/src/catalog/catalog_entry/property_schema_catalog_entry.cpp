#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

PropertySchemaCatalogEntry::PropertySchemaCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::PROPERTY_SCHEMA_ENTRY, schema, catalog, info->propertyschema)
	, property_keys(void_alloc), extent_ids(void_alloc), local_extent_id_version(0) {
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
	for (auto &it : this->property_types) {
		types.push_back(it);
	}
	return types;
}

vector<idx_t> PropertySchemaCatalogEntry::GetColumnIdxs(vector<string> &property_keys) {
	vector<idx_t> column_idxs;
	for (auto &it : this->property_key_names) {
		fprintf(stdout, "Property %s\n", it.c_str());
	}
	for (auto &it : property_keys) {
		fprintf(stdout, "Find %s\n", it.c_str());
		auto idx = std::find(this->property_key_names.begin(), this->property_key_names.end(), it);
		if (idx == this->property_key_names.end()) throw InvalidInputException("");
		fprintf(stdout, "push_back %ld\n", idx - this->property_key_names.begin());
		column_idxs.push_back(idx - this->property_key_names.begin());
	}
	return column_idxs;
}

void PropertySchemaCatalogEntry::SetTypes(vector<LogicalType> &types) {
	D_ASSERT(property_types.empty());
	for (auto &it : types) {
		property_types.push_back(it);
	}
}

void PropertySchemaCatalogEntry::SetKeys(vector<string> &key_names) {
	D_ASSERT(property_key_names.empty());
	for (auto &it : key_names) {
		property_key_names.push_back(it);
	}
}

void PropertySchemaCatalogEntry::AppendType(LogicalType type) {
	property_types.push_back(move(type));
}

void PropertySchemaCatalogEntry::AppendKey(string key) {
	property_key_names.push_back(move(key));
}

PartitionID PropertySchemaCatalogEntry::GetPartitionID() {
	return pid;
}

} // namespace duckdb
