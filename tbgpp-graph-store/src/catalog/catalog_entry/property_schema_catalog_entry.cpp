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
}

unique_ptr<CatalogEntry> PropertySchemaCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreatePropertySchemaInfo>(schema->name, name);
	//return make_unique<PropertySchemaCatalogEntry>(catalog, schema, create_info.get());
}

void PropertySchemaCatalogEntry::AddExtent(ExtentCatalogEntry* extent_cat) {
	extent_ids.push_back(extent_cat->oid);
}

ExtentID PropertySchemaCatalogEntry::GetNewExtentID() {
	ExtentID new_eid = pid;
	new_eid = new_eid << 16;
	return new_eid + local_extent_id_version++;
}

} // namespace duckdb
