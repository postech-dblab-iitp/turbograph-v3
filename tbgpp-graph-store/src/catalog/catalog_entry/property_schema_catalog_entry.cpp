#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

PropertySchemaCatalogEntry::PropertySchemaCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info)
    : StandardEntry(CatalogType::PROPERTY_SCHEMA_ENTRY, schema, catalog, info->propertyschema) {
	this->temporary = info->temporary;
}

unique_ptr<CatalogEntry> PropertySchemaCatalogEntry::Copy(ClientContext &context) {
	auto create_info = make_unique<CreatePropertySchemaInfo>(schema->name, name);
	return make_unique<PropertySchemaCatalogEntry>(catalog, schema, create_info.get());
}

void PropertySchemaCatalogEntry::AddExtent(ExtentCatalogEntry* extent_cat) {
	extent_ids.push_back(extent_cat->oid);
}

} // namespace duckdb
