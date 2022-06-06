#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

ExtentCatalogEntry::ExtentCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateExtentInfo *info)
    : StandardEntry(CatalogType::EXTENT_ENTRY, schema, catalog, info->extent) {
	this->temporary = info->temporary;
	this->extent_type = info->extent_type;
	this->eid = info->eid;
}

unique_ptr<CatalogEntry> ExtentCatalogEntry::Copy(ClientContext &context) {
	auto create_info = make_unique<CreateExtentInfo>(schema->name, name, extent_type, eid);
	return make_unique<ExtentCatalogEntry>(catalog, schema, create_info.get());
}

void ExtentCatalogEntry::SetExtentType(ExtentType extent_type_) {
	extent_type = extent_type_;
}

} // namespace duckdb
