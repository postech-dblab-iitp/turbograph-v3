#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

PartitionCatalogEntry::PartitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePartitionInfo *info)
    : StandardEntry(CatalogType::PARTITION_ENTRY, schema, catalog, info->partition) {
	this->temporary = info->temporary;
}

void PartitionCatalogEntry::AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> property_schemas) {
	for (int i = 0; i < property_schemas.size(); i++) {
		property_schema_index[property_schemas[i]].push_back(psid);
	}
}

unique_ptr<CatalogEntry> PartitionCatalogEntry::Copy(ClientContext &context) {
	auto create_info = make_unique<CreatePartitionInfo>(schema->name, name);
	return make_unique<PartitionCatalogEntry>(catalog, schema, create_info.get());
}

} // namespace duckdb
