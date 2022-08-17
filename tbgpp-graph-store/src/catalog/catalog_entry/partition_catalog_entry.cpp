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
    : StandardEntry(CatalogType::PARTITION_ENTRY, schema, catalog, info->partition, void_alloc), property_schema_index(void_alloc) {
	this->temporary = info->temporary;
}

void PartitionCatalogEntry::AddPropertySchema(ClientContext &context, PropertySchemaID psid, vector<PropertyKeyID> &property_schemas) {
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

} // namespace duckdb
