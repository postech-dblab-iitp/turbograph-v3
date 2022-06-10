#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

ChunkDefinitionCatalogEntry::ChunkDefinitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info)
    : StandardEntry(CatalogType::EXTENT_ENTRY, schema, catalog, info->chunkdefinition) {
	this->temporary = info->temporary;
	this->data_type = info->type;
}

unique_ptr<CatalogEntry> ChunkDefinitionCatalogEntry::Copy(ClientContext &context) {
	auto create_info = make_unique<CreateChunkDefinitionInfo>(schema->name, name, data_type);
	return make_unique<ChunkDefinitionCatalogEntry>(catalog, schema, create_info.get());
}

} // namespace duckdb
