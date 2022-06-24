#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

ExtentCatalogEntry::ExtentCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateExtentInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::EXTENT_ENTRY, schema, catalog, info->extent), chunks(void_alloc) {
	this->temporary = info->temporary;
	this->extent_type = info->extent_type;
	this->eid = info->eid;
	this->local_chunkdefinition_id_version = 0;
	this->pid = info->pid;
}

unique_ptr<CatalogEntry> ExtentCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreateExtentInfo>(schema->name, name, extent_type, eid);
	//return make_unique<ExtentCatalogEntry>(catalog, schema, create_info.get());
}

void ExtentCatalogEntry::SetExtentType(ExtentType extent_type_) {
	extent_type = extent_type_;
}

LocalChunkDefinitionID ExtentCatalogEntry::GetNextChunkDefinitionID() {
	return local_chunkdefinition_id_version++;
}

void ExtentCatalogEntry::AddChunkDefinitionID(ChunkDefinitionID cdf_id) {
	chunks.push_back(cdf_id);
}

} // namespace duckdb
