#include "extent/extent_manager.hpp"
#include "common/types/data_chunk.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_extent_info.hpp"

namespace duckdb {

vector<ExtentID> ExtentManager::CreateVertexExtents(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry & prop_schema_cat_entry) {
    // Create New Extent in Catalog
    ExtentID = prop_schema_cat_entry
    Catalog& cat_instance = context.db->GetCatalog();
    cat_instance.CreateExtent();

    // For each Vector in DataChunk create new chunk definition

    // Get Buffer from Cache Manager
    // Cache Object ID: 64bit = ExtentID {PartitionID: 16bit, LocalExtentID: 16bit}, ChunkID: 32bit

    // Copy (or Compress and Copy) DataChunk .. or Create DataChunk in SHM? No!

}

} // namespace duckdb