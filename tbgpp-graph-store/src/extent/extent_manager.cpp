#include "extent/extent_manager.hpp"
#include "common/types/data_chunk.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "cache/chunk_cache_manager.h"

namespace duckdb {

ExtentManager::ExtentManager() {}

vector<ExtentID> ExtentManager::CreateVertexExtents(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry & prop_schema_cat_entry) {
    // Create New Extent in Catalog
    ExtentID new_eid = prop_schema_cat_entry.GetNewExtentID();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = "extent" + std::to_string(new_eid);
    CreateExtentInfo extent_info("main", extent_name.c_str(), ExtentType::EXTENT, new_eid);
    ExtentCatalogEntry* extent_cat = (ExtentCatalogEntry*) cat_instance.CreateExtent(context, &extent_info);

    LocalChunkDefinitionID chunk_definition_idx = 0;
    ChunkDefinitionID cdf_id_base = new_eid;
    cdf_id_base << 32;
    for (auto &l_type : input.GetTypes()) {
        // For each Vector in DataChunk create new chunk definition
        ChunkDefinitionID cdf_id = cdf_id_base + chunk_definition_idx;
        string chunkdefinition_name = "chunkdefinition" + std::to_string(cdf_id);
        CreateChunkDefinitionInfo chunkdefinition_info("main", chunkdefinition_name, l_type);
        ChunkDefinitionCatalogEntry* chunkdefinition_cat = (ChunkDefinitionCatalogEntry*) cat_instance.CreateChunkDefinition(context, &chunkdefinition_info);

        // Get Buffer from Cache Manager
        // Cache Object ID: 64bit = ChunkDefinitionID
        uint8_t* buf_ptr;
        size_t buf_size;
        size_t alloc_buf_size = input.size() * GetTypeIdSize(l_type.InternalType());
        string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
        ChunkCacheManager::ccm->CreateSegment(cdf_id, file_path, alloc_buf_size, false);
        ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &buf_ptr, &buf_size);

        // Copy (or Compress and Copy) DataChunk
        memcpy(buf_ptr, input.data[chunk_definition_idx].GetData(), alloc_buf_size);

        // Set Dirty & Unpin Segment & Flush //
        ChunkCacheManager::ccm->SetDirty(cdf_id);
        ChunkCacheManager::ccm->UnPinSegment(cdf_id);

        chunk_definition_idx++;
    }
}

} // namespace duckdb