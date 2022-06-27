#include "extent/extent_manager.hpp"
#include "common/types/data_chunk.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "cache/chunk_cache_manager.h"
#include "common/directory_helper.hpp"

namespace duckdb {

ExtentManager::ExtentManager() {}

ExtentID ExtentManager::CreateExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry) {
    // Get New ExtentID & Create ExtentCatalogEntry
    PartitionID pid = prop_schema_cat_entry.GetPartitionID();
    ExtentID new_eid = prop_schema_cat_entry.GetNewExtentID();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = "ext_" + std::to_string(new_eid);
    CreateExtentInfo extent_info("main", extent_name.c_str(), ExtentType::EXTENT, new_eid, pid);
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.CreateExtent(context, &extent_info);
    
    // MkDir for the extent
    std::string extent_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid);
    MkDir(extent_dir_path, true);

    // Append Chunk
    _AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    return new_eid;
}

void ExtentManager::CreateExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentID new_eid) {
    // Create ExtentCatalogEntry
    PartitionID pid = prop_schema_cat_entry.GetPartitionID();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = "ext_" + std::to_string(new_eid);
    CreateExtentInfo extent_info("main", extent_name.c_str(), ExtentType::EXTENT, new_eid, pid);
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.CreateExtent(context, &extent_info);

    // MkDir for the extent
    std::string extent_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid);
    MkDir(extent_dir_path, true);

    // Append Chunk
    _AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
}

void ExtentManager::AppendChunkToExistingExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentID eid) {
    Catalog& cat_instance = context.db->GetCatalog();
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(eid));
    PartitionID pid = prop_schema_cat_entry.GetPartitionID();
    for (auto &l_type : input.GetTypes()) prop_schema_cat_entry.AppendType(l_type);
    _AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, eid);
}

void ExtentManager::_AppendChunkToExtent(ClientContext &context, DataChunk &input, Catalog& cat_instance, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID new_eid) {
    idx_t input_chunk_idx = 0;
    ChunkDefinitionID cdf_id_base = new_eid;
    cdf_id_base = cdf_id_base << 32;
    for (auto &l_type : input.GetTypes()) {
        // For each Vector in DataChunk create new chunk definition
        LocalChunkDefinitionID chunk_definition_idx = extent_cat_entry.GetNextChunkDefinitionID();
        ChunkDefinitionID cdf_id = cdf_id_base + chunk_definition_idx;
        string chunkdefinition_name = "cdf_" + std::to_string(cdf_id);
        CreateChunkDefinitionInfo chunkdefinition_info("main", chunkdefinition_name, l_type);
        ChunkDefinitionCatalogEntry* chunkdefinition_cat = (ChunkDefinitionCatalogEntry*) cat_instance.CreateChunkDefinition(context, &chunkdefinition_info);
        extent_cat_entry.AddChunkDefinitionID(cdf_id);

        // Get Buffer from Cache Manager
        // Cache Object ID: 64bit = ChunkDefinitionID
        uint8_t* buf_ptr;
        size_t buf_size;
        size_t alloc_buf_size;
        if (l_type == LogicalType::ADJLIST) {
            idx_t *adj_list_buffer = (idx_t*) input.data[input_chunk_idx].GetData();
            alloc_buf_size = sizeof(idx_t) * adj_list_buffer[STANDARD_VECTOR_SIZE - 1];
            fprintf(stdout, "adj_list_buffer[0] = %ld, adj_list_buffer[1] = %ld\n", adj_list_buffer[0], adj_list_buffer[1]);
        } else {
            alloc_buf_size = input.size() * GetTypeIdSize(l_type.InternalType());
        }
        string file_path_prefix = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_"
            + std::to_string(new_eid) + std::string("/chunk_");
        ChunkCacheManager::ccm->CreateSegment(cdf_id, file_path_prefix, alloc_buf_size, false);
        ChunkCacheManager::ccm->PinSegment(cdf_id, file_path_prefix, &buf_ptr, &buf_size);

        // Copy (or Compress and Copy) DataChunk
        memcpy(buf_ptr, input.data[input_chunk_idx].GetData(), alloc_buf_size);

        // Set Dirty & Unpin Segment & Flush
        ChunkCacheManager::ccm->SetDirty(cdf_id);
        ChunkCacheManager::ccm->UnPinSegment(cdf_id);
        input_chunk_idx++;
    }
}

} // namespace duckdb