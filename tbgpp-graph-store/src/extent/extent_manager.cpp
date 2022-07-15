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
#include "extent/compression/compression_function.hpp"

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
    //_AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
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
    //_AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
}

void ExtentManager::AppendChunkToExistingExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentID eid, vector<string> append_keys) {
    Catalog& cat_instance = context.db->GetCatalog();
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(eid));
    PartitionID pid = prop_schema_cat_entry.GetPartitionID();
    for (auto &l_type : input.GetTypes()) prop_schema_cat_entry.AppendType(l_type);
    for (auto &key : append_keys) prop_schema_cat_entry.AppendKey(key);
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
        } else if (l_type == LogicalType::VARCHAR) {
            size_t string_len_total = 0;
            string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();
            for (size_t i = 0; i < input.size(); i++) {
                string_len_total += sizeof(uint32_t); // size of len field
                string_len_total += string_buffer[i].GetSize();
            }
            alloc_buf_size = string_len_total + sizeof(uint64_t);
        } else {
            D_ASSERT(TypeIsConstantSize(l_type.InternalType()));
            alloc_buf_size = input.size() * GetTypeIdSize(l_type.InternalType()) + sizeof(uint64_t);
        }
        //fprintf(stdout, "cdf %ld Alloc_buf_size = %ld\n", cdf_id, alloc_buf_size);
        string file_path_prefix = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_"
            + std::to_string(new_eid) + std::string("/chunk_");
        ChunkCacheManager::ccm->CreateSegment(cdf_id, file_path_prefix, alloc_buf_size, false);
        ChunkCacheManager::ccm->PinSegment(cdf_id, file_path_prefix, &buf_ptr, &buf_size);

        // Copy (or Compress and Copy) DataChunk
        if (l_type == LogicalType::VARCHAR) {
            size_t offset = 0;
            size_t input_size = input.size();
            memcpy(buf_ptr + offset, &input_size, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            uint32_t string_len;
            string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();
            for (size_t i = 0; i < input.size(); i++) {
                string_len = string_buffer[i].GetSize();
                memcpy(buf_ptr + offset, &string_len, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(buf_ptr + offset, string_buffer[i].GetDataUnsafe(), string_len);
                offset += string_len;
            }
        } else if (l_type == LogicalType::ADJLIST) {
            memcpy(buf_ptr, input.data[input_chunk_idx].GetData(), alloc_buf_size);
        } else {
            size_t input_size = input.size();
            memcpy(buf_ptr, &input_size, sizeof(uint64_t));
            memcpy(buf_ptr + sizeof(uint64_t), input.data[input_chunk_idx].GetData(), alloc_buf_size - sizeof(uint64_t));
        }

        // Set Dirty & Unpin Segment & Flush
        ChunkCacheManager::ccm->SetDirty(cdf_id);
        ChunkCacheManager::ccm->UnPinSegment(cdf_id);
        input_chunk_idx++;
    }
}

void ExtentManager::_AppendChunkToExtentWithCompression(ClientContext &context, DataChunk &input, Catalog& cat_instance, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID new_eid) {
    idx_t input_chunk_idx = 0;
    ChunkDefinitionID cdf_id_base = new_eid;
    cdf_id_base = cdf_id_base << 32;
    for (auto &l_type : input.GetTypes()) {
        // Get Physical Type
        PhysicalType p_type = l_type.InternalType();
        // For each Vector in DataChunk create new chunk definition
        LocalChunkDefinitionID chunk_definition_idx = extent_cat_entry.GetNextChunkDefinitionID();
        ChunkDefinitionID cdf_id = cdf_id_base + chunk_definition_idx;
        string chunkdefinition_name = "cdf_" + std::to_string(cdf_id);
        CreateChunkDefinitionInfo chunkdefinition_info("main", chunkdefinition_name, l_type);
        ChunkDefinitionCatalogEntry* chunkdefinition_cat = (ChunkDefinitionCatalogEntry*) cat_instance.CreateChunkDefinition(context, &chunkdefinition_info);
        extent_cat_entry.AddChunkDefinitionID(cdf_id);

        // Analyze compression to find best compression method
        CompressionFunctionType best_compression_function;
        best_compression_function = BITPACKING;

        // Get Buffer from Cache Manager
        // Cache Object ID: 64bit = ChunkDefinitionID
        uint8_t *buf_ptr;
        size_t buf_size;
        size_t alloc_buf_size;
        if (l_type == LogicalType::ADJLIST) {
            idx_t *adj_list_buffer = (idx_t*) input.data[input_chunk_idx].GetData();
            alloc_buf_size = sizeof(idx_t) * adj_list_buffer[STANDARD_VECTOR_SIZE - 1];
        } else if (l_type == LogicalType::VARCHAR) {
            size_t string_len_total = 0;
            string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();
            for (size_t i = 0; i < input.size(); i++) {
                string_len_total += sizeof(uint32_t); // size of len field
                string_len_total += string_buffer[i].GetSize();
            }
            alloc_buf_size = string_len_total + sizeof(CompressionHeader);
        } else {
            D_ASSERT(TypeIsConstantSize(p_type));
            alloc_buf_size = input.size() * GetTypeIdSize(p_type) + sizeof(CompressionHeader);
        }
        
        string file_path_prefix = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_"
            + std::to_string(new_eid) + std::string("/chunk_");
        ChunkCacheManager::ccm->CreateSegment(cdf_id, file_path_prefix, alloc_buf_size, false);
        ChunkCacheManager::ccm->PinSegment(cdf_id, file_path_prefix, &buf_ptr, &buf_size);

        // Copy (or Compress and Copy) DataChunk
        if (l_type == LogicalType::VARCHAR) {
            size_t offset = 0;
            size_t input_size = input.size();
            CompressionHeader comp_header(UNCOMPRESSED, input_size);
            memcpy(buf_ptr + offset, &comp_header, sizeof(CompressionHeader));
            offset += sizeof(CompressionHeader);
            uint32_t string_len;
            string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();
            for (size_t i = 0; i < input.size(); i++) {
                string_len = string_buffer[i].GetSize();
                memcpy(buf_ptr + offset, &string_len, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(buf_ptr + offset, string_buffer[i].GetDataUnsafe(), string_len);
                offset += string_len;
            }
        } else if (l_type == LogicalType::ADJLIST) {
            memcpy(buf_ptr, input.data[input_chunk_idx].GetData(), alloc_buf_size);
        } else {
            // TODO type support check should be done by CompressionFunction
            if (BitpackingPrimitives::TypeIsSupported(p_type)) {
                // Set Compression Function
                CompressionFunction comp_func(best_compression_function, p_type); // best_compression_function = BITPACKING
                // Compress
                size_t input_size = input.size();
                data_ptr_t data_to_compress = input.data[input_chunk_idx].GetData();
                CompressionHeader comp_header(BITPACKING, input_size);
                memcpy(buf_ptr, &comp_header, sizeof(CompressionHeader));
                comp_func.Compress(buf_ptr + sizeof(CompressionHeader), buf_size, data_to_compress, input_size);
            } else {
                size_t input_size = input.size();
                CompressionHeader comp_header(UNCOMPRESSED, input_size);
                memcpy(buf_ptr, &comp_header, sizeof(CompressionHeader));
                memcpy(buf_ptr + sizeof(CompressionHeader), input.data[input_chunk_idx].GetData(), alloc_buf_size - sizeof(CompressionHeader));
            }
        }

        // Set Dirty & Unpin Segment & Flush
        ChunkCacheManager::ccm->SetDirty(cdf_id);
        ChunkCacheManager::ccm->UnPinSegment(cdf_id);
        input_chunk_idx++;
    }
}

} // namespace duckdb