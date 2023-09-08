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

ExtentID ExtentManager::CreateExtent(ClientContext &context, DataChunk &input, PartitionCatalogEntry &partition_cat) {
    // Get New ExtentID & Create ExtentCatalogEntry
    PartitionID pid = partition_cat.GetPartitionID();
    ExtentID new_eid = partition_cat.GetNewExtentID();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = "ext_" + std::to_string(new_eid);
    CreateExtentInfo extent_info("main", extent_name.c_str(), ExtentType::EXTENT, new_eid, pid, input.size());
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.CreateExtent(context, &extent_info);
    
    // MkDir for the extent
    std::string extent_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid);
    MkDir(extent_dir_path, true);

    // Append Chunk
    //_AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, *extent_cat_entry, pid, new_eid);
    return new_eid;
}

void ExtentManager::CreateExtent(ClientContext &context, DataChunk &input, PartitionCatalogEntry &partition_cat, ExtentID new_eid) {
    // Create ExtentCatalogEntry
    PartitionID pid = partition_cat.GetPartitionID();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = "ext_" + std::to_string(new_eid);
    CreateExtentInfo extent_info("main", extent_name.c_str(), ExtentType::EXTENT, new_eid, pid, input.size());
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.CreateExtent(context, &extent_info);

    // MkDir for the extent
    std::string extent_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid);
    MkDir(extent_dir_path, true);

    // Append Chunk
    //_AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, *extent_cat_entry, pid, new_eid);
}

void ExtentManager::AppendChunkToExistingExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &property_schema_cat, ExtentID eid) {
    Catalog& cat_instance = context.db->GetCatalog();
    ExtentCatalogEntry* extent_cat_entry = (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(eid));
    PartitionID pid = property_schema_cat.GetPartitionID();
    _AppendChunkToExtentWithCompression(context, input, cat_instance, *extent_cat_entry, pid, eid);
}

void ExtentManager::_AppendChunkToExtentWithCompression(ClientContext &context, DataChunk &input, Catalog& cat_instance, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID new_eid) {
    idx_t input_chunk_idx = 0;
    ChunkDefinitionID cdf_id_base = new_eid;
    cdf_id_base = cdf_id_base << 32;
    for (auto &l_type : input.GetTypes()) {
        auto append_chunk_start = std::chrono::high_resolution_clock::now();
        // Get Physical Type
        PhysicalType p_type = l_type.InternalType();
        // For each Vector in DataChunk create new chunk definition
        LocalChunkDefinitionID chunk_definition_idx = extent_cat_entry.GetNextChunkDefinitionID();
        ChunkDefinitionID cdf_id = cdf_id_base + chunk_definition_idx;
        string chunkdefinition_name = "cdf_" + std::to_string(cdf_id);
        CreateChunkDefinitionInfo chunkdefinition_info("main", chunkdefinition_name, l_type);
        ChunkDefinitionCatalogEntry* chunkdefinition_cat = (ChunkDefinitionCatalogEntry*) cat_instance.CreateChunkDefinition(context, &chunkdefinition_info);
        extent_cat_entry.AddChunkDefinitionID(cdf_id);
        chunkdefinition_cat->SetNumEntriesInColumn(input.size());

        // Analyze compression to find best compression method
        CompressionFunctionType best_compression_function = UNCOMPRESSED;
        //if (l_type == LogicalType::VARCHAR) best_compression_function = DICTIONARY;
        // Create Compressionheader, based on nullity
        // CompressionHeader comp_header(UNCOMPRESSED, input.size(), SwizzlingType::SWIZZLE_NONE);
        CompressionHeader comp_header(UNCOMPRESSED, input.size());
        // if (FlatVector::HasNull(input.data[input_chunk_idx])) {
        //     comp_header.SetNullMask(FlatVector::GetNullMask(input.data[input_chunk_idx]));
        // }
        // auto comp_header_size = comp_header.GetValidSize();
        auto comp_header_size = sizeof(CompressionHeader);

        // Get Buffer from Cache Manager
        // Cache Object ID: 64bit = ChunkDefinitionID
        uint8_t *buf_ptr;
        size_t buf_size;
        size_t alloc_buf_size;
        if (l_type.id() == LogicalTypeId::FORWARD_ADJLIST || l_type.id() == LogicalTypeId::BACKWARD_ADJLIST) {
            idx_t *adj_list_buffer = (idx_t*) input.data[input_chunk_idx].GetData();
            alloc_buf_size = sizeof(idx_t) * adj_list_buffer[STORAGE_STANDARD_VECTOR_SIZE - 1] + comp_header_size;
        } else if (l_type.id() == LogicalTypeId::VARCHAR) {
            // New Implementation
            size_t string_len_total = 0;
            string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();

             // Accumulate the length of all non-inlined strings
            for (size_t i = 0; i < input.size(); i++)
                string_len_total += string_buffer[i].IsInlined() ? 0 : string_buffer[i].GetSize();

            // Accumulate the string_t array length
            if (best_compression_function == DICTIONARY)
                string_len_total += (input.size() * 2 * sizeof(uint32_t)); // for selection buffer, index buffer
            else
                string_len_total += (input.size() * sizeof(string_t)); // string len field

            // Calculate the final size
            alloc_buf_size = string_len_total + comp_header_size;
        } else if (l_type.id() == LogicalTypeId::LIST) {
            size_t list_len_total = 0;
            size_t child_type_size = GetTypeIdSize(ListType::GetChildType(l_type).InternalType());
            list_entry_t *list_buffer = (list_entry_t*)input.data[input_chunk_idx].GetData();
            // fprintf(stderr, "input_chunk_idx %ld list_buffer %p\n", input_chunk_idx, list_buffer);
            for (size_t i = 0; i < input.size(); i++) { // Accumulate the length of all child datas
                list_len_total += list_buffer[i].length;
            }
            list_len_total *= child_type_size;
            if (best_compression_function == DICTIONARY)
                list_len_total += (input.size() * 2 * sizeof(uint32_t)); // for selection buffer, index buffer
            else
                list_len_total += (input.size() * sizeof(list_entry_t)); // string len field
            alloc_buf_size = list_len_total + comp_header_size;
        } else {
            D_ASSERT(TypeIsConstantSize(p_type));
            alloc_buf_size = input.size() * GetTypeIdSize(p_type) + comp_header_size;
        }
        
        string file_path_prefix = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_"
            + std::to_string(new_eid) + std::string("/chunk_");
        ChunkCacheManager::ccm->CreateSegment(cdf_id, file_path_prefix, alloc_buf_size, false);
        ChunkCacheManager::ccm->PinSegment(cdf_id, file_path_prefix, &buf_ptr, &buf_size, false, true);
        // fprintf(stderr, "[ChunkCacheManager] Get size %ld buffer, requested buf size = %ld\n", buf_size, alloc_buf_size);

        // Copy (or Compress and Copy) DataChunk
        auto chunk_compression_start = std::chrono::high_resolution_clock::now();
        if (l_type.id() == LogicalTypeId::VARCHAR) {
            if (best_compression_function == DICTIONARY) {
                // Set Compression Function
                CompressionFunction comp_func(best_compression_function, p_type);
                // Compress
                size_t input_size = input.size();
                data_ptr_t data_to_compress = input.data[input_chunk_idx].GetData();
                comp_header.SetCompFuncType(DICTIONARY);
                memcpy(buf_ptr, &comp_header, comp_header_size);
                comp_func.Compress(buf_ptr + comp_header_size, buf_size - comp_header_size, data_to_compress, input_size);
            } else {
                // Copy CompressionHeader
                size_t input_size = input.size();
                size_t string_len_offset = sizeof(CompressionHeader);
                size_t string_data_offset = sizeof(CompressionHeader) + input_size * sizeof(uint64_t);
                CompressionHeader comp_header(UNCOMPRESSED, input_size);
                memcpy(buf_ptr, &comp_header, sizeof(CompressionHeader));

                uint32_t string_len;
                uint64_t accumulated_string_len = 0;
                string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();

                for (size_t i = 0; i < input.size(); i++) {
                    accumulated_string_len += string_buffer[i].GetSize();
                    memcpy(buf_ptr + string_len_offset, &accumulated_string_len, sizeof(uint64_t));
                    string_len_offset += sizeof(uint64_t);
                }

                for (size_t i = 0; i < input.size(); i++) {
                    string_len = string_buffer[i].GetSize();
                    memcpy(buf_ptr + string_data_offset, string_buffer[i].GetDataUnsafe(), string_len);
                    string_data_offset += string_len;
                }
            }
        } else if (l_type.id() == LogicalTypeId::FORWARD_ADJLIST || l_type.id() == LogicalTypeId::BACKWARD_ADJLIST) {
            idx_t *adj_list_buffer = (idx_t*) input.data[input_chunk_idx].GetData();
            size_t input_size = adj_list_buffer[STORAGE_STANDARD_VECTOR_SIZE - 1];
            memcpy(buf_ptr, &comp_header, comp_header_size);
            memcpy(buf_ptr + comp_header_size, input.data[input_chunk_idx].GetData(), alloc_buf_size - comp_header_size);
        } else if (l_type.id() == LogicalTypeId::LIST) {
            list_entry_t *list_buffer = (list_entry_t*) input.data[input_chunk_idx].GetData();
            size_t input_size = input.size();
            Vector &child_vec = ListVector::GetEntry(input.data[input_chunk_idx]);
            memcpy(buf_ptr, &comp_header, comp_header_size);
            memcpy(buf_ptr + comp_header_size, input.data[input_chunk_idx].GetData(), input_size * sizeof(list_entry_t));
            memcpy(buf_ptr + comp_header_size + input_size * sizeof(list_entry_t), child_vec.GetData(), alloc_buf_size - comp_header_size - input_size * sizeof(list_entry_t));
            icecream::ic.enable(); IC(); IC(comp_header_size + input_size * sizeof(list_entry_t), alloc_buf_size - comp_header_size - input_size * sizeof(list_entry_t)); icecream::ic.disable();
        } else {
            // Create MinMaxArray in ChunkDefinitionCatalog
            size_t input_size = input.size();
            if (input.GetTypes()[input_chunk_idx] == LogicalType::UBIGINT) {
                chunkdefinition_cat->CreateMinMaxArray(input.data[input_chunk_idx], input_size);
            }

            // Copy Data Into Cache
            // TODO type support check should be done by CompressionFunction
            if (best_compression_function == BITPACKING && BitpackingPrimitives::TypeIsSupported(p_type)) {
                // Set Compression Function
                CompressionFunction comp_func(best_compression_function, p_type); // best_compression_function = BITPACKING
                // Compress
                data_ptr_t data_to_compress = input.data[input_chunk_idx].GetData();
                comp_header.SetCompFuncType(BITPACKING);
                memcpy(buf_ptr, &comp_header, comp_header_size);
                comp_func.Compress(buf_ptr + comp_header_size, buf_size - comp_header_size, data_to_compress, input_size);
            } else {
                memcpy(buf_ptr, &comp_header, comp_header_size);
                memcpy(buf_ptr + comp_header_size, input.data[input_chunk_idx].GetData(), alloc_buf_size - comp_header_size);
            }
        }
        auto chunk_compression_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> chunk_compression_duration = chunk_compression_end - chunk_compression_start;

        // Set Dirty & Unpin Segment & Flush
        ChunkCacheManager::ccm->SetDirty(cdf_id);
        ChunkCacheManager::ccm->UnPinSegment(cdf_id);
        input_chunk_idx++;

        auto append_chunk_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> chunk_duration = append_chunk_end - append_chunk_start;
        // fprintf(stdout, "\t\tAppendChunk %ld -> %p size %ld, Total Elapsed: %.6f, Compression Elapsed: %.3f\n", cdf_id, buf_ptr, input.size(), chunk_duration.count(), chunk_compression_duration.count());
    }
}

} // namespace duckdb