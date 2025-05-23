#include "storage/extent/extent_manager.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "storage/extent/compression/compression_function.hpp"
#include "storage/cache/chunk_cache_manager.h"

#include "common/types/data_chunk.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/directory_helper.hpp"

namespace duckdb {

ExtentManager::ExtentManager() {}

ExtentID
ExtentManager::CreateExtent(ClientContext &context, DataChunk &input, PartitionCatalogEntry &part_cat, PropertySchemaCatalogEntry &ps_cat) {
    // Get New ExtentID & Create ExtentCatalogEntry
    PartitionID pid = part_cat.GetPartitionID();
    PropertySchemaID psid = ps_cat.GetOid();
    ExtentID new_eid = part_cat.GetNewExtentID();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = DEFAULT_EXTENT_PREFIX + std::to_string(new_eid);
    CreateExtentInfo extent_info(DEFAULT_SCHEMA, extent_name.c_str(), ExtentType::EXTENT, new_eid, pid, psid, input.size());
    ExtentCatalogEntry *extent_cat_entry = (ExtentCatalogEntry *)cat_instance.CreateExtent(context, &extent_info);
    
    // MkDir for the extent
    std::string extent_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid);
    MkDir(extent_dir_path, true);

    // Append Chunk
    //_AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, *extent_cat_entry, pid, new_eid);
    _UpdatePartitionMinMaxArray(context, cat_instance, part_cat, ps_cat, *extent_cat_entry);
    return new_eid;
}

void
ExtentManager::CreateExtent(ClientContext &context, DataChunk &input, PartitionCatalogEntry &part_cat, PropertySchemaCatalogEntry &ps_cat, ExtentID new_eid) {
    // Create ExtentCatalogEntry
    PartitionID pid = part_cat.GetPartitionID();
    PropertySchemaID psid = ps_cat.GetOid();
    Catalog& cat_instance = context.db->GetCatalog();
    string extent_name = DEFAULT_EXTENT_PREFIX + std::to_string(new_eid);
    CreateExtentInfo extent_info(DEFAULT_SCHEMA, extent_name.c_str(), ExtentType::EXTENT, new_eid, pid, psid, input.size());
    ExtentCatalogEntry *extent_cat_entry = (ExtentCatalogEntry *)cat_instance.CreateExtent(context, &extent_info);

    // MkDir for the extent
    std::string extent_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid);
    MkDir(extent_dir_path, true);

    // Append Chunk
    //_AppendChunkToExtent(context, input, cat_instance, prop_schema_cat_entry, *extent_cat_entry, pid, new_eid);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, *extent_cat_entry, pid, new_eid);
    _UpdatePartitionMinMaxArray(context, cat_instance, part_cat, ps_cat, *extent_cat_entry);
}

void ExtentManager::AppendChunkToExistingExtent(ClientContext &context, DataChunk &input, ExtentID eid) {
    Catalog& cat_instance = context.db->GetCatalog();
    ExtentCatalogEntry* extent_cat_entry = 
        (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, DEFAULT_EXTENT_PREFIX + std::to_string(eid));
    PartitionID pid = static_cast<PartitionID>(eid >> 16);
    _AppendChunkToExtentWithCompression(context, input, cat_instance, *extent_cat_entry, pid, eid);
}

void ExtentManager::_AppendChunkToExtentWithCompression(ClientContext &context, DataChunk &input, Catalog& cat_instance, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID new_eid) {
    // Reaquire partition, property schema catalog entry
    auto ps_oid = extent_cat_entry.ps_oid;
    auto& prop_schema_cat_entry = *((PropertySchemaCatalogEntry *)cat_instance.GetEntry(context, DEFAULT_SCHEMA, ps_oid));
    auto partition_oid = prop_schema_cat_entry.partition_oid;
    auto& part_cat_entry = *((PartitionCatalogEntry *)cat_instance.GetEntry(context, DEFAULT_SCHEMA, partition_oid));
    auto& property_keys = *prop_schema_cat_entry.GetPropKeyIDs();

    // Actual run
    idx_t input_chunk_idx = 0;
    ChunkDefinitionID cdf_id_base = new_eid;
    cdf_id_base = cdf_id_base << 32;
    for (auto &l_type : input.GetTypes()) {
        auto prop_key_id = property_keys[input_chunk_idx];
        auto append_chunk_start = std::chrono::high_resolution_clock::now();
        // Get Physical Type
        PhysicalType p_type = l_type.InternalType();
        // For each Vector in DataChunk create new chunk definition
        LocalChunkDefinitionID chunk_definition_idx;
        if (l_type == LogicalType::FORWARD_ADJLIST || l_type == LogicalType::BACKWARD_ADJLIST) {
            chunk_definition_idx = extent_cat_entry.GetNextAdjListChunkDefinitionID();
        } else {
            chunk_definition_idx = extent_cat_entry.GetNextChunkDefinitionID();
        }
        ChunkDefinitionID cdf_id = cdf_id_base + chunk_definition_idx;
        string chunkdefinition_name = DEFAULT_CHUNKDEFINITION_PREFIX + std::to_string(cdf_id);
        CreateChunkDefinitionInfo chunkdefinition_info(DEFAULT_SCHEMA, chunkdefinition_name, l_type);
        ChunkDefinitionCatalogEntry *chunkdefinition_cat = 
            (ChunkDefinitionCatalogEntry *)cat_instance.CreateChunkDefinition(context, &chunkdefinition_info);
        if (l_type == LogicalType::FORWARD_ADJLIST || l_type == LogicalType::BACKWARD_ADJLIST) {
            extent_cat_entry.AddAdjListChunkDefinitionID(cdf_id);
        } else {
            extent_cat_entry.AddChunkDefinitionID(cdf_id);
        }
        chunkdefinition_cat->SetNumEntriesInColumn(input.size());

        // Analyze compression to find best compression method
        CompressionFunctionType best_compression_function = UNCOMPRESSED;
        //if (l_type == LogicalType::VARCHAR) best_compression_function = DICTIONARY;
        // Create Compressionheader, based on nullity
        CompressionHeader comp_header(UNCOMPRESSED, input.size(), SwizzlingType::SWIZZLE_NONE);
        if (FlatVector::HasNull(input.data[input_chunk_idx])) {
            if (input.size() != FlatVector::Validity(input.data[input_chunk_idx]).CountValid(input.size())) {
                comp_header.SetNullMask();
            }
        }
        auto comp_header_size = comp_header.GetSizeWoBitSet();

        uint8_t *buf_ptr = nullptr;
        size_t buf_size = 0;
        size_t alloc_buf_size = 0;
        size_t bitmap_size = 0;
		const size_t slot_for_num_adj = 1;

        /**
         * TODO: check null is correctly handled for LIST types
         * If there is null, we cannot use the size of the input vector to calculate the size of the buffer
         */

        // Calculate the size of the buffer to allocate
        if (l_type.id() == LogicalTypeId::FORWARD_ADJLIST || l_type.id() == LogicalTypeId::BACKWARD_ADJLIST) {
            idx_t *adj_list_buffer = (idx_t*) input.data[input_chunk_idx].GetData();
            size_t num_adj_lists = adj_list_buffer[0];
            size_t adj_list_size = num_adj_lists == 0 ? 0 : adj_list_buffer[num_adj_lists];
            alloc_buf_size = 
                sizeof(idx_t) * (slot_for_num_adj + adj_list_size) +
                comp_header_size;
        } else if (l_type.id() == LogicalTypeId::VARCHAR) {
            // New Implementation
            size_t string_len_total = 0;
            string_t *string_buffer = (string_t*)input.data[input_chunk_idx].GetData();

             // Accumulate the length of all non-inlined strings
            for (size_t i = 0; i < input.size(); i++) {
                if(FlatVector::IsNull(input.data[input_chunk_idx], i)) continue;
                string_len_total += string_buffer[i].IsInlined() ? 0 : string_buffer[i].GetSize();
            }

            // Accumulate the string_t array length
            if (best_compression_function == DICTIONARY)
                string_len_total += (input.size() * 2 * sizeof(uint32_t)); // for selection buffer, index buffer
            else
                string_len_total += (input.size() * sizeof(string_t)); // string len field

            // Calculate the final size
            alloc_buf_size = string_len_total + comp_header_size + 512;
        } else if (l_type.id() == LogicalTypeId::LIST) {
            size_t list_len_total = 0;
            size_t child_type_size = GetTypeIdSize(ListType::GetChildType(l_type).InternalType());
            list_entry_t *list_buffer = (list_entry_t*)input.data[input_chunk_idx].GetData();
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

        // Check null mask
        if (comp_header.HasNullMask()) {
            bitmap_size = (input.size() + 7) / 8;
            // alloc_buf_size += bitmap_size;
            comp_header.SetNullBitmapOffset(alloc_buf_size);
        }

        // Get Buffer from Cache Manager. Cache Object ID: 64bit = ChunkDefinitionID
        string file_path_prefix = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid) + "/ext_"
            + std::to_string(new_eid) + std::string("/chunk_");
        ChunkCacheManager::ccm->CreateSegment(cdf_id, file_path_prefix, alloc_buf_size + bitmap_size, false);
        ChunkCacheManager::ccm->PinSegment(cdf_id, file_path_prefix, &buf_ptr, &buf_size, false, true);
        std::memset(buf_ptr, 0, buf_size);

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
                size_t string_t_offset = comp_header_size;
                size_t string_data_offset = comp_header_size + input_size * sizeof(string_t);
                comp_header.SetSwizzlingType(SwizzlingType::SWIZZLE_VARCHAR);
                memcpy(buf_ptr, &comp_header, comp_header_size);

                // For each string_t, write string_t and actual string if not inlined
                string_t *string_buffer = (string_t *)input.data[input_chunk_idx].GetData();
                uint64_t accumulated_string_len = 0;
                for (size_t i = 0; i < input.size(); i++) {
                    if(!FlatVector::IsNull(input.data[input_chunk_idx], i)) {
                        string_t& str = string_buffer[i];
                        if (str.IsInlined()) {
                            memcpy(buf_ptr + string_t_offset, &str, sizeof(string_t));
                        } else {
                            // Copy actual string
                            memcpy(buf_ptr + string_data_offset + accumulated_string_len, str.GetDataUnsafe(), str.GetSize());
                            // Calculate pointer address
                            uint8_t *swizzled_pointer = buf_ptr + string_data_offset + accumulated_string_len;
                            string_t swizzled_str(reinterpret_cast<char *>(swizzled_pointer), str.GetSize());
                            memcpy(buf_ptr + string_t_offset, &swizzled_str, sizeof(string_t));
                            accumulated_string_len += str.GetSize();
                        }
                    }
                    string_t_offset += sizeof(string_t);
                }
            }
        } else if (l_type.id() == LogicalTypeId::FORWARD_ADJLIST || l_type.id() == LogicalTypeId::BACKWARD_ADJLIST) {
            idx_t *adj_list_buffer = (idx_t*) input.data[input_chunk_idx].GetData();
            memcpy(buf_ptr, &comp_header, comp_header_size);
            memcpy(buf_ptr + comp_header_size, input.data[input_chunk_idx].GetData(), alloc_buf_size - comp_header_size);
        } else if (l_type.id() == LogicalTypeId::LIST) {
            list_entry_t *list_buffer = (list_entry_t*) input.data[input_chunk_idx].GetData();
            size_t input_size = input.size();
            Vector &child_vec = ListVector::GetEntry(input.data[input_chunk_idx]);
            memcpy(buf_ptr, &comp_header, comp_header_size);
            memcpy(buf_ptr + comp_header_size, input.data[input_chunk_idx].GetData(), input_size * sizeof(list_entry_t));
            memcpy(buf_ptr + comp_header_size + input_size * sizeof(list_entry_t), child_vec.GetData(), alloc_buf_size - comp_header_size - input_size * sizeof(list_entry_t));
            // icecream::ic.enable(); IC(); IC(comp_header_size + input_size * sizeof(list_entry_t), alloc_buf_size - comp_header_size - input_size * sizeof(list_entry_t)); icecream::ic.disable();
        } else {
            // Create MinMaxArray in ChunkDefinitionCatalog. We support only INT types for now.
            size_t input_size = input.size();
            if (input.GetTypes()[input_chunk_idx] == LogicalType::UBIGINT ||
                input.GetTypes()[input_chunk_idx] == LogicalType::ID ||
                input.GetTypes()[input_chunk_idx] == LogicalType::BIGINT) {
                chunkdefinition_cat->CreateMinMaxArray(input.data[input_chunk_idx], input_size);
                // _UpdatePartitionMinMaxArray(part_cat_entry, prop_key_id, *chunkdefinition_cat);
                // part_cat_entry.UpdateWelfordStdDevArray(prop_key_id, input.data[input_chunk_idx], input_size);
                // if(input_chunk_idx == 0) fprintf(stdout, "StdDev: %f\n", part_cat_entry.GetStdDev(prop_key_id));
            }

            // Copy Data Into Cache
            // TODO type support check should be done by CompressionFunction
            if (best_compression_function == BITPACKING && BitpackingPrimitives::TypeIsSupported(p_type)) {
                D_ASSERT(false);
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
        if (comp_header.HasNullMask()) {
            auto *validity_data = (char *)(FlatVector::Validity(input.data[input_chunk_idx]).GetData());
            memcpy(buf_ptr + alloc_buf_size, validity_data, bitmap_size);
        }
        auto chunk_compression_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> chunk_compression_duration = chunk_compression_end - chunk_compression_start;

        // Set Dirty & Unpin Segment & Flush
        ChunkCacheManager::ccm->SetDirty(cdf_id);
        ChunkCacheManager::ccm->UnPinSegment(cdf_id);
        input_chunk_idx++;

        auto append_chunk_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> chunk_duration = append_chunk_end - append_chunk_start;
    }
}

void ExtentManager::_UpdatePartitionMinMaxArray(ClientContext &context, Catalog& cat_instance, PartitionCatalogEntry &part_cat, PropertySchemaCatalogEntry &ps_cat, ExtentCatalogEntry &extent_cat_entry){
    auto& property_keys = *ps_cat.GetPropKeyIDs();
    auto& chunkdef_ids = extent_cat_entry.chunks;
    for (int i = 0; i < property_keys.size(); i++) {
        auto property_key_id = property_keys[i];
        auto chunkdef_id = chunkdef_ids[i];
        auto chunkdef_cat_entry = (ChunkDefinitionCatalogEntry*) cat_instance.GetEntry(context, CatalogType::CHUNKDEFINITION_ENTRY, 
                                                                DEFAULT_SCHEMA, DEFAULT_CHUNKDEFINITION_PREFIX + std::to_string(chunkdef_id));
        
        if (chunkdef_cat_entry->IsMinMaxArrayExist()) {
            vector<minmax_t> minmax = move(chunkdef_cat_entry->GetMinMaxArray());
            for (auto &minmax_pair : minmax) {
                part_cat.UpdateMinMaxArray(property_key_id, minmax_pair.min, minmax_pair.max);
            }
        }
    }
}

void ExtentManager::_UpdatePartitionMinMaxArray(PartitionCatalogEntry &part_cat, PropertyKeyID prop_key_id, ChunkDefinitionCatalogEntry& chunkdef_cat_entry) {
    if (chunkdef_cat_entry.IsMinMaxArrayExist()) {
        vector<minmax_t> minmax = move(chunkdef_cat_entry.GetMinMaxArray());
        for (auto &minmax_pair : minmax) {
            part_cat.UpdateMinMaxArray(prop_key_id, minmax_pair.min, minmax_pair.max);
        }
    }
}

} // namespace duckdb