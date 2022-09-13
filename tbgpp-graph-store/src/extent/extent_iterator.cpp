#include "extent/extent_iterator.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "cache/disk_aio/TypeDef.hpp"
#include "cache/chunk_cache_manager.h"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "common/types/data_chunk.hpp"
#include "extent/compression/compression_function.hpp"

#include "icecream.hpp" 

namespace duckdb {

// TODO: select extent to iterate using min & max & key
// Initialize iterator that iterates all extents
void ExtentIterator::Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry) {
    ps_cat_entry = property_schema_cat_entry;
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
        // Initialize Data Chunks
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++) data_chunks[i] = new DataChunk();
    } else {
        support_double_buffering = false;
        num_data_chunks = 1;
        // Initialize Data Chunks
        data_chunks[0] = new DataChunk();
        for (int i = 1; i < MAX_NUM_DATA_CHUNKS; i++) data_chunks[i] = nullptr;
    }

    toggle = 0;
    current_idx = 0;
    max_idx = property_schema_cat_entry->extent_ids.size();
    ext_property_types = move(property_schema_cat_entry->GetTypes());

    for (int i = 0; i < property_schema_cat_entry->extent_ids.size(); i++)
        ext_ids_to_iterate.push_back(property_schema_cat_entry->extent_ids[i]);

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        size_t chunk_size = extent_cat_entry->chunks.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        for (int i = 0; i < chunk_size; i++) {
            ChunkDefinitionID cdf_id = extent_cat_entry->chunks[i];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
    }
}

void ExtentIterator::Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_) {
    ps_cat_entry = property_schema_cat_entry;
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
        // Initialize Data Chunks
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++) data_chunks[i] = new DataChunk();
    } else {
        support_double_buffering = false;
        num_data_chunks = 1;
        // Initialize Data Chunks
        data_chunks[0] = new DataChunk();
        for (int i = 1; i < MAX_NUM_DATA_CHUNKS; i++) data_chunks[i] = nullptr;
    }

    toggle = 0;
    current_idx = 0;
    max_idx = property_schema_cat_entry->extent_ids.size();
    ext_property_types = move(target_types_);
    target_idxs = move(target_idxs_);
    for (size_t i = 0; i < property_schema_cat_entry->extent_ids.size(); i++)
        ext_ids_to_iterate.push_back(property_schema_cat_entry->extent_ids[i]);

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        size_t chunk_size = ext_property_types.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            if (ext_property_types[i] == LogicalType::ID) continue;
            ChunkDefinitionID cdf_id = extent_cat_entry->chunks[target_idxs[j++]];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
    }
}

void ExtentIterator::Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid) {
    ps_cat_entry = property_schema_cat_entry;
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
        // Initialize Data Chunks
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++) data_chunks[i] = new DataChunk();
    } else {
        support_double_buffering = false;
        num_data_chunks = 1;
        // Initialize Data Chunks
        data_chunks[0] = new DataChunk();
        for (int i = 1; i < MAX_NUM_DATA_CHUNKS; i++) data_chunks[i] = nullptr;
    }

    toggle = 0;
    current_idx = 0;
    max_idx = 1;
    ext_property_types = move(target_types_);
    target_idxs = move(target_idxs_);

    ext_ids_to_iterate.push_back(target_eid);

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        size_t chunk_size = ext_property_types.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            if (ext_property_types[i] == LogicalType::ID) continue;
            ChunkDefinitionID cdf_id = extent_cat_entry->chunks[target_idxs[j++]];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
    }
}

// Get Next Extent with all properties
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, bool is_output_chunk_initialized) {
    // We should avoid data copy here.. but copy for demo temporarliy
    
    // Keep previous values
    // fprintf(stdout, "Z\n");
    int prev_toggle = toggle;
    idx_t previous_idx = current_idx++;
    toggle = (toggle + 1) % num_data_chunks;
    if (current_idx > max_idx) return false;

    // fprintf(stdout, "K\n");
    // Request I/O to the next extent if we can support double buffering
    Catalog& cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx) {
        // fprintf(stdout, "Q\n");
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        // Unpin previous chunks
        if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[toggle].size() == 0);
        for (size_t i = 0; i < io_requested_cdf_ids[toggle].size(); i++)
            ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[toggle][i]);

        size_t chunk_size = ext_property_types.empty() ? extent_cat_entry->chunks.size() : ext_property_types.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);
        
        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            if (!ext_property_types.empty() && ext_property_types[i] == LogicalType::ID) continue;
            ChunkDefinitionID cdf_id = target_idxs.empty() ? 
                extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idxs[j++]];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
    }

    // fprintf(stdout, "W\n");
    // Request chunk cache manager to finalize I/O
    for (int i = 0; i < io_requested_cdf_ids[prev_toggle].size(); i++)
        ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[prev_toggle][i], true, false);

    // Initialize DataChunk using cached buffer
    /*data_chunks[prev_toggle]->Destroy();
    data_chunks[prev_toggle]->Initialize(ext_property_types, io_requested_buf_ptrs[prev_toggle]);
    data_chunks[prev_toggle]->SetCardinality(io_requested_buf_sizes[prev_toggle][0] / GetTypeIdSize(ext_property_types[0].InternalType())); // XXX.. bug can occur
    output = data_chunks[prev_toggle];*/

    // Initialize output DataChunk & copy each column
    // fprintf(stdout, "E\n");
    if (!is_output_chunk_initialized) {
        output.Reset();
        // fprintf(stdout, "EE\n");
        output.Initialize(ext_property_types);
    }
    int idx_for_cardinality = -1;
    CompressionHeader comp_header;
    // TODO record data cardinality in Chunk Definition?
    for (size_t i = 0; i < ext_property_types.size(); i++) {
        if (ext_property_types[i] == LogicalType::VARCHAR) {
            idx_for_cardinality = i;
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            break;
        } else if (ext_property_types[i] == LogicalType::ADJLIST) {
            continue;
        } else if (ext_property_types[i] == LogicalType::ID) {
            continue;
        } else {
            idx_for_cardinality = i;
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            break;
        }
    }
    // fprintf(stdout, "R\n");
    if (idx_for_cardinality == -1) {
        throw InvalidInputException("ExtentIt Cardinality Bug");
    } else {
        // fprintf(stdout, "Set Cardinality %ld\n", comp_header.data_len);
        output.SetCardinality(comp_header.data_len);
    }
    output_eid = ext_ids_to_iterate[previous_idx];
    // fprintf(stdout, "T, size = %ld\n", ext_property_types.size());
    for (size_t i = 0; i < ext_property_types.size(); i++) {
        if (ext_property_types[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            fprintf(stdout, "Load Column %ld, cdf %ld, size = %ld %ld, io_req_buf_size = %ld comp_type = %d, data_len = %ld, %p\n", 
                            i, io_requested_cdf_ids[prev_toggle][i], output.size(), comp_header.data_len, 
                            io_requested_buf_sizes[prev_toggle][i], (int)comp_header.comp_type, comp_header.data_len, io_requested_buf_ptrs[prev_toggle][i]);
        } else {
            fprintf(stdout, "Load Column %ld\n", i);
        }
        if (ext_property_types[i] == LogicalType::VARCHAR) {
            // fprintf(stdout, "VARCHAR\n");
            if (comp_header.comp_type == DICTIONARY) {
                PhysicalType p_type = ext_property_types[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] -  sizeof(CompressionHeader),
                                       output.data[i], comp_header.data_len);
            } else {
                auto strings = FlatVector::GetData<string_t>(output.data[i]);
                uint64_t string_offset, prev_string_offset = 0;
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader));
                size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t);
                for (size_t output_idx = 0; output_idx < comp_header.data_len; output_idx++) {
                    string_offset = offset_arr[output_idx];
                    strings[output_idx] = StringVector::AddString(output.data[i], (char*)(io_requested_buf_ptrs[prev_toggle][i] + string_data_offset), string_offset - prev_string_offset);

                    string_data_offset += (string_offset - prev_string_offset);
                    prev_string_offset = string_offset;
                    D_ASSERT(string_data_offset <= io_requested_buf_sizes[prev_toggle][i]);
                }
            }
        } else if (ext_property_types[i] == LogicalType::ADJLIST) {
            // fprintf(stdout, "ADJLIST\n");
            // TODO we need to allocate buffer for adjlist
            // idx_t *adjListBase = (idx_t *)io_requested_buf_ptrs[prev_toggle][i];
            // size_t adj_list_end = adjListBase[STANDARD_VECTOR_SIZE - 1];
            // // output.InitializeAdjListColumn(i, adj_list_size);
            // // memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i], io_requested_buf_sizes[prev_toggle][i]);
            // memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i], STANDARD_VECTOR_SIZE * sizeof(idx_t));
            // VectorListBuffer &adj_list_buffer = (VectorListBuffer &)*output.data[i].GetAuxiliary();
            // for (idx_t adj_list_idx = STANDARD_VECTOR_SIZE; adj_list_idx < adj_list_end; adj_list_idx++) {
            //     adj_list_buffer.PushBack(Value::UBIGINT(adjListBase[adj_list_idx]));
            // }
            // // memcpy(output.data[i].GetAuxiliary()->GetData(), io_requested_buf_ptrs[prev_toggle][i] + STANDARD_VECTOR_SIZE * sizeof(idx_t), 
            // //        adj_list_size * sizeof(idx_t));
        } else if (ext_property_types[i] == LogicalType::ID) {
            // fprintf(stdout, "ID\n");
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[i].GetData();
            for (size_t seqno = 0; seqno < comp_header.data_len; seqno++)
                id_column[seqno] = physical_id_base + seqno;
        } else {
            // fprintf(stdout, "ELSE\n");
            if (comp_header.comp_type == BITPACKING) {
                // fprintf(stdout, "ELSE-A\n");
                PhysicalType p_type = ext_property_types[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] -  sizeof(CompressionHeader),
                                       output.data[i], comp_header.data_len);
            } else {
                // fprintf(stdout, "ELSE-B\n");
                memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] - sizeof(CompressionHeader));
            }
        }
    }
    // fprintf(stdout, "U\n");
    
    return true;
}

// Get Next Extent with filterKey
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, std::string filterKey, duckdb::Value filterValue, bool is_output_chunk_initialized) {
    // TODO we assume that there is only one tuple that matches the predicate
    // We should avoid data copy here.. but copy for demo temporarliy
    
    // Keep previous values
    // fprintf(stdout, "Z\n");
    int prev_toggle = toggle;
    idx_t previous_idx = current_idx++;
    toggle = (toggle + 1) % num_data_chunks;
    if (current_idx > max_idx) return false;

    // fprintf(stdout, "K\n");
    // Request I/O to the next extent if we can support double buffering
    Catalog& cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx) {
        // fprintf(stdout, "Q\n");
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        // Unpin previous chunks
        if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[toggle].size() == 0);
        for (size_t i = 0; i < io_requested_cdf_ids[toggle].size(); i++)
            ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[toggle][i]);

        size_t chunk_size = ext_property_types.empty() ? extent_cat_entry->chunks.size() : ext_property_types.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);
        
        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            if (!ext_property_types.empty() && ext_property_types[i] == LogicalType::ID) continue;
            ChunkDefinitionID cdf_id = target_idxs.empty() ? 
                extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idxs[j++]];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
    }

    // fprintf(stdout, "W\n");
    // Request chunk cache manager to finalize I/O
    for (int i = 0; i < io_requested_cdf_ids[prev_toggle].size(); i++)
        ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[prev_toggle][i], true, false);

    vector<string> pks = move(ps_cat_entry->GetKeys());
    auto idx = std::find(pks.begin(), pks.end(), filterKey);
    if (idx == pks.end()) throw InvalidInputException("");
    output_eid = ext_ids_to_iterate[previous_idx];
    ChunkDefinitionID filter_cdf_id = (ChunkDefinitionID) output_eid;
    filter_cdf_id = filter_cdf_id << 32;
    filter_cdf_id = filter_cdf_id + (idx - pks.begin());

    // Initialize output DataChunk & copy each column
    // fprintf(stdout, "E\n");
    if (!is_output_chunk_initialized) {
        output.Reset();
        // fprintf(stdout, "EE\n");
        output.Initialize(ext_property_types);
    }

    idx_t scan_start_offset, scan_end_offset, scan_length;
    ChunkDefinitionCatalogEntry* cdf_cat_entry = 
            (ChunkDefinitionCatalogEntry*) cat_instance.GetEntry(context, CatalogType::CHUNKDEFINITION_ENTRY, "main", "cdf_" + std::to_string(filter_cdf_id));
    if (cdf_cat_entry->IsMinMaxArrayExist()) {
        vector<minmax_t> minmax = move(cdf_cat_entry->GetMinMaxArray());

        bool find_block_to_scan = false;
        for (size_t i = 0; i < minmax.size(); i++) {
            // if (i == 0)
                // std::cout << "Min: " << minmax[i].min << ", Max: " << minmax[i].max << std::endl;
            if (minmax[i].min <= filterValue.GetValue<idx_t>() && minmax[i].max >= filterValue.GetValue<idx_t>()) {
                scan_start_offset = i * MIN_MAX_ARRAY_SIZE;
                scan_end_offset = MIN((i + 1) * MIN_MAX_ARRAY_SIZE, cdf_cat_entry->GetNumEntriesInColumn());
                find_block_to_scan = true;
                break;
            }
        }
        if (!find_block_to_scan) {
            output.SetCardinality(0);
            return true;
        }
    } else {
        scan_start_offset = 0;
        scan_end_offset = cdf_cat_entry->GetNumEntriesInColumn();
    }
    
    scan_length = scan_end_offset - scan_start_offset;

    // Find the column index
IC(filter_cdf_id);
for( auto& i: io_requested_cdf_ids[prev_toggle]) { IC( i ); }

    auto col_idx_find_result = std::find(io_requested_cdf_ids[prev_toggle].begin(), io_requested_cdf_ids[prev_toggle].end(), filter_cdf_id);
    if (col_idx_find_result == io_requested_cdf_ids[prev_toggle].end()) throw InvalidInputException("I/O Error");
    idx_t col_idx = col_idx_find_result - io_requested_cdf_ids[prev_toggle].begin();

    // Get Compression Header
    int idx_for_cardinality = -1;
    CompressionHeader comp_header;
    // TODO record data cardinality in Chunk Definition?
    for (size_t i = 0; i < ext_property_types.size(); i++) {
        if (ext_property_types[i] == LogicalType::VARCHAR) {
            idx_for_cardinality = i;
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            break;
        } else if (ext_property_types[i] == LogicalType::ADJLIST) {
            continue;
        } else if (ext_property_types[i] == LogicalType::ID) {
            continue;
        } else {
            idx_for_cardinality = i;
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            break;
        }
    }

    // Find the index of a row that matches a predicate
    bool find_matched_row = false;
    idx_t matched_row_idx;
    if (ext_property_types[col_idx] == LogicalType::VARCHAR) {
        if (comp_header.comp_type == DICTIONARY) {
            throw NotImplementedException("Filter predicate on DICTIONARY compression is not implemented yet");
        } else {
            auto strings = FlatVector::GetData<string_t>(output.data[col_idx]);
            size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t);
            uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[prev_toggle][col_idx] + sizeof(CompressionHeader));
            uint64_t string_offset, prev_string_offset;
            for (size_t input_idx = scan_start_offset; input_idx < scan_end_offset; input_idx++) {
                prev_string_offset = input_idx == 0 ? 0 : offset_arr[input_idx - 1];
                string_offset = offset_arr[input_idx];
                string string_val((char*)(io_requested_buf_ptrs[prev_toggle][col_idx] + string_data_offset + prev_string_offset), string_offset - prev_string_offset);
                Value str_val(string_val);
                if (str_val == filterValue) {
                    matched_row_idx = input_idx;
                    find_matched_row = true;
                    break;
                }
            }
        }
    } else if (ext_property_types[col_idx] == LogicalType::ADJLIST) {
        throw InvalidInputException("Filter predicate on ADJLIST column");
    } else if (ext_property_types[col_idx] == LogicalType::ID) {
        throw InvalidInputException("Filter predicate on PID column");
    } else {
        if (comp_header.comp_type == BITPACKING) {
            throw NotImplementedException("Filter predicate on BITPACKING compression is not implemented yet");
        } else {
            LogicalType column_type = ext_property_types[col_idx];
            Vector column_vec(column_type, (data_ptr_t)(io_requested_buf_ptrs[prev_toggle][col_idx] + sizeof(CompressionHeader)));
            for (idx_t i = scan_start_offset; i < scan_end_offset; i++) {
                if (column_vec.GetValue(i) == filterValue) {
                    matched_row_idx = i;
                    find_matched_row = true;
                    break;
                }
            }
        }
    }

    // fprintf(stdout, "R\n");
    if (idx_for_cardinality == -1) {
        throw InvalidInputException("ExtentIt Cardinality Bug");
    } else {
        if (find_matched_row) {
            output.SetCardinality(1); // TODO 1 -> matched tuple count
        } else {
            output.SetCardinality(0);
            return true;
        }
    }
    
    // fprintf(stdout, "T, size = %ld\n", ext_property_types.size());
    for (size_t i = 0; i < ext_property_types.size(); i++) {
        if (ext_property_types[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            // fprintf(stdout, "Load Column %ld, cdf %ld, size = %ld %ld, io_req_buf_size = %ld comp_type = %d, data_len = %ld, %p\n", 
            //                 i, io_requested_cdf_ids[prev_toggle][i], output.size(), comp_header.data_len, 
            //                 io_requested_buf_sizes[prev_toggle][i], (int)comp_header.comp_type, comp_header.data_len, io_requested_buf_ptrs[prev_toggle][i]);
        } else {
            // fprintf(stdout, "Load Column %ld\n", i);
        }
        if (ext_property_types[i] == LogicalType::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                PhysicalType p_type = ext_property_types[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] -  sizeof(CompressionHeader),
                                       output.data[i], comp_header.data_len);
            } else {
                auto strings = FlatVector::GetData<string_t>(output.data[i]);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader));
                uint64_t prev_string_offset = matched_row_idx == 0 ? 0 : offset_arr[matched_row_idx - 1];
                uint64_t string_offset = offset_arr[matched_row_idx];
                size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t) + prev_string_offset;
                strings[0] = StringVector::AddString(output.data[i], (char*)(io_requested_buf_ptrs[prev_toggle][i] + string_data_offset), string_offset - prev_string_offset);
            }
        } else if (ext_property_types[i] == LogicalType::ADJLIST) {
            // TODO we need to allocate buffer for adjlist
            // idx_t *adjListBase = (idx_t *)io_requested_buf_ptrs[prev_toggle][i];
            // size_t adj_list_end = adjListBase[STANDARD_VECTOR_SIZE - 1];
            // // output.InitializeAdjListColumn(i, adj_list_size);
            // // memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i], io_requested_buf_sizes[prev_toggle][i]);
            // memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i], STANDARD_VECTOR_SIZE * sizeof(idx_t));
            // VectorListBuffer &adj_list_buffer = (VectorListBuffer &)*output.data[i].GetAuxiliary();
            // for (idx_t adj_list_idx = STANDARD_VECTOR_SIZE; adj_list_idx < adj_list_end; adj_list_idx++) {
            //     adj_list_buffer.PushBack(Value::UBIGINT(adjListBase[adj_list_idx]));
            // }
            // // memcpy(output.data[i].GetAuxiliary()->GetData(), io_requested_buf_ptrs[prev_toggle][i] + STANDARD_VECTOR_SIZE * sizeof(idx_t), 
            // //        adj_list_size * sizeof(idx_t));
        } else if (ext_property_types[i] == LogicalType::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[i].GetData();
            id_column[0] = physical_id_base + matched_row_idx;
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_types[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] -  sizeof(CompressionHeader),
                                       output.data[i], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(ext_property_types[i].InternalType());
                memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader) + matched_row_idx * type_size, type_size);
            }
        }
    }
    // fprintf(stdout, "U\n");

    
    return true;
}

// For Seek Operator
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, idx_t target_seqno, bool is_output_chunk_initialized) {
    // We should avoid data copy here.. but copy for demo temporarliy
    
    // Keep previous values
    // fprintf(stdout, "Z\n");
    int prev_toggle = toggle;
    idx_t previous_idx = current_idx++;
    toggle = (toggle + 1) % num_data_chunks;
    if (current_idx > max_idx) return false;

    // fprintf(stdout, "K\n");
    // Request I/O to the next extent if we can support double buffering
    Catalog& cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx) {
        // fprintf(stdout, "Q\n");
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, "main", "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        // Unpin previous chunks
        if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[toggle].size() == 0);
        for (size_t i = 0; i < io_requested_cdf_ids[toggle].size(); i++)
            ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[toggle][i]);

        size_t chunk_size = ext_property_types.empty() ? extent_cat_entry->chunks.size() : ext_property_types.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);
        
        for (int i = 0; i < chunk_size; i++) {
            if (!ext_property_types.empty() && ext_property_types[i] == LogicalType::ID) continue;
            ChunkDefinitionID cdf_id = target_idxs.empty() ? 
                extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idxs[i]];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
    }

    // fprintf(stdout, "W\n");
    // Request chunk cache manager to finalize I/O
    for (int i = 0; i < io_requested_cdf_ids[prev_toggle].size(); i++)
        ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[prev_toggle][i], true, false);

    // Initialize DataChunk using cached buffer
    /*data_chunks[prev_toggle]->Destroy();
    data_chunks[prev_toggle]->Initialize(ext_property_types, io_requested_buf_ptrs[prev_toggle]);
    data_chunks[prev_toggle]->SetCardinality(io_requested_buf_sizes[prev_toggle][0] / GetTypeIdSize(ext_property_types[0].InternalType())); // XXX.. bug can occur
    output = data_chunks[prev_toggle];*/

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(ext_property_types);
    }
    CompressionHeader comp_header;
    // TODO record data cardinality in Chunk Definition?
    output.SetCardinality(1);
    output_eid = ext_ids_to_iterate[previous_idx];

    // fprintf(stdout, "T, size = %ld\n", ext_property_types.size());
    for (size_t i = 0; i < ext_property_types.size(); i++) {
        if (ext_property_types[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
            // fprintf(stdout, "Load Column %ld, cdf %ld, size = %ld %ld, io_req = %ld comp_type = %d, data_len = %ld, %p\n", 
            //                 i, io_requested_cdf_ids[prev_toggle][i], output.size(), comp_header.data_len, 
            //                 io_requested_buf_sizes[prev_toggle][i], (int)comp_header.comp_type, comp_header.data_len, io_requested_buf_ptrs[prev_toggle][i]);
        } else {
            // fprintf(stdout, "Load Column %ld\n", i);
        }
        if (ext_property_types[i] == LogicalType::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_types[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] -  sizeof(CompressionHeader),
                                       output.data[i], comp_header.data_len);
            } else {
                auto strings = FlatVector::GetData<string_t>(output.data[i]);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader));
                uint64_t prev_string_offset = target_seqno == 0 ? 0 : offset_arr[target_seqno - 1];
                uint64_t string_offset = offset_arr[target_seqno];
                size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t) + prev_string_offset;
                strings[0] = StringVector::AddString(output.data[i], (char*)(io_requested_buf_ptrs[prev_toggle][i] + string_data_offset), string_offset - prev_string_offset);
            }
        } else if (ext_property_types[i] == LogicalType::ADJLIST) {
            // TODO
            // idx_t *adjListBase = (idx_t *)io_requested_buf_ptrs[prev_toggle][i];
            // idx_t start_offset = target_seqno == 0 ? STANDARD_VECTOR_SIZE : adjListBase[target_seqno - 1];
            // idx_t end_offset = adjListBase[target_seqno];
            // size_t adj_list_size = end_offset - start_offset;
            // // output.InitializeAdjListColumn(i, adj_list_size);
            // memcpy(output.data[i].GetData(), &adj_list_size, sizeof(size_t));
            // VectorListBuffer &adj_list_buffer = (VectorListBuffer &)*output.data[i].GetAuxiliary();
            // for (idx_t adj_list_idx = start_offset; adj_list_idx < end_offset; adj_list_idx++) {
            //     adj_list_buffer.PushBack(Value::UBIGINT(adjListBase[adj_list_idx]));
            // }
            // memcpy(output.data[i].GetAuxiliary()->GetData(), adjListBase + start_offset, adj_list_size * sizeof(idx_t));
        } else if (ext_property_types[i] == LogicalType::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[i].GetData();
            id_column[0] = physical_id_base + target_seqno;
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_types[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader), io_requested_buf_sizes[prev_toggle][i] -  sizeof(CompressionHeader),
                                       output.data[i], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(ext_property_types[i].InternalType());
                memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader) + target_seqno * type_size, type_size);
            }
        }
    }
    // fprintf(stdout, "U\n");
    return true;
}

bool ExtentIterator::GetExtent(data_ptr_t &chunk_ptr) {
    D_ASSERT(ext_property_types[0] == LogicalType::ADJLIST); // Only for ADJLIIST now..
    // Keep previous values
    int prev_toggle = toggle;
    if (current_idx > max_idx) return false;

    // Request chunk cache manager to finalize I/O
    for (int i = 0; i < io_requested_cdf_ids[prev_toggle].size(); i++)
        ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[prev_toggle][i], true, false);

    CompressionHeader comp_header;
    
    D_ASSERT(ext_property_types.size() == 1);
    for (size_t i = 0; i < ext_property_types.size(); i++) {
        //memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
        // fprintf(stdout, "Load Column %ld, cdf %ld, size = %ld, io_req = %ld comp_type = %d, data_len = %ld, %p\n", 
        //                 i, io_requested_cdf_ids[prev_toggle][i], comp_header.data_len, 
        //                 io_requested_buf_sizes[prev_toggle][i], (int)comp_header.comp_type, comp_header.data_len, io_requested_buf_ptrs[prev_toggle][i]);
        chunk_ptr = (data_ptr_t)(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader));
    }
    return true;
}


bool ExtentIterator::_CheckIsMemoryEnough() {
    // TODO check memory.. if possible, use double buffering
    // Maybe this code is useless. Leave it to BFM
    bool enough = true;

    return enough;
}

void AdjacencyListIterator::Initialize(ClientContext &context, int adjColIdx, uint64_t vid) {
    ExtentID target_eid = vid >> 32;

    if (is_initialized && target_eid == cur_eid) return;

    vector<LogicalType> target_types { LogicalType::ADJLIST };
	vector<idx_t> target_idxs { (idx_t)adjColIdx };
    
    ext_it = new ExtentIterator();
    ext_it->Initialize(context, nullptr, target_types, target_idxs, target_eid);
}

void AdjacencyListIterator::getAdjListRange(uint64_t vid, uint64_t *start_idx, uint64_t *end_idx) {
    idx_t target_seqno = vid & 0x00000000FFFFFFFF;
    data_ptr_t adj_list;
    ext_it->GetExtent(adj_list);
    idx_t *adjListBase = (idx_t *)adj_list;
    *start_idx = target_seqno == 0 ? STANDARD_VECTOR_SIZE : adjListBase[target_seqno - 1];
    *end_idx = adjListBase[target_seqno];
}

void AdjacencyListIterator::getAdjListPtr(uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr) {
    idx_t target_seqno = vid & 0x00000000FFFFFFFF;
    data_ptr_t adj_list;
    ext_it->GetExtent(adj_list);
    idx_t *adjListBase = (idx_t *)adj_list;
    idx_t start_idx = target_seqno == 0 ? STANDARD_VECTOR_SIZE : adjListBase[target_seqno - 1];
    idx_t end_idx = adjListBase[target_seqno];
    start_ptr = adjListBase + start_idx;
    end_ptr = adjListBase + end_idx;
}

}