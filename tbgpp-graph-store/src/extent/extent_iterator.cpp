#include "extent/extent_iterator.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "cache/disk_aio/TypeDef.hpp"
#include "cache/chunk_cache_manager.h"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"

#include "icecream.hpp"

// #define DEBUG_LOAD_COLUMN

namespace duckdb {

// TODO: select extent to iterate using min & max & key
// Initialize iterator that iterates all extents
void ExtentIterator::Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry) {
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
    current_idx_in_this_extent = 0;
    max_idx = property_schema_cat_entry->extent_ids.size();
    ext_property_type = move(property_schema_cat_entry->GetTypesWithCopy());

    for (int i = 0; i < property_schema_cat_entry->extent_ids.size(); i++)
        ext_ids_to_iterate.push_back(property_schema_cat_entry->extent_ids[i]);

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        size_t chunk_size = extent_cat_entry->chunks.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        for (int i = 0; i < chunk_size; i++) {
            ChunkDefinitionID cdf_id = extent_cat_entry->chunks[i];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
        num_tuples_in_current_extent[toggle] = extent_cat_entry->GetNumTuplesInExtent();
    }
    is_initialized = true;
}

void ExtentIterator::Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_) {
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
    current_idx_in_this_extent = 0;
    max_idx = property_schema_cat_entry->extent_ids.size();
    ext_property_type = target_types_;
    target_idx = target_idxs_;
    for (size_t i = 0; i < property_schema_cat_entry->extent_ids.size(); i++)
        ext_ids_to_iterate.push_back(property_schema_cat_entry->extent_ids[i]);
    
    target_idxs_offset = 1;
    // for (int i = 0; i < ext_property_type.size(); i++) {
    //     if (ext_property_type[i] == LogicalType::ID) {
    //         target_idxs_offset = 1;
    //         break;
    //     }
    // }

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));

        size_t chunk_size = ext_property_type.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            // icecream::ic.enable(); IC(); IC(i, (int)ext_property_type[i].id()); icecream::ic.disable();
            if (ext_property_type[i] == LogicalType::ID) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                // icecream::ic.enable(); IC(); IC(i, io_requested_cdf_ids[toggle][i]); icecream::ic.disable();
                j++;
                continue;
            }
            if (target_idx[j] == std::numeric_limits<uint64_t>::max()) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                // icecream::ic.enable(); IC(); IC(i, io_requested_cdf_ids[toggle][i]); icecream::ic.disable();
                j++;
                continue;
            }
            ChunkDefinitionID cdf_id = extent_cat_entry->chunks[target_idx[j++] - target_idxs_offset]; // TODO bug..
            io_requested_cdf_ids[toggle][i] = cdf_id;
            // icecream::ic.enable(); IC(); IC(i, io_requested_cdf_ids[toggle][i]); icecream::ic.disable();
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
        num_tuples_in_current_extent[toggle] = extent_cat_entry->GetNumTuplesInExtent();
    }
    is_initialized = true;
}

void ExtentIterator::Initialize(ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid) {
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
    current_idx_in_this_extent = 0;
    max_idx = 1;
    ext_property_type = target_types_;
    target_idx = target_idxs_;
// icecream::ic.enable(); IC(); icecream::ic.disable();
    ext_ids_to_iterate.push_back(target_eid);

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        // icecream::ic.enable(); IC(); IC(target_eid); icecream::ic.disable();
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        size_t chunk_size = ext_property_type.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            ChunkDefinitionID cdf_id;
            if (ext_property_type[i] == LogicalType::ID) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                continue;
            } else if (ext_property_type[i] == LogicalType::FORWARD_ADJLIST || ext_property_type[i] == LogicalType::BACKWARD_ADJLIST) {
                cdf_id = extent_cat_entry->adjlist_chunks[target_idx[j++]];
            } else {
                cdf_id = extent_cat_entry->chunks[target_idx[j++]];
            }
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
        num_tuples_in_current_extent[toggle] = extent_cat_entry->GetNumTuplesInExtent();
    }
    is_initialized = true;
}

int
ExtentIterator::RequestNewIO(ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid, ExtentID &evicted_eid) {
    ext_ids_to_iterate.push_back(target_eid);

    int next_toggle = (toggle + 1) % num_data_chunks;
    idx_t previous_idx = current_idx;
    current_idx++;
    max_idx++;

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the new extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
            if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
        }

        size_t chunk_size = ext_property_type.size();
        io_requested_cdf_ids[next_toggle].resize(chunk_size);
        io_requested_buf_ptrs[next_toggle].resize(chunk_size);
        io_requested_buf_sizes[next_toggle].resize(chunk_size);

        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            ChunkDefinitionID cdf_id;
            if (ext_property_type[i] == LogicalType::ID) {
                io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                continue;
            } else if (ext_property_type[i] == LogicalType::FORWARD_ADJLIST || ext_property_type[i] == LogicalType::BACKWARD_ADJLIST) {
                cdf_id = extent_cat_entry->adjlist_chunks[target_idx[j++]];
            } else {
                cdf_id = extent_cat_entry->chunks[target_idx[j++]];
            }
            io_requested_cdf_ids[next_toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
        }
        num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
    }
    is_initialized = true;
    evicted_eid = previous_idx == 0 ? std::numeric_limits<ExtentID>::max() : ext_ids_to_iterate[previous_idx - 1];
    toggle++;
    return next_toggle;
}

// Initialize For Seek
void 
ExtentIterator::Initialize(ClientContext &context, vector<vector<LogicalType>> &target_types_, vector<vector<idx_t>> &target_idxs_, 
                           vector<idx_t> &target_idx_per_eid_, vector<ExtentID> target_eids) {
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
    current_idx_in_this_extent = 0;
    max_idx = target_eids.size();
    ext_property_types = target_types_;
    target_idxs = target_idxs_; // TODO avoid copy?
    target_idx_per_eid = target_idx_per_eid_;

    for (size_t i = 0; i < target_eids.size(); i++)
        ext_ids_to_iterate.push_back(target_eids[i]);

    target_idxs_offset = 1;

    Catalog& cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        size_t chunk_size = ext_property_types[current_idx].size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);

        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            if (ext_property_types[current_idx][i] == LogicalType::ID) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                j++;
                continue;
            }
            if (target_idxs[target_idx_per_eid[current_idx]][j] == std::numeric_limits<uint64_t>::max()) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                j++;
                continue;
            }
            ChunkDefinitionID cdf_id = extent_cat_entry->chunks[target_idxs[target_idx_per_eid[current_idx]][j++] - target_idxs_offset];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id); // TODO wrong path
            // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
        num_tuples_in_current_extent[toggle] = extent_cat_entry->GetNumTuplesInExtent();
    }
    is_initialized = true;
}

// Get Next Extent with all properties. For full scan (w/o output_column_idxs)
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, size_t scan_size, bool is_output_chunk_initialized) {
    // We should avoid data copy here.. but copy for demo temporarliy
    // Keep previous values
    // icecream::ic.enable(); IC(); IC(current_idx, max_idx, current_idx_in_this_extent, scan_size); icecream::ic.disable();
    if (current_idx_in_this_extent == (STORAGE_STANDARD_VECTOR_SIZE / scan_size)) {
        current_idx++;
        current_idx_in_this_extent = 0;
    }
    if (current_idx > max_idx) return false;

    // Request I/O to the next extent if we can support double buffering
    // IC(prev_toggle, toggle, next_toggle, current_idx, current_idx_in_this_extent, max_idx, scan_size);
    Catalog& cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx && current_idx_in_this_extent == 0) {
        if (current_idx != 0) toggle = (toggle + 1) % num_data_chunks;
        int next_toggle = (toggle + 1) % num_data_chunks;
        if (current_idx < max_idx - 1) {
            ExtentCatalogEntry* extent_cat_entry = 
                (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx + 1]));
            
            // Unpin previous chunks
            for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
                if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
                ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
            }

            size_t chunk_size = ext_property_type.empty() ? extent_cat_entry->chunks.size() : ext_property_type.size();
            io_requested_cdf_ids[next_toggle].resize(chunk_size);
            io_requested_buf_ptrs[next_toggle].resize(chunk_size);
            io_requested_buf_sizes[next_toggle].resize(chunk_size);
            
            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                if (!ext_property_type.empty() && ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                if (!target_idx.empty() && (target_idx[j] == std::numeric_limits<uint64_t>::max())) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id = target_idx.empty() ? 
                    extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idx[j++] - target_idxs_offset];
                io_requested_cdf_ids[next_toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
                // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
            }
            num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
        }
    }

    // Request chunk cache manager to finalize I/O
    if (current_idx_in_this_extent == 0) {
        for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i], true, false);
            // icecream::ic.enable(); IC(); IC(io_requested_cdf_ids[toggle][i], toggle, i); icecream::ic.disable();
        }
    }

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(ext_property_type);
    }
    CompressionHeader comp_header;
    
    if (num_tuples_in_current_extent[toggle] < (current_idx_in_this_extent * scan_size)) return false;
    size_t remain_data_size = num_tuples_in_current_extent[toggle] - (current_idx_in_this_extent * scan_size);
    size_t output_cardinality = std::min((size_t) scan_size, remain_data_size);
    output.SetCardinality(output_cardinality);
    output_eid = ext_ids_to_iterate[current_idx];
    idx_t scan_begin_offset = current_idx_in_this_extent * scan_size;
    idx_t scan_end_offset = std::min((current_idx_in_this_extent + 1) * scan_size, num_tuples_in_current_extent[toggle]);
    // D_ASSERT(comp_header.data_len <= STORAGE_STANDARD_VECTOR_SIZE);
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        if ((ext_property_type[i] != LogicalType::ID) && (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max())) {
            FlatVector::Validity(output.data[i]).SetAllInvalid(output.size());
            // FlatVector::Validity(output.data[i]).SetAllInvalid(output.size());
            continue;
        }
        if (ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i], sizeof(CompressionHeader));
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Full Scan] Load Column %ld -> %ld, cdf %ld, type %d, scan_size = %ld %ld (from %ld to %ld), total_size = %ld, io_req_buf_size = %ld comp_type = %d, data_len = %ld, %p -> %p\n", 
                            i, i, io_requested_cdf_ids[toggle][i], (int)ext_property_type[i].id(), output.size(), scan_size,
                            scan_begin_offset, scan_end_offset, comp_header.data_len,
                            io_requested_buf_sizes[toggle][i], (int)comp_header.comp_type, comp_header.data_len,
                            io_requested_buf_ptrs[toggle][i], output.data[i].GetData());
#endif
        } else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Full Scan] Load Column %ld -> %ld\n", i, i);
#endif
        }
        auto comp_header_valid_size = sizeof(CompressionHeader);
        if (ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] - comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                // FlatVector::SetData(output.data[i], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + sizeof(string_t) * scan_begin_offset);
                auto strings = FlatVector::GetData<string_t>(output.data[i]);
                size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[toggle][i] + sizeof(CompressionHeader));
                uint64_t string_offset, prev_string_offset;
                size_t output_idx = 0;
                for (size_t input_idx = scan_begin_offset; input_idx < scan_end_offset; input_idx++) {
                    prev_string_offset = input_idx == 0 ? 0 : offset_arr[input_idx - 1];
                    string_offset = offset_arr[input_idx];                    
                    strings[output_idx] = StringVector::AddString(output.data[i], (const char*)(io_requested_buf_ptrs[toggle][i] + string_data_offset + prev_string_offset), string_offset - prev_string_offset);
                    output_idx++;
                }
            }
        } else if (ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t data_array_offset = comp_header.data_len * type_size;

            // ListVector::SetData(output.data[i], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + scan_begin_offset * type_size);
            // ListVector::SetChildData(output.data[i], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + data_array_offset);
        } else if (ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST || ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
        } else if (ext_property_type[i].id() == LogicalTypeId::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[i].GetData();
            idx_t output_seqno = 0;
            for (size_t seqno = scan_begin_offset; seqno < scan_end_offset; seqno++)
                id_column[output_seqno++] = physical_id_base + seqno;
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(ext_property_type[i].InternalType());
                // FlatVector::SetData(output.data[i], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + scan_begin_offset * type_size);
                memcpy(output.data[i].GetData(), io_requested_buf_ptrs[toggle][i] + sizeof(CompressionHeader) + scan_begin_offset * type_size, (scan_end_offset - scan_begin_offset) * type_size);
            }
        }
    }
    
    current_idx_in_this_extent++;
    return true;
}

// Get Next Extent with all properties. For full scan
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, vector<idx_t> &output_column_idxs, size_t scan_size, bool is_output_chunk_initialized) {
    // We should avoid data copy here.. but copy for demo temporarliy
    // Keep previous values
    // icecream::ic.enable(); IC(); IC(current_idx, max_idx, current_idx_in_this_extent, scan_size); icecream::ic.disable();
    if (current_idx_in_this_extent == (STORAGE_STANDARD_VECTOR_SIZE / scan_size)) {
        current_idx++;
        current_idx_in_this_extent = 0;
    }
    if (current_idx > max_idx) return false;

    // Request I/O to the next extent if we can support double buffering
    // IC(prev_toggle, toggle, next_toggle, current_idx, current_idx_in_this_extent, max_idx, scan_size);
    Catalog& cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx && current_idx_in_this_extent == 0) {
        if (current_idx != 0) toggle = (toggle + 1) % num_data_chunks;
        int next_toggle = (toggle + 1) % num_data_chunks;
        if (current_idx < max_idx - 1) {
            ExtentCatalogEntry* extent_cat_entry = 
                (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx + 1]));
            
            // Unpin previous chunks
            for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
                if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
                ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
            }

            size_t chunk_size = ext_property_type.empty() ? extent_cat_entry->chunks.size() : ext_property_type.size();
            io_requested_cdf_ids[next_toggle].resize(chunk_size);
            io_requested_buf_ptrs[next_toggle].resize(chunk_size);
            io_requested_buf_sizes[next_toggle].resize(chunk_size);
            
            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                if (!ext_property_type.empty() && ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                if (!target_idx.empty() && (target_idx[j] == std::numeric_limits<uint64_t>::max())) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id = target_idx.empty() ? 
                    extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idx[j++] - target_idxs_offset];
                io_requested_cdf_ids[next_toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
                // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
            }
            num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
        }
    }
// IC();

    // Request chunk cache manager to finalize I/O
    if (current_idx_in_this_extent == 0) {
        for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i], true, false);
            // icecream::ic.enable(); IC(); IC(io_requested_cdf_ids[toggle][i], toggle, i); icecream::ic.disable();
        }
    }

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(ext_property_type);
    }
    CompressionHeader comp_header;
    
    if (num_tuples_in_current_extent[toggle] < (current_idx_in_this_extent * scan_size)) return false;
    size_t remain_data_size = num_tuples_in_current_extent[toggle] - (current_idx_in_this_extent * scan_size);
    size_t output_cardinality = std::min((size_t) scan_size, remain_data_size);
    output.SetCardinality(output_cardinality);
    output_eid = ext_ids_to_iterate[current_idx];
// IC();
    idx_t scan_begin_offset = current_idx_in_this_extent * scan_size;
    idx_t scan_end_offset = std::min((current_idx_in_this_extent + 1) * scan_size, num_tuples_in_current_extent[toggle]);
// icecream::ic.enable();IC(output_eid, current_idx_in_this_extent, scan_size, scan_begin_offset, scan_end_offset, idx_for_cardinality, comp_header.data_len);icecream::ic.disable();
    // D_ASSERT(comp_header.data_len <= STORAGE_STANDARD_VECTOR_SIZE);
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        if ((ext_property_type[i] != LogicalType::ID) && (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max())) {
            FlatVector::Validity(output.data[output_column_idxs[i]]).SetAllInvalid(output.size());
            // FlatVector::Validity(output.data[i]).SetAllInvalid(output.size());
            continue;
        }
        if (ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i], sizeof(CompressionHeader));
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Full Scan] Load Column %ld -> %ld, cdf %ld, type %d, scan_size = %ld %ld (from %ld to %ld), total_size = %ld, io_req_buf_size = %ld comp_type = %d, data_len = %ld, %p -> %p\n", 
                            i, output_column_idxs[i], io_requested_cdf_ids[toggle][i], (int)ext_property_type[i].id(), output.size(), scan_size,
                            scan_begin_offset, scan_end_offset, comp_header.data_len,
                            io_requested_buf_sizes[toggle][i], (int)comp_header.comp_type, comp_header.data_len,
                            // io_requested_buf_ptrs[toggle][i], output.data[i].GetData());
                            io_requested_buf_ptrs[toggle][i], output.data[output_column_idxs[i]].GetData());
#endif
        } else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Full Scan] Load Column %ld -> %ld\n", i, output_column_idxs[i]);
#endif
        }
        auto comp_header_valid_size = sizeof(CompressionHeader);
        if (ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] - comp_header_valid_size,
                                    //    output.data[i], comp_header.data_len);
                                       output.data[output_column_idxs[i]], comp_header.data_len);
            } else {
                // FlatVector::SetData(output.data[i], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + sizeof(string_t) * scan_begin_offset);
                // auto strings = FlatVector::GetData<string_t>(output.data[i]);
                auto strings = FlatVector::GetData<string_t>(output.data[output_column_idxs[i]]);
                size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[toggle][i] + sizeof(CompressionHeader));
                uint64_t string_offset, prev_string_offset;
                size_t output_idx = 0;
                for (size_t input_idx = scan_begin_offset; input_idx < scan_end_offset; input_idx++) {
                    prev_string_offset = input_idx == 0 ? 0 : offset_arr[input_idx - 1];
                    string_offset = offset_arr[input_idx];                    
                    // strings[output_idx] = StringVector::AddString(output.data[i], (const char*)(io_requested_buf_ptrs[toggle][i] + string_data_offset + prev_string_offset), string_offset - prev_string_offset);
                    strings[output_idx] = StringVector::AddString(output.data[output_column_idxs[i]], (const char*)(io_requested_buf_ptrs[toggle][i] + string_data_offset + prev_string_offset), string_offset - prev_string_offset);
                    output_idx++;
                }
            }
        } else if (ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t data_array_offset = comp_header.data_len * type_size;

            // ListVector::SetData(output.data[output_column_idxs[i]], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + scan_begin_offset * type_size);
            // ListVector::SetChildData(output.data[output_column_idxs[i]], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + data_array_offset);
        } else if (ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST || ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
        } else if (ext_property_type[i].id() == LogicalTypeId::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[output_column_idxs[i]].GetData();
            // idx_t *id_column = (idx_t *)output.data[i].GetData();
            idx_t output_seqno = 0;
            for (size_t seqno = scan_begin_offset; seqno < scan_end_offset; seqno++)
                id_column[output_seqno++] = physical_id_base + seqno;
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[output_column_idxs[i]], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(ext_property_type[i].InternalType());
                // FlatVector::SetData(output.data[i], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + scan_begin_offset * type_size);
                // FlatVector::SetData(output.data[output_column_idxs[i]], io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + scan_begin_offset * type_size);
                memcpy(output.data[i].GetData(), io_requested_buf_ptrs[toggle][i] + sizeof(CompressionHeader) + scan_begin_offset * type_size, (scan_end_offset - scan_begin_offset) * type_size);
            }
        }
    }
// IC();    
    
    current_idx_in_this_extent++;
    return true;
}

// Get Next Extent with filterKey
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, 
                                   int64_t &filterKeyColIdx, duckdb::Value &filterValue, vector<idx_t> &output_column_idxs, 
                                   std::vector<duckdb::LogicalType> &scanSchema, bool is_output_chunk_initialized) {
    // TODO we assume that there is only one tuple that matches the predicate
    // We should avoid data copy here.. but copy for demo temporarliy
    
    // Keep previous values
    int prev_toggle = toggle;
    idx_t previous_idx = current_idx++;
    if (current_idx > max_idx) return false;
// icecream::ic.enable(); IC(); icecream::ic.disable();
    // Request I/O to the next extent if we can support double buffering
    Catalog& cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx) {
        toggle = (toggle + 1) % num_data_chunks;
        ExtentCatalogEntry* extent_cat_entry = 
            (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
        
        // Unpin previous chunks
        if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[toggle].size() == 0);
        for (size_t i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[toggle][i]);
        }

        size_t chunk_size = ext_property_type.empty() ? extent_cat_entry->chunks.size() : ext_property_type.size();
        io_requested_cdf_ids[toggle].resize(chunk_size);
        io_requested_buf_ptrs[toggle].resize(chunk_size);
        io_requested_buf_sizes[toggle].resize(chunk_size);
        
        int j = 0;
        for (int i = 0; i < chunk_size; i++) {
            if (!ext_property_type.empty() && ext_property_type[i] == LogicalType::ID) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                j++;
                continue;
            }
            if (!target_idx.empty() && (target_idx[j] == std::numeric_limits<uint64_t>::max())) {
                io_requested_cdf_ids[toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                j++;
                continue;
            }
            ChunkDefinitionID cdf_id = target_idx.empty() ? 
                extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idx[j++] - target_idxs_offset];
            io_requested_cdf_ids[toggle][i] = cdf_id;
            string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
            // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
            ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[toggle][i], &io_requested_buf_sizes[toggle][i], true);
        }
        num_tuples_in_current_extent[toggle] = extent_cat_entry->GetNumTuplesInExtent();
    }

    // Request chunk cache manager to finalize I/O
    for (int i = 0; i < io_requested_cdf_ids[prev_toggle].size(); i++) {
        if (io_requested_cdf_ids[prev_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
        ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[prev_toggle][i], true, false);
    }

    output_eid = ext_ids_to_iterate[previous_idx];
    ChunkDefinitionID filter_cdf_id = (ChunkDefinitionID) output_eid;
    filter_cdf_id = filter_cdf_id << 32;
    filter_cdf_id = filter_cdf_id + filterKeyColIdx - target_idxs_offset;

    vector<bool> valid_output;
    valid_output.resize(target_idx.size());
    idx_t output_idx = 0;
    for (idx_t i = 0; i < target_idx.size(); i++) {
        if (output_column_idxs.size() == 0) {
            valid_output[i] = false;
            continue;
        } else {
            if (output_column_idxs[output_idx] == target_idx[i]) {
                valid_output[i] = true;
                output_idx++;
            } else {
                valid_output[i] = false;
            }
        }
    }

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(scanSchema);
    }

    idx_t scan_start_offset, scan_end_offset, scan_length;
    ChunkDefinitionCatalogEntry* cdf_cat_entry = 
            (ChunkDefinitionCatalogEntry*) cat_instance.GetEntry(context, CatalogType::CHUNKDEFINITION_ENTRY, DEFAULT_SCHEMA, "cdf_" + std::to_string(filter_cdf_id));

    // TODO move this logic to InitializeScan (We don't need to do I/O in this case)
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
    auto col_idx_find_result = std::find(io_requested_cdf_ids[prev_toggle].begin(), io_requested_cdf_ids[prev_toggle].end(), filter_cdf_id);
    if (col_idx_find_result == io_requested_cdf_ids[prev_toggle].end()) throw InvalidInputException("I/O Error");
    idx_t col_idx = col_idx_find_result - io_requested_cdf_ids[prev_toggle].begin();

    // Get Compression Header
    CompressionHeader comp_header;

    // Find the index of a row that matches a predicate
    bool find_matched_row = false;
    idx_t matched_row_idx;
    if (ext_property_type[col_idx] == LogicalType::VARCHAR) {
        memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][col_idx], sizeof(CompressionHeader));
        if (comp_header.comp_type == DICTIONARY) {
            throw NotImplementedException("Filter predicate on DICTIONARY compression is not implemented yet");
        } else {
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
    } else if (ext_property_type[col_idx] == LogicalType::FORWARD_ADJLIST || ext_property_type[col_idx] == LogicalType::BACKWARD_ADJLIST) {
        throw InvalidInputException("Filter predicate on ADJLIST column");
    } else if (ext_property_type[col_idx] == LogicalType::ID) {
        throw InvalidInputException("Filter predicate on PID column");
    } else {
        memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][col_idx], sizeof(CompressionHeader));
        if (comp_header.comp_type == BITPACKING) {
            throw NotImplementedException("Filter predicate on BITPACKING compression is not implemented yet");
        } else {
            LogicalType column_type = ext_property_type[col_idx];
            Vector column_vec(column_type, (data_ptr_t)(io_requested_buf_ptrs[prev_toggle][col_idx] + sizeof(CompressionHeader)));
            for (idx_t i = scan_start_offset; i < scan_end_offset; i++) {
                // icecream::ic.enable(); IC(); IC(column_vec.GetValue(i).GetValue<idx_t>(), filterValue.GetValue<idx_t>()); icecream::ic.disable();
                if (column_vec.GetValue(i) == filterValue) {
                    matched_row_idx = i;
                    find_matched_row = true;
                    break;
                }
            }
        }
    }

    if (find_matched_row) {
        output.SetCardinality(1); // TODO 1 -> matched tuple count
    } else {
        output.SetCardinality(0);
        return true;
    }
    
    output_idx = 0;
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        if (!valid_output[i]) continue;
        if (ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[prev_toggle][i], sizeof(CompressionHeader));
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Scan With Filter] Load Column %ld -> %ld, cdf %ld, size = %ld %ld, io_req_buf_size = %ld comp_type = %d, data_len = %ld, %p\n", 
                            i, output_idx, io_requested_cdf_ids[prev_toggle][i], output.size(), comp_header.data_len, 
                            io_requested_buf_sizes[prev_toggle][i], (int)comp_header.comp_type, comp_header.data_len, io_requested_buf_ptrs[prev_toggle][i]);
#endif
        } else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Scan With Filter] Load Column %ld\n", i);
#endif
        }
        auto comp_header_valid_size = sizeof(CompressionHeader);
        if (ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + comp_header_valid_size, io_requested_buf_sizes[prev_toggle][i] -  comp_header_valid_size,
                                       output.data[output_idx], comp_header.data_len);
            } else {
                // FlatVector::SetData(output.data[output_idx], io_requested_buf_ptrs[prev_toggle][i] + comp_header_valid_size + sizeof(string_t) * matched_row_idx);
                auto strings = FlatVector::GetData<string_t>(output.data[output_idx]);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader));
                uint64_t prev_string_offset = matched_row_idx == 0 ? 0 : offset_arr[matched_row_idx - 1];
                uint64_t string_offset = offset_arr[matched_row_idx];
                size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t) + prev_string_offset;
                strings[0] = StringVector::AddString(output.data[output_idx], (char*)(io_requested_buf_ptrs[prev_toggle][i] + string_data_offset), string_offset - prev_string_offset);
            }
        } else if (ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t data_array_offset = comp_header.data_len * type_size;

            ListVector::SetData(output.data[output_idx], io_requested_buf_ptrs[prev_toggle][i] + comp_header_valid_size + matched_row_idx * type_size);
            ListVector::SetChildData(output.data[output_idx], io_requested_buf_ptrs[prev_toggle][i] + comp_header_valid_size + data_array_offset);
        } else if (ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST || ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
            // TODO we need to allocate buffer for adjlist
            // idx_t *adjListBase = (idx_t *)io_requested_buf_ptrs[prev_toggle][i];
            // size_t adj_list_end = adjListBase[STORAGE_STANDARD_VECTOR_SIZE - 1];
            // // output.InitializeAdjListColumn(i, adj_list_size);
            // // memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i], io_requested_buf_sizes[prev_toggle][i]);
            // memcpy(output.data[i].GetData(), io_requested_buf_ptrs[prev_toggle][i], STORAGE_STANDARD_VECTOR_SIZE * sizeof(idx_t));
            // VectorListBuffer &adj_list_buffer = (VectorListBuffer &)*output.data[i].GetAuxiliary();
            // for (idx_t adj_list_idx = STORAGE_STANDARD_VECTOR_SIZE; adj_list_idx < adj_list_end; adj_list_idx++) {
            //     adj_list_buffer.PushBack(Value::UBIGINT(adjListBase[adj_list_idx]));
            // }
            // // memcpy(output.data[i].GetAuxiliary()->GetData(), io_requested_buf_ptrs[prev_toggle][i] + STORAGE_STANDARD_VECTOR_SIZE * sizeof(idx_t), 
            // //        adj_list_size * sizeof(idx_t));
        } else if (ext_property_type[i].id() == LogicalTypeId::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[output_idx].GetData();
            id_column[0] = physical_id_base + matched_row_idx;
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[prev_toggle][i] + comp_header_valid_size, io_requested_buf_sizes[prev_toggle][i] -  comp_header_valid_size,
                                       output.data[output_idx], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(ext_property_type[i].InternalType());
                // FlatVector::SetData(output.data[output_idx], io_requested_buf_ptrs[prev_toggle][i] + comp_header_valid_size + matched_row_idx * type_size);
                memcpy(output.data[output_idx].GetData(), io_requested_buf_ptrs[prev_toggle][i] + sizeof(CompressionHeader) + matched_row_idx * type_size, type_size);
            }
        }
        output_idx++;
    }
        
    return true;
}

// For Seek Operator
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, ExtentID target_eid, idx_t target_seqno, bool is_output_chunk_initialized) {
    D_ASSERT(false);
    return true;
}

// For Seek Operator - Bulk Mode
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, 
                                   ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, vector<idx_t> &output_column_idxs,
                                   idx_t start_seqno, idx_t end_seqno, bool is_output_chunk_initialized) {
    int prev_toggle = toggle;
    // icecream::ic.enable(); IC(); IC(target_eid, current_eid); icecream::ic.disable();
    if (target_eid != current_eid) {
        // Keep previous values
        if (current_eid != std::numeric_limits<uint32_t>::max())
           toggle = (toggle + 1) % num_data_chunks;
        int next_toggle = (toggle + 1) % num_data_chunks;
        idx_t previous_idx = current_idx++;
        if (current_idx > max_idx) return false;

        // Request I/O to the next extent if we can support double buffering
        Catalog& cat_instance = context.db->GetCatalog();
        if (support_double_buffering && current_idx < max_idx) {
            // IC(toggle, prev_toggle, target_eid, current_eid, current_idx, ext_ids_to_iterate[current_idx]);
            if (current_idx < 2 || (ext_ids_to_iterate[current_idx] != ext_ids_to_iterate[current_idx - 2])) {
                ExtentCatalogEntry* extent_cat_entry = 
                    (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
                
                // Unpin previous chunks
                if (current_eid != std::numeric_limits<uint32_t>::max()) {
                    if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[next_toggle].size() == 0);
                    for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
                        if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
                        ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
                    }
                }

                size_t chunk_size = ext_property_type.empty() ? extent_cat_entry->chunks.size() : ext_property_type.size();
                io_requested_cdf_ids[next_toggle].resize(chunk_size);
                io_requested_buf_ptrs[next_toggle].resize(chunk_size);
                io_requested_buf_sizes[next_toggle].resize(chunk_size);
                
                int j = 0;
                for (int i = 0; i < chunk_size; i++) {
                    if (!ext_property_type.empty() && ext_property_type[i] == LogicalType::ID) {
                        io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                        j++;
                        continue;
                    }
                    if (!target_idxs[target_idx_per_eid[current_idx]].empty() && 
                        (target_idxs[target_idx_per_eid[current_idx]][j] == std::numeric_limits<uint64_t>::max())) {
                        io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                        j++;
                        continue;
                    }
                    ChunkDefinitionID cdf_id = target_idxs[target_idx_per_eid[current_idx]].empty() ? 
                        extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idxs[target_idx_per_eid[current_idx]][j++] - target_idxs_offset];
                    io_requested_cdf_ids[next_toggle][i] = cdf_id;
                    string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
                    // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                    ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
                }
                num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
            }
        }

        // Request chunk cache manager to finalize I/O
        for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i], true, false);
        }
        current_eid = ext_ids_to_iterate[previous_idx];

        // Initialize DataChunk using cached buffer
        /*data_chunks[prev_toggle]->Destroy();
        data_chunks[prev_toggle]->Initialize(ext_property_type, io_requested_buf_ptrs[prev_toggle]);
        data_chunks[prev_toggle]->SetCardinality(io_requested_buf_sizes[prev_toggle][0] / GetTypeIdSize(ext_property_type[0].InternalType())); // XXX.. bug can occur
        output = data_chunks[prev_toggle];*/

        // Initialize output DataChunk & copy each column
        if (!is_output_chunk_initialized) {
            // icecream::ic.enable();IC();icecream::ic.disable();
            output.Reset();
            // icecream::ic.enable();IC();icecream::ic.disable();
            output.Initialize(ext_property_type);
        }
        
        output_eid = ext_ids_to_iterate[previous_idx];
    } else {
        output_eid = current_eid;
    }

    CompressionHeader comp_header;
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        if (ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i], sizeof(CompressionHeader));
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk] Load Column %ld -> %ld, cdf %ld, size = %ld %ld, io_req = %ld comp_type = %d -> %d, data_len = %ld, %p -> %p\n", 
                            i, output_column_idxs[i], io_requested_cdf_ids[toggle][i], output.size(), comp_header.data_len, 
                            io_requested_buf_sizes[toggle][i], (int)comp_header.comp_type, (int) ext_property_type[i].id(), comp_header.data_len,
                            io_requested_buf_ptrs[toggle][i], output.data[i].GetData());
#endif
        } else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk] Load Column %ld\n", i);
#endif
        }
        auto comp_header_valid_size = sizeof(CompressionHeader);
        if (ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                auto strings = FlatVector::GetData<string_t>(output.data[output_column_idxs[i]]);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size);
                Vector &vids = input.data[nodeColIdx];
                for (idx_t seqno = start_seqno; seqno <= end_seqno; seqno++) {
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    uint64_t prev_string_offset = target_seqno == 0 ? 0 : offset_arr[target_seqno - 1];
                    uint64_t string_offset = offset_arr[target_seqno];
                    size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t) + prev_string_offset;
                    strings[seqno] = StringVector::AddString(output.data[output_column_idxs[i]], (char*)(io_requested_buf_ptrs[toggle][i] + string_data_offset), string_offset - prev_string_offset);
                    // strings[seqno] = *((string_t*)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + target_seqno * sizeof(string_t)));
                }
            }
        } else if (ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t offset_array_size = comp_header.data_len * type_size;

            D_ASSERT(false); /* zero copy for LIST is not implemented in this function */

            Vector &vids = input.data[nodeColIdx];
            size_t list_offset = 0;
            size_t list_size_to_append = 0;
            size_t child_type_size = GetTypeIdSize(ListType::GetChildType(ext_property_type[i]).InternalType());
            list_entry_t *list_vec = (list_entry_t *)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size);
            list_entry_t *output_list_vec = (list_entry_t *)output.data[output_column_idxs[i]].GetData();
            for (idx_t seqno = start_seqno; seqno <= end_seqno; seqno++) {
                idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                output_list_vec[seqno].length = list_vec[target_seqno].length;
                list_size_to_append += (list_vec[target_seqno].length * child_type_size);
            }

            Vector &child_vec = ListVector::GetEntry(output.data[output_column_idxs[i]]);
            size_t last_offset = start_seqno == 0 ? 0 : output_list_vec[start_seqno - 1].offset + output_list_vec[start_seqno - 1].length;
            ListVector::Reserve(output.data[output_column_idxs[i]], last_offset + list_size_to_append);
            for (idx_t seqno = start_seqno; seqno <= end_seqno; seqno++) {
                idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                icecream::ic.enable(); IC(); IC(target_seqno, last_offset, start_seqno, seqno, end_seqno, offset_array_size, list_vec[target_seqno].offset, list_vec[target_seqno].length, child_type_size); icecream::ic.disable();
                memcpy(child_vec.GetData() + last_offset * child_type_size, io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + offset_array_size + list_vec[target_seqno].offset * child_type_size, list_vec[target_seqno].length * child_type_size);
                output_list_vec[seqno].offset = last_offset;
                last_offset += list_vec[target_seqno].length;
            }
        } else if (ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST || ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
            // TODO
        } else if (ext_property_type[i].id() == LogicalTypeId::ID) {
            // throw InvalidInputException("Never occur");
            Vector &vids = input.data[nodeColIdx];
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[output_column_idxs[i]].GetData();
            idx_t output_seqno = 0;
            for (size_t seqno = start_seqno; seqno <= end_seqno; seqno++) {
                idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                id_column[seqno] = physical_id_base + target_seqno;
                D_ASSERT(id_column[seqno] == getIdRefFromVectorTemp(vids, seqno));
            }
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(ext_property_type[i].InternalType());
                Vector &vids = input.data[nodeColIdx];
                for (idx_t seqno = start_seqno; seqno <= end_seqno; seqno++) {
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    // icecream::ic.enable(); IC(); IC(start_seqno, seqno, end_seqno, vids[seqno], target_seqno); icecream::ic.disable();
                    memcpy(output.data[output_column_idxs[i]].GetData() + seqno * type_size, io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + target_seqno * type_size, type_size);
                }
                
            }
        }
    }
// icecream::ic.enable(); IC(); icecream::ic.disable();
    return true;
}

// For Seek Operator - Bulk Mode + Target Seqnos
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, 
                                   ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, vector<idx_t> &output_column_idxs,
                                   vector<idx_t> &target_seqnos, bool is_output_chunk_initialized) {
    int prev_toggle = toggle;
    auto &cur_ext_property_type = ext_property_types[target_idx_per_eid[current_idx]];
    if (target_eid != current_eid) {
        // Keep previous values
        idx_t previous_idx, next_idx;
        if (current_eid != std::numeric_limits<uint32_t>::max()) {
            toggle = (toggle + 1) % num_data_chunks;
            previous_idx = current_idx++;
            next_idx = current_idx + 1;
        } else {
            // first time here
            previous_idx = current_idx;
            next_idx = current_idx + 1;
        }
        int next_toggle = (toggle + 1) % num_data_chunks;
        int prev_toggle = (toggle - 1 + num_data_chunks) % num_data_chunks;
        if (current_idx > max_idx) return false;
        cur_ext_property_type = ext_property_types[target_idx_per_eid[current_idx]];

        // Request I/O to the next extent if we can support double buffering
        Catalog& cat_instance = context.db->GetCatalog();
        if (support_double_buffering && next_idx < max_idx) {
            ExtentCatalogEntry* extent_cat_entry = 
                (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[next_idx]));
            
            // Unpin previous chunks
            if (current_eid != std::numeric_limits<uint32_t>::max()) {
                if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[next_toggle].size() == 0);
                for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
                    if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
                    ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
                }
            }

            auto &next_ext_property_type = ext_property_types[target_idx_per_eid[next_idx]];
            size_t chunk_size = next_ext_property_type.empty() ? extent_cat_entry->chunks.size() : next_ext_property_type.size();
            io_requested_cdf_ids[next_toggle].resize(chunk_size);
            io_requested_buf_ptrs[next_toggle].resize(chunk_size);
            io_requested_buf_sizes[next_toggle].resize(chunk_size);
            
            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                if (!next_ext_property_type.empty() && next_ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                if (!target_idxs[target_idx_per_eid[next_idx]].empty() && 
                    (target_idxs[target_idx_per_eid[next_idx]][j] == std::numeric_limits<uint64_t>::max())) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id = target_idxs[target_idx_per_eid[next_idx]].empty() ? 
                    extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idxs[target_idx_per_eid[next_idx]][j++] - target_idxs_offset];
                io_requested_cdf_ids[next_toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
                ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
            }
            num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
        }

        // Request chunk cache manager to finalize I/O
        for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i], true, false);
        }
        current_eid = ext_ids_to_iterate[current_idx];

        // Initialize DataChunk using cached buffer
        /*data_chunks[prev_toggle]->Destroy();
        data_chunks[prev_toggle]->Initialize(ext_property_type, io_requested_buf_ptrs[prev_toggle]);
        data_chunks[prev_toggle]->SetCardinality(io_requested_buf_sizes[prev_toggle][0] / GetTypeIdSize(ext_property_type[0].InternalType())); // XXX.. bug can occur
        output = data_chunks[prev_toggle];*/

        // Initialize output DataChunk & copy each column
        if (!is_output_chunk_initialized) {
            // icecream::ic.enable();IC();icecream::ic.disable();
            output.Reset();
            // icecream::ic.enable();IC();icecream::ic.disable();
            output.Initialize(cur_ext_property_type);
        }
        
        output_eid = ext_ids_to_iterate[current_idx];
    } else {
        output_eid = current_eid;
    }

    CompressionHeader comp_header;
    for (size_t i = 0; i < cur_ext_property_type.size(); i++) {
        if (cur_ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i], sizeof(CompressionHeader));
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk] Load Column %ld -> %ld, cdf %ld, size = %ld %ld, io_req = %ld comp_type = %d -> %d, data_len = %ld, target_seqnos.size() = %ld, %p -> %p\n", 
                            i, output_column_idxs[i], io_requested_cdf_ids[toggle][i], output.size(), comp_header.data_len, 
                            io_requested_buf_sizes[toggle][i], (int)comp_header.comp_type, (int) cur_ext_property_type[i].id(), comp_header.data_len,
                            target_seqnos.size(), io_requested_buf_ptrs[toggle][i], output.data[i].GetData());
#endif
        } else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk] Load Column %ld\n", i);
#endif
        }
        auto comp_header_valid_size = sizeof(CompressionHeader);
        if (cur_ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = cur_ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                auto strings = FlatVector::GetData<string_t>(output.data[output_column_idxs[i]]);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size);
                Vector &vids = input.data[nodeColIdx];
                auto &validity = FlatVector::Validity(output.data[output_column_idxs[i]]);
                for (auto seqno_idx = 0; seqno_idx < target_seqnos.size(); seqno_idx++) {
                    idx_t seqno = target_seqnos[seqno_idx];
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    uint64_t prev_string_offset = target_seqno == 0 ? 0 : offset_arr[target_seqno - 1];
                    uint64_t string_offset = offset_arr[target_seqno];
                    size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t) + prev_string_offset;
                    strings[seqno] = StringVector::AddString(output.data[output_column_idxs[i]], (char*)(io_requested_buf_ptrs[toggle][i] + string_data_offset), string_offset - prev_string_offset);
                    // strings[seqno] = *((string_t*)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + target_seqno * sizeof(string_t)));
                    validity.SetValid(seqno);
                }
            }
        } else if (cur_ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t offset_array_size = comp_header.data_len * type_size;

            D_ASSERT(false); /* zero copy for LIST is not implemented in this function */

            // Vector &vids = input.data[nodeColIdx];
            // size_t list_offset = 0;
            // size_t list_size_to_append = 0;
            // size_t child_type_size = GetTypeIdSize(ListType::GetChildType(ext_property_type[i]).InternalType());
            // list_entry_t *list_vec = (list_entry_t *)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size);
            // list_entry_t *output_list_vec = (list_entry_t *)output.data[output_column_idxs[i]].GetData();
            // for (auto seqno_idx = 0; target_seqnos.size(); seqno_idx++) {
            //     idx_t seqno = target_seqnos[seqno_idx];
            //     idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
            //     output_list_vec[seqno].length = list_vec[target_seqno].length;
            //     list_size_to_append += (list_vec[target_seqno].length * child_type_size);
            // }

            // Vector &child_vec = ListVector::GetEntry(output.data[output_column_idxs[i]]);
            // size_t last_offset = start_seqno == 0 ? 0 : output_list_vec[start_seqno - 1].offset + output_list_vec[start_seqno - 1].length;
            // ListVector::Reserve(output.data[output_column_idxs[i]], last_offset + list_size_to_append);
            // for (auto seqno_idx = 0; target_seqnos.size(); seqno_idx++) {
            //     idx_t seqno = target_seqnos[seqno_idx];
            //     idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
            //     icecream::ic.enable(); IC(); IC(target_seqno, last_offset, start_seqno, seqno, end_seqno, offset_array_size, list_vec[target_seqno].offset, list_vec[target_seqno].length, child_type_size); icecream::ic.disable();
            //     memcpy(child_vec.GetData() + last_offset * child_type_size, io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + offset_array_size + list_vec[target_seqno].offset * child_type_size, list_vec[target_seqno].length * child_type_size);
            //     output_list_vec[seqno].offset = last_offset;
            //     last_offset += list_vec[target_seqno].length;
            // }
        } else if (cur_ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST || cur_ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
            // TODO
        } else if (cur_ext_property_type[i].id() == LogicalTypeId::ID) {
            // throw InvalidInputException("Never occur");
            Vector &vids = input.data[nodeColIdx];
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[output_column_idxs[i]].GetData();
            idx_t output_seqno = 0;
            for (auto seqno_idx = 0; seqno_idx < target_seqnos.size(); seqno_idx++) {
                idx_t seqno = target_seqnos[seqno_idx];
                idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                id_column[seqno] = physical_id_base + target_seqno;
                D_ASSERT(id_column[seqno] == getIdRefFromVectorTemp(vids, seqno));
            }
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = cur_ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(cur_ext_property_type[i].InternalType());
                Vector &vids = input.data[nodeColIdx];
                auto &validity = FlatVector::Validity(output.data[output_column_idxs[i]]);
                for (auto seqno_idx = 0; seqno_idx < target_seqnos.size(); seqno_idx++) {
                    idx_t seqno = target_seqnos[seqno_idx];
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    memcpy(output.data[output_column_idxs[i]].GetData() + seqno * type_size, io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + target_seqno * type_size, type_size);
                    validity.SetValid(seqno);
                }
                
            }
        }
    }
// icecream::ic.enable(); IC(); icecream::ic.disable();
    return true;
}

// For Seek Operator + Filter predicate - Bulk Mode
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                                   int64_t &filterKeyColIdx, Value &filterValue, ExtentID target_eid, DataChunk &input,
                                   idx_t nodeColIdx, vector<idx_t> &output_column_idxs, vector<idx_t> &target_seqnos,
                                   idx_t &cur_output_idx, SelectionVector &sel, bool is_output_chunk_initialized) {
    int prev_toggle = toggle;
    auto &cur_ext_property_type = ext_property_types[target_idx_per_eid[current_idx]];
    Catalog& cat_instance = context.db->GetCatalog();
    // if (target_eid != current_eid) {
    //     // Keep previous values
    //     if (current_eid != std::numeric_limits<uint32_t>::max())
    //        toggle = (toggle + 1) % num_data_chunks;
    //     int next_toggle = (toggle + 1) % num_data_chunks;
    //     idx_t previous_idx = current_idx++;
    //     if (current_idx > max_idx) return false;

    //     // Request I/O to the next extent if we can support double buffering
    //     if (support_double_buffering && current_idx < max_idx) {
    //         // IC(toggle, prev_toggle, target_eid, current_eid, current_idx, ext_ids_to_iterate[current_idx]);
    //         if (current_idx < 2 || (ext_ids_to_iterate[current_idx] != ext_ids_to_iterate[current_idx - 2])) {
    //             ExtentCatalogEntry* extent_cat_entry = 
    //                 (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[current_idx]));
                
    //             // Unpin previous chunks
    //             if (current_eid != std::numeric_limits<uint32_t>::max()) {
    //                 if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[next_toggle].size() == 0);
    //                 for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
    //                     if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
    //                     ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
    //                 }
    //             }

    //             size_t chunk_size = ext_property_type.empty() ? extent_cat_entry->chunks.size() : ext_property_type.size();
    //             io_requested_cdf_ids[next_toggle].resize(chunk_size);
    //             io_requested_buf_ptrs[next_toggle].resize(chunk_size);
    //             io_requested_buf_sizes[next_toggle].resize(chunk_size);
                
    //             int j = 0;
    //             for (int i = 0; i < chunk_size; i++) {
    //                 if (!ext_property_type.empty() && ext_property_type[i] == LogicalType::ID) {
    //                     io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
    //                     j++;
    //                     continue;
    //                 }
    //                 if (!target_idx.empty() && (target_idx[j] == std::numeric_limits<uint64_t>::max())) {
    //                     io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
    //                     j++;
    //                     continue;
    //                 }
    //                 ChunkDefinitionID cdf_id = target_idx.empty() ? 
    //                     extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idx[j++] - target_idxs_offset];
    //                 io_requested_cdf_ids[next_toggle][i] = cdf_id;
    //                 string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
    //                 ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
    //             }
    //             num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
    //         }
    //     }

    //     // Request chunk cache manager to finalize I/O
    //     for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
    //         if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
    //         ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i], true, false);
    //     }
    //     current_eid = ext_ids_to_iterate[previous_idx];

    //     // Initialize output DataChunk & copy each column
    //     if (!is_output_chunk_initialized) {
    //         output.Reset();
    //         output.Initialize(ext_property_type);
    //     }
        
    //     output_eid = ext_ids_to_iterate[previous_idx];
    // } else {
    //     output_eid = current_eid;
    // }
    if (target_eid != current_eid) {
        // Keep previous values
        idx_t previous_idx, next_idx;
        if (current_eid != std::numeric_limits<uint32_t>::max()) {
            toggle = (toggle + 1) % num_data_chunks;
            previous_idx = current_idx++;
            next_idx = current_idx + 1;
        } else {
            // first time here
            previous_idx = current_idx;
            next_idx = current_idx + 1;
        }
        int next_toggle = (toggle + 1) % num_data_chunks;
        int prev_toggle = (toggle - 1 + num_data_chunks) % num_data_chunks;
        if (current_idx > max_idx) return false;
        cur_ext_property_type = ext_property_types[target_idx_per_eid[current_idx]];

        // Request I/O to the next extent if we can support double buffering
        if (support_double_buffering && next_idx < max_idx) {
            ExtentCatalogEntry* extent_cat_entry = 
                (ExtentCatalogEntry*) cat_instance.GetEntry(context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, "ext_" + std::to_string(ext_ids_to_iterate[next_idx]));
            
            // Unpin previous chunks
            if (current_eid != std::numeric_limits<uint32_t>::max()) {
                if (previous_idx == 0) D_ASSERT(io_requested_cdf_ids[next_toggle].size() == 0);
                for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size(); i++) {
                    if (io_requested_cdf_ids[next_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
                    ChunkCacheManager::ccm->UnPinSegment(io_requested_cdf_ids[next_toggle][i]);
                }
            }

            auto &next_ext_property_type = ext_property_types[target_idx_per_eid[next_idx]];
            size_t chunk_size = next_ext_property_type.empty() ? extent_cat_entry->chunks.size() : next_ext_property_type.size();
            io_requested_cdf_ids[next_toggle].resize(chunk_size);
            io_requested_buf_ptrs[next_toggle].resize(chunk_size);
            io_requested_buf_sizes[next_toggle].resize(chunk_size);
            
            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                if (!next_ext_property_type.empty() && next_ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                if (!target_idxs[target_idx_per_eid[next_idx]].empty() && 
                    (target_idxs[target_idx_per_eid[next_idx]][j] == std::numeric_limits<uint64_t>::max())) {
                    io_requested_cdf_ids[next_toggle][i] = std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id = target_idxs[target_idx_per_eid[next_idx]].empty() ? 
                    extent_cat_entry->chunks[i] : extent_cat_entry->chunks[target_idxs[target_idx_per_eid[next_idx]][j++] - target_idxs_offset];
                io_requested_cdf_ids[next_toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE + std::string("/chunk_") + std::to_string(cdf_id);
                ChunkCacheManager::ccm->PinSegment(cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i], &io_requested_buf_sizes[next_toggle][i], true);
            }
            num_tuples_in_current_extent[next_toggle] = extent_cat_entry->GetNumTuplesInExtent();
        }

        // Request chunk cache manager to finalize I/O
        for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i], true, false);
        }
        current_eid = ext_ids_to_iterate[current_idx];

        // Initialize DataChunk using cached buffer
        /*data_chunks[prev_toggle]->Destroy();
        data_chunks[prev_toggle]->Initialize(ext_property_type, io_requested_buf_ptrs[prev_toggle]);
        data_chunks[prev_toggle]->SetCardinality(io_requested_buf_sizes[prev_toggle][0] / GetTypeIdSize(ext_property_type[0].InternalType())); // XXX.. bug can occur
        output = data_chunks[prev_toggle];*/

        // Initialize output DataChunk & copy each column
        if (!is_output_chunk_initialized) {
            // icecream::ic.enable();IC();icecream::ic.disable();
            output.Reset();
            // icecream::ic.enable();IC();icecream::ic.disable();
            output.Initialize(cur_ext_property_type);
        }
        
        output_eid = ext_ids_to_iterate[current_idx];
    } else {
        output_eid = current_eid;
    }

    ChunkDefinitionID filter_cdf_id = (ChunkDefinitionID) output_eid;
    filter_cdf_id = filter_cdf_id << 32;
    filter_cdf_id = filter_cdf_id + filterKeyColIdx - target_idxs_offset;

    vector<bool> valid_output;
    valid_output.resize(target_idxs[0].size());
    if (target_idxs[0].size() == output_column_idxs.size()) { // all columns should be loaded
        for (idx_t i = 0; i < target_idxs[0].size(); i++) { valid_output[i] = true; }
    } else { // filter column does not need to be loaded (TODO)
        for (idx_t i = 0; i < target_idxs[0].size(); i++) {
            if (target_idxs[0][i] == filterKeyColIdx) valid_output[i] = false;
            else valid_output[i] = true;
        }
    }

    CompressionHeader comp_header;
    bool find_matched_row = false;
    vector<idx_t> matched_row_idxs;
    idx_t scan_start_offset, scan_end_offset, scan_length;
    // Find the column index
    auto col_idx_find_result = std::find(io_requested_cdf_ids[toggle].begin(), io_requested_cdf_ids[toggle].end(), filter_cdf_id);
    if (col_idx_find_result == io_requested_cdf_ids[toggle].end()) throw InvalidInputException("I/O Error");
    idx_t col_idx = col_idx_find_result - io_requested_cdf_ids[toggle].begin();

    ChunkDefinitionCatalogEntry* cdf_cat_entry = 
            (ChunkDefinitionCatalogEntry*) cat_instance.GetEntry(context, CatalogType::CHUNKDEFINITION_ENTRY, DEFAULT_SCHEMA, "cdf_" + std::to_string(filter_cdf_id));
    
    if (cdf_cat_entry->IsMinMaxArrayExist()) {
        vector<minmax_t> minmax = move(cdf_cat_entry->GetMinMaxArray());
        bool find_block_to_scan = false;
        for (size_t i = 0; i < minmax.size(); i++) {
            if (minmax[i].min <= filterValue.GetValue<idx_t>() && minmax[i].max >= filterValue.GetValue<idx_t>()) {
                // TODO this logic only works for sorted column
                scan_start_offset = i * MIN_MAX_ARRAY_SIZE;
                scan_end_offset = MIN((i + 1) * MIN_MAX_ARRAY_SIZE, cdf_cat_entry->GetNumEntriesInColumn());
                find_block_to_scan = true;

                // Find the index of a row that matches a predicate.
                findRowsThatSatisfyPredicate(input, nodeColIdx, target_seqnos, filterValue, col_idx, scan_start_offset, scan_end_offset, matched_row_idxs, sel);
            }
        }
        if (!find_block_to_scan) {
            output.SetCardinality(0);
            return true;
        }
    } else {
        scan_start_offset = 0;
        scan_end_offset = cdf_cat_entry->GetNumEntriesInColumn();
        findRowsThatSatisfyPredicate(input, nodeColIdx, target_seqnos, filterValue, col_idx, scan_start_offset, scan_end_offset, matched_row_idxs, sel);
    }

    if (matched_row_idxs.size() > 0) {
        idx_t tmp_output_idx = cur_output_idx;
        for (idx_t i = 0; i < matched_row_idxs.size(); i++) {
            sel.set_index(matched_row_idxs[i], tmp_output_idx++);
        }
        output.SetCardinality(matched_row_idxs.size());
    } else {
        output.SetCardinality(0);
        return true;
    }

    idx_t output_chunk_idx;
    idx_t j = 0;
    for (size_t i = 0; i < cur_ext_property_type.size(); i++) {
        output_chunk_idx = cur_output_idx;
        if (!valid_output[i]) continue;
        if (cur_ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i], sizeof(CompressionHeader));
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk] Load Column %ld -> %ld, cdf %ld, size = %ld %ld, io_req = %ld comp_type = %d -> %d, data_len = %ld, %p -> %p\n", 
                            i, output_column_idxs[j], io_requested_cdf_ids[toggle][i], output.size(), comp_header.data_len, 
                            io_requested_buf_sizes[toggle][i], (int)comp_header.comp_type, (int) cur_ext_property_type[i].id(), comp_header.data_len,
                            io_requested_buf_ptrs[toggle][i], output.data[i].GetData());
#endif
        } else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk] Load Column %ld\n", i);
#endif
        }
        auto comp_header_valid_size = sizeof(CompressionHeader);
        if (cur_ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = cur_ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                auto strings = FlatVector::GetData<string_t>(output.data[output_column_idxs[j]]);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size);
                Vector &vids = input.data[nodeColIdx];
                for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                    idx_t seqno = matched_row_idxs[idx];
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    // strings[output_chunk_idx++] = *((string_t*)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + target_seqno * sizeof(string_t)));
                    uint64_t prev_string_offset = target_seqno == 0 ? 0 : offset_arr[target_seqno - 1];
                    uint64_t string_offset = offset_arr[target_seqno];
                    size_t string_data_offset = sizeof(CompressionHeader) + comp_header.data_len * sizeof(uint64_t) + prev_string_offset;
                    strings[output_chunk_idx++] = StringVector::AddString(output.data[output_column_idxs[j]], (char*)(io_requested_buf_ptrs[toggle][i] + string_data_offset), string_offset - prev_string_offset);
                }
            }
        } else if (cur_ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t offset_array_size = comp_header.data_len * type_size;

            D_ASSERT(false); /* zero copy for LIST is not implemented in this function */

            // Vector &vids = input.data[nodeColIdx];
            // size_t list_offset = 0;
            // size_t list_size_to_append = 0;
            // size_t child_type_size = GetTypeIdSize(ListType::GetChildType(cur_ext_property_type[i]).InternalType());
            // list_entry_t *list_vec = (list_entry_t *)(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size);
            // list_entry_t *output_list_vec = (list_entry_t *)output.data[output_column_idxs[j]].GetData();

            // for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
            //     idx_t seqno = matched_row_idxs[idx];
            //     idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
            //     output_list_vec[output_chunk_idx++].length = list_vec[target_seqno].length;
            //     list_size_to_append += (list_vec[target_seqno].length * child_type_size);
            // }

            // Vector &child_vec = ListVector::GetEntry(output.data[output_column_idxs[j]]);
            // size_t last_offset = start_seqno == 0 ? 0 : output_list_vec[start_seqno - 1].offset + output_list_vec[start_seqno - 1].length;
            // ListVector::Reserve(output.data[output_column_idxs[j]], last_offset + list_size_to_append);
            // output_chunk_idx = cur_output_idx;

            // for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
            //     idx_t seqno = matched_row_idxs[idx];
            //     idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
            //     // icecream::ic.enable(); IC(); IC(target_seqno, last_offset, start_seqno, seqno, end_seqno, offset_array_size, list_vec[target_seqno].offset, list_vec[target_seqno].length, child_type_size); icecream::ic.disable();
            //     memcpy(child_vec.GetData() + last_offset * child_type_size, io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + offset_array_size + list_vec[target_seqno].offset * child_type_size, list_vec[target_seqno].length * child_type_size);
            //     output_list_vec[output_chunk_idx++].offset = last_offset;
            //     last_offset += list_vec[target_seqno].length;
            // }
        } else if (cur_ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST || cur_ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
            // TODO
            D_ASSERT(false);
        } else if (cur_ext_property_type[i].id() == LogicalTypeId::ID) {
            Vector &vids = input.data[nodeColIdx];
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[output_column_idxs[j]].GetData();
            idx_t output_seqno = 0;
            for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                idx_t seqno = matched_row_idxs[idx];
                idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                id_column[output_chunk_idx] = physical_id_base + target_seqno;
                D_ASSERT(id_column[output_chunk_idx] == getIdRefFromVectorTemp(vids, seqno));
                output_chunk_idx++;
            }
        } else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = cur_ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] + comp_header_valid_size, io_requested_buf_sizes[toggle][i] -  comp_header_valid_size,
                                       output.data[i], comp_header.data_len);
            } else {
                size_t type_size = GetTypeIdSize(cur_ext_property_type[i].InternalType());
                Vector &vids = input.data[nodeColIdx];
                
                for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                    idx_t seqno = matched_row_idxs[idx];
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    memcpy(output.data[output_column_idxs[j]].GetData() + output_chunk_idx * type_size, io_requested_buf_ptrs[toggle][i] + comp_header_valid_size + target_seqno * type_size, type_size);
                    output_chunk_idx++;
                }
                
            }
        }
        j++;
    }

    cur_output_idx += matched_row_idxs.size();

    return true;
}

// For AdjList
bool ExtentIterator::GetExtent(data_ptr_t &chunk_ptr, int target_toggle, bool is_initialized) {
    D_ASSERT(ext_property_type[0] == LogicalType::FORWARD_ADJLIST || ext_property_type[0] == LogicalType::BACKWARD_ADJLIST); // Only for ADJLIIST now..
    // Keep previous values
    int prev_toggle = toggle;
    if (current_idx > max_idx) return false;

    // Request chunk cache manager to finalize I/O
    if (!is_initialized) { // We don't need I/O actually..
        for (int i = 0; i < io_requested_cdf_ids[target_toggle].size(); i++) {
            if (io_requested_cdf_ids[target_toggle][i] == std::numeric_limits<ChunkDefinitionID>::max()) continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[target_toggle][i], true, false);
        }
    }

    CompressionHeader comp_header;
    
    D_ASSERT(ext_property_type.size() == 1);
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        chunk_ptr = (data_ptr_t)(io_requested_buf_ptrs[target_toggle][i] + sizeof(CompressionHeader));
    }
    return true;
}


bool ExtentIterator::_CheckIsMemoryEnough() {
    // TODO check memory.. if possible, use double buffering
    // Maybe this code is useless. Leave it to BFM
    bool enough = true;

    return enough;
}

}