#include "storage/extent/extent_iterator.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "storage/cache/disk_aio/TypeDef.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"

#include "common/types/rowcol_type.hpp"
#include "icecream.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

/* velox related headers */
#include <numeric>  // For std::iota
#include "common/types/validity_mask.hpp"
#include "common/types/value.hpp"
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/type/Filter.h"

using namespace facebook::velox;

namespace duckdb {

// #define DO_SIMD_FOR_SEEK
// #define DO_PREFETCH_FOR_SEEK

inline int128_t ConvertTo128(const hugeint_t &value)
{
    int128_t result = static_cast<int128_t>(value.upper);
    result = result << 64;
    result |= static_cast<int128_t>(value.lower);
    return result;
}

// TODO: select extent to iterate using min & max & key
// Initialize iterator that iterates all extents
void ExtentIterator::Initialize(
    ClientContext &context,
    PropertySchemaCatalogEntry *property_schema_cat_entry)
{
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
        // Initialize Data Chunks
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++)
            data_chunks[i] = new DataChunk();
    }
    else {
        support_double_buffering = false;
        num_data_chunks = 1;
        // Initialize Data Chunks
        data_chunks[0] = new DataChunk();
        for (int i = 1; i < MAX_NUM_DATA_CHUNKS; i++)
            data_chunks[i] = nullptr;
    }

    toggle = 0;
    current_idx = 0;
    current_idx_in_this_extent = 0;
    max_idx = property_schema_cat_entry->extent_ids.size();
    ext_property_type = move(property_schema_cat_entry->GetTypesWithCopy());

    ext_ids_to_iterate.reserve(property_schema_cat_entry->extent_ids.size());
    for (int i = 0; i < property_schema_cat_entry->extent_ids.size(); i++)
        ext_ids_to_iterate.push_back(property_schema_cat_entry->extent_ids[i]);

    Catalog &cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        if (!ObtainFromCache(ext_ids_to_iterate[current_idx], toggle)) {
            ExtentCatalogEntry *extent_cat_entry =
                (ExtentCatalogEntry *)cat_instance.GetEntry(
                    context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                    DEFAULT_EXTENT_PREFIX +
                        std::to_string(ext_ids_to_iterate[current_idx]));

            size_t chunk_size = extent_cat_entry->chunks.size();
            io_requested_cdf_ids[toggle].resize(chunk_size);
            io_requested_buf_ptrs[toggle].resize(chunk_size);
            io_requested_buf_sizes[toggle].resize(chunk_size);

            for (int i = 0; i < chunk_size; i++) {
                ChunkDefinitionID cdf_id = extent_cat_entry->chunks[i];
                io_requested_cdf_ids[toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE +
                                   std::string("/chunk_") +
                                   std::to_string(cdf_id);
                // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                ChunkCacheManager::ccm->PinSegment(
                    cdf_id, file_path, &io_requested_buf_ptrs[toggle][i],
                    &io_requested_buf_sizes[toggle][i], true);
            }
            num_tuples_in_current_extent[toggle] =
                extent_cat_entry->GetNumTuplesInExtent();
            PopulateCache(ext_ids_to_iterate[current_idx], toggle);
        }
    }
    is_initialized = true;
}

void ExtentIterator::Initialize(
    ClientContext &context,
    PropertySchemaCatalogEntry *property_schema_cat_entry,
    vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_)
{
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
        // Initialize Data Chunks
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++)
            data_chunks[i] = new DataChunk();
    }
    else {
        support_double_buffering = false;
        num_data_chunks = 1;
        // Initialize Data Chunks
        data_chunks[0] = new DataChunk();
        for (int i = 1; i < MAX_NUM_DATA_CHUNKS; i++)
            data_chunks[i] = nullptr;
    }

    toggle = 0;
    current_idx = 0;
    current_idx_in_this_extent = 0;
    max_idx = property_schema_cat_entry->extent_ids.size();
    ext_property_type = target_types_;
    target_idx = target_idxs_;

    ext_ids_to_iterate.reserve(property_schema_cat_entry->extent_ids.size());
    for (size_t i = 0; i < property_schema_cat_entry->extent_ids.size(); i++)
        ext_ids_to_iterate.push_back(property_schema_cat_entry->extent_ids[i]);

    target_idxs_offset = 1;
    // for (int i = 0; i < ext_property_type.size(); i++) {
    //     if (ext_property_type[i] == LogicalType::ID) {
    //         target_idxs_offset = 1;
    //         break;
    //     }
    // }

    Catalog &cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        if (!ObtainFromCache(ext_ids_to_iterate[current_idx], toggle)) {
            ExtentCatalogEntry *extent_cat_entry =
                (ExtentCatalogEntry *)cat_instance.GetEntry(
                    context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                    DEFAULT_EXTENT_PREFIX +
                        std::to_string(ext_ids_to_iterate[current_idx]));

            size_t chunk_size = ext_property_type.size();
            io_requested_cdf_ids[toggle].resize(chunk_size);
            io_requested_buf_ptrs[toggle].resize(chunk_size);
            io_requested_buf_sizes[toggle].resize(chunk_size);

            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                // icecream::ic.enable(); IC(); IC(i, (int)ext_property_type[i].id()); icecream::ic.disable();
                if (ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    // icecream::ic.enable(); IC(); IC(i, io_requested_cdf_ids[toggle][i]); icecream::ic.disable();
                    j++;
                    continue;
                }
                if (target_idx[j] == std::numeric_limits<uint64_t>::max()) {
                    io_requested_cdf_ids[toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    // icecream::ic.enable(); IC(); IC(i, io_requested_cdf_ids[toggle][i]); icecream::ic.disable();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id =
                    extent_cat_entry->chunks[target_idx[j++] -
                                             target_idxs_offset];  // TODO bug..
                io_requested_cdf_ids[toggle][i] = cdf_id;
                // icecream::ic.enable(); IC(); IC(i, io_requested_cdf_ids[toggle][i]); icecream::ic.disable();
                string file_path = DiskAioParameters::WORKSPACE +
                                   std::string("/chunk_") +
                                   std::to_string(cdf_id);
                // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                ChunkCacheManager::ccm->PinSegment(
                    cdf_id, file_path, &io_requested_buf_ptrs[toggle][i],
                    &io_requested_buf_sizes[toggle][i], true);
            }
            num_tuples_in_current_extent[toggle] =
                extent_cat_entry->GetNumTuplesInExtent();
            PopulateCache(ext_ids_to_iterate[current_idx], toggle);
        }
    }
    is_initialized = true;
}

void ExtentIterator::Initialize(ClientContext &context,
                                vector<LogicalType> &target_types_,
                                vector<idx_t> &target_idxs_,
                                ExtentID target_eid)
{
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
    }
    else {
        support_double_buffering = false;
        num_data_chunks = 1;
    }

    toggle = 0;
    current_idx = 0;
    current_idx_in_this_extent = 0;
    max_idx = 1;
    ext_property_type = target_types_;
    target_idx = target_idxs_;
    ext_ids_to_iterate.push_back(target_eid);

    Catalog &cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        if (!ObtainFromCache(ext_ids_to_iterate[current_idx], toggle)) {
            ExtentCatalogEntry *extent_cat_entry =
                (ExtentCatalogEntry *)cat_instance.GetEntry(
                    context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                    DEFAULT_EXTENT_PREFIX +
                        std::to_string(ext_ids_to_iterate[current_idx]));

            size_t chunk_size = ext_property_type.size();
            io_requested_cdf_ids[toggle].resize(chunk_size);
            io_requested_buf_ptrs[toggle].resize(chunk_size);
            io_requested_buf_sizes[toggle].resize(chunk_size);

            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                ChunkDefinitionID cdf_id;
                if (ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    continue;
                }
                else if (ext_property_type[i] == LogicalType::FORWARD_ADJLIST ||
                         ext_property_type[i] ==
                             LogicalType::BACKWARD_ADJLIST) {
                    cdf_id = extent_cat_entry->adjlist_chunks[target_idx[j++]];
                }
                else {
                    cdf_id = extent_cat_entry->chunks[target_idx[j++]];
                }
                io_requested_cdf_ids[toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE +
                                   std::string("/chunk_") +
                                   std::to_string(cdf_id);
                // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                ChunkCacheManager::ccm->PinSegment(
                    cdf_id, file_path, &io_requested_buf_ptrs[toggle][i],
                    &io_requested_buf_sizes[toggle][i], true);
            }
            num_tuples_in_current_extent[toggle] =
                extent_cat_entry->GetNumTuplesInExtent();
            PopulateCache(ext_ids_to_iterate[current_idx], toggle);
        }
    }
    is_initialized = true;
}

int ExtentIterator::RequestNewIO(ClientContext &context, ExtentID target_eid, ExtentID &evicted_eid)
{
    ext_ids_to_iterate.push_back(target_eid);

    int next_toggle = (toggle + 1) % num_data_chunks;
    idx_t previous_idx = current_idx;
    current_idx++;
    max_idx++;

    Catalog &cat_instance = context.db->GetCatalog();
    // Request I/O for the new extent
    {
        if (!ObtainFromCache(ext_ids_to_iterate[current_idx], next_toggle)) {
            ExtentCatalogEntry *extent_cat_entry =
                (ExtentCatalogEntry *)cat_instance.GetEntry(
                    context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                    DEFAULT_EXTENT_PREFIX +
                        std::to_string(ext_ids_to_iterate[current_idx]));

            for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size();
                 i++) {
                if (io_requested_cdf_ids[next_toggle][i] ==
                    std::numeric_limits<ChunkDefinitionID>::max())
                    continue;
                ChunkCacheManager::ccm->UnPinSegment(
                    io_requested_cdf_ids[next_toggle][i]);
            }

            size_t chunk_size = ext_property_type.size();
            io_requested_cdf_ids[next_toggle].resize(chunk_size);
            io_requested_buf_ptrs[next_toggle].resize(chunk_size);
            io_requested_buf_sizes[next_toggle].resize(chunk_size);

            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                ChunkDefinitionID cdf_id;
                if (ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[next_toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    continue;
                }
                else if (ext_property_type[i] == LogicalType::FORWARD_ADJLIST ||
                         ext_property_type[i] ==
                             LogicalType::BACKWARD_ADJLIST) {
                    cdf_id = extent_cat_entry->adjlist_chunks[target_idx[j++]];
                }
                else {
                    cdf_id = extent_cat_entry->chunks[target_idx[j++]];
                }
                io_requested_cdf_ids[next_toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE +
                                   std::string("/chunk_") +
                                   std::to_string(cdf_id);
                ChunkCacheManager::ccm->PinSegment(
                    cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i],
                    &io_requested_buf_sizes[next_toggle][i], true);
            }
            num_tuples_in_current_extent[next_toggle] =
                extent_cat_entry->GetNumTuplesInExtent();
            PopulateCache(ext_ids_to_iterate[current_idx], next_toggle);
        }
    }
    is_initialized = true;
    evicted_eid = previous_idx == 0 ? std::numeric_limits<ExtentID>::max()
                                    : ext_ids_to_iterate[previous_idx - 1];
    toggle++;
    return next_toggle;
}

// Initialize For Seek
void ExtentIterator::Initialize(ClientContext &context,
                                vector<idx_t> *target_idx_per_eid_,
                                vector<ExtentID> target_eids)
{
    if (_CheckIsMemoryEnough()) {
        support_double_buffering = true;
        num_data_chunks = MAX_NUM_DATA_CHUNKS;
    }
    else {
        support_double_buffering = false;
        num_data_chunks = 1;
    }

    toggle = 0;
    current_idx = 0;
    current_idx_in_this_extent = 0;
    current_eid = (ExtentID)std::numeric_limits<uint32_t>::max();
    max_idx = target_eids.size();
    target_idx_per_eid = target_idx_per_eid_;
    ext_ids_to_iterate = move(target_eids);

    target_idxs_offset = 1;

    Catalog &cat_instance = context.db->GetCatalog();
    // Request I/O for the first extent
    {
        if (!ObtainFromCache(ext_ids_to_iterate[current_idx], toggle)) {
            ExtentCatalogEntry *extent_cat_entry =
                (ExtentCatalogEntry *)cat_instance.GetEntry(
                    context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                    DEFAULT_EXTENT_PREFIX +
                        std::to_string(ext_ids_to_iterate[current_idx]));

            // size_t chunk_size = ext_property_types[current_idx].size();
            size_t chunk_size =
                ext_property_types[(*target_idx_per_eid)[current_idx]].size();
            io_requested_cdf_ids[toggle].resize(chunk_size);
            io_requested_buf_ptrs[toggle].resize(chunk_size);
            io_requested_buf_sizes[toggle].resize(chunk_size);

            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                if (ext_property_types[(*target_idx_per_eid)[current_idx]][i] ==
                    LogicalType::ID) {
                    io_requested_cdf_ids[toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                if (target_idxs[(*target_idx_per_eid)[current_idx]][j] ==
                    std::numeric_limits<uint64_t>::max()) {
                    io_requested_cdf_ids[toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id =
                    extent_cat_entry->chunks
                        [target_idxs[(*target_idx_per_eid)[current_idx]][j++] -
                         target_idxs_offset];
                io_requested_cdf_ids[toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE +
                                   std::string("/chunk_") +
                                   std::to_string(cdf_id);  // TODO wrong path
                // icecream::ic.enable(); IC(); IC(cdf_id); icecream::ic.disable();
                ChunkCacheManager::ccm->PinSegment(
                    cdf_id, file_path, &io_requested_buf_ptrs[toggle][i],
                    &io_requested_buf_sizes[toggle][i], true);
            }
            num_tuples_in_current_extent[toggle] =
                extent_cat_entry->GetNumTuplesInExtent();
            PopulateCache(ext_ids_to_iterate[current_idx], toggle);
        }
    }
    is_initialized = true;
}

void ExtentIterator::Rewind()
{
    toggle = 0;
    current_idx = 0;
    current_idx_in_this_extent = 0;
    is_rewinded = true;
    current_eid = (ExtentID)std::numeric_limits<uint32_t>::max();
    bool cache_sucess =
        ObtainFromCache(ext_ids_to_iterate[current_idx], toggle);
    if (!cache_sucess)
        throw NotImplementedException("Rewind: Cache miss");
}

bool ExtentIterator::RequestNextIO(ClientContext &context, DataChunk &output,
                                   ExtentID &output_eid,
                                   bool is_output_chunk_initialized)
{
    // Keep previous values
    idx_t previous_idx, next_idx;
    if (current_eid != std::numeric_limits<uint32_t>::max()) {
        toggle = (toggle + 1) % num_data_chunks;
        previous_idx = current_idx++;
        next_idx = current_idx + 1;
    }
    else {
        // first time here
        previous_idx = current_idx;
        next_idx = current_idx + 1;
    }
    int next_toggle = (toggle + 1) % num_data_chunks;
    int prev_toggle = (toggle - 1 + num_data_chunks) % num_data_chunks;
    if (current_idx > max_idx)
        return false;
    auto &cur_ext_property_type =
        ext_property_types[(*target_idx_per_eid)[current_idx]];

    // Request I/O to the next extent if we can support double buffering
    Catalog &cat_instance = context.db->GetCatalog();
    if (support_double_buffering && next_idx < max_idx) {
        if (!ObtainFromCache(ext_ids_to_iterate[next_idx], next_toggle)) {
            ExtentCatalogEntry *extent_cat_entry =
                (ExtentCatalogEntry *)cat_instance.GetEntry(
                    context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                    DEFAULT_EXTENT_PREFIX +
                        std::to_string(ext_ids_to_iterate[next_idx]));

            // Unpin previous chunks
            if (current_eid != std::numeric_limits<uint32_t>::max()) {
                for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size();
                     i++) {
                    if (io_requested_cdf_ids[next_toggle][i] ==
                        std::numeric_limits<ChunkDefinitionID>::max())
                        continue;
                    ChunkCacheManager::ccm->UnPinSegment(
                        io_requested_cdf_ids[next_toggle][i]);
                }
            }

            auto &next_ext_property_type =
                ext_property_types[(*target_idx_per_eid)[next_idx]];
            size_t chunk_size = next_ext_property_type.empty()
                                    ? extent_cat_entry->chunks.size()
                                    : next_ext_property_type.size();
            io_requested_cdf_ids[next_toggle].resize(chunk_size);
            io_requested_buf_ptrs[next_toggle].resize(chunk_size);
            io_requested_buf_sizes[next_toggle].resize(chunk_size);

            int j = 0;
            for (int i = 0; i < chunk_size; i++) {
                if (!next_ext_property_type.empty() &&
                    next_ext_property_type[i] == LogicalType::ID) {
                    io_requested_cdf_ids[next_toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                if (!target_idxs[(*target_idx_per_eid)[next_idx]].empty() &&
                    (target_idxs[(*target_idx_per_eid)[next_idx]][j] ==
                     std::numeric_limits<uint64_t>::max())) {
                    io_requested_cdf_ids[next_toggle][i] =
                        std::numeric_limits<ChunkDefinitionID>::max();
                    j++;
                    continue;
                }
                ChunkDefinitionID cdf_id =
                    target_idxs[(*target_idx_per_eid)[next_idx]].empty()
                        ? extent_cat_entry->chunks[i]
                        : extent_cat_entry->chunks
                              [target_idxs[(*target_idx_per_eid)[next_idx]]
                                          [j++] -
                               target_idxs_offset];
                io_requested_cdf_ids[next_toggle][i] = cdf_id;
                string file_path = DiskAioParameters::WORKSPACE +
                                   std::string("/chunk_") +
                                   std::to_string(cdf_id);
                ChunkCacheManager::ccm->PinSegment(
                    cdf_id, file_path, &io_requested_buf_ptrs[next_toggle][i],
                    &io_requested_buf_sizes[next_toggle][i], true);
            }
            num_tuples_in_current_extent[next_toggle] =
                extent_cat_entry->GetNumTuplesInExtent();
            PopulateCache(ext_ids_to_iterate[next_idx], next_toggle);

            for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
                if (io_requested_cdf_ids[toggle][i] ==
                    std::numeric_limits<ChunkDefinitionID>::max())
                    continue;
                ChunkCacheManager::ccm->FinalizeIO(
                    io_requested_cdf_ids[toggle][i], true, false);
            }
        }
        current_eid = ext_ids_to_iterate[current_idx];
    }

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(cur_ext_property_type);
    }

    output_eid = ext_ids_to_iterate[current_idx];
    return true;
}

// Get Next Extent with all properties. For full scan (w/o output_column_idxs)
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output,
                                   ExtentID &output_eid, size_t scan_size,
                                   bool is_output_chunk_initialized)
{
    // We should avoid data copy here.. but copy for demo temporarliy
    // Keep previous values
    if (current_idx_in_this_extent ==
        ((STORAGE_STANDARD_VECTOR_SIZE + scan_size - 1) / scan_size)) {
        current_idx++;
        current_idx_in_this_extent = 0;
    }
    if (current_idx > max_idx)
        return false;

    requestIOForDoubleBuffering(context);
    requestFinalizeIO();

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(ext_property_type);
    }

    if (num_tuples_in_current_extent[toggle] <
        (current_idx_in_this_extent * scan_size))
        return false;
    output_eid = ext_ids_to_iterate[current_idx];

    // Do Scan
    vector<idx_t> output_column_idxs;
    for (idx_t i = 0; i < ext_property_type.size(); i++)
        output_column_idxs.push_back(i);
    idx_t scan_begin_offset = 0, scan_end_offset = 0;
    getScanRange(scan_size, scan_begin_offset, scan_end_offset);
    referenceRows(output, output_eid, scan_size, output_column_idxs,
                  scan_begin_offset, scan_end_offset);

    current_idx_in_this_extent++;
    return true;
}

// Get Next Extent with all properties. For full scan
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output,
                                   ExtentID &output_eid,
                                   vector<idx_t> &output_column_idxs,
                                   size_t scan_size,
                                   bool is_output_chunk_initialized)
{
    // We should avoid data copy here.. but copy for demo temporarliy
    // Keep previous values
    // icecream::ic.enable(); IC(); IC(current_idx, max_idx, current_idx_in_this_extent, scan_size); icecream::ic.disable();
    if (current_idx_in_this_extent ==
        ((STORAGE_STANDARD_VECTOR_SIZE + scan_size - 1) / scan_size)) {
        current_idx++;
        current_idx_in_this_extent = 0;
    }
    if (current_idx > max_idx)
        return false;

    requestIOForDoubleBuffering(context);
    requestFinalizeIO();

    // Initialize output DataChunk & copy each column
    if (!is_output_chunk_initialized) {
        output.Destroy();
        output.Initialize(ext_property_type);
    }

    if (num_tuples_in_current_extent[toggle] <
        (current_idx_in_this_extent * scan_size))
        return false;
    output_eid = ext_ids_to_iterate[current_idx];

    // Do scan
    idx_t scan_begin_offset = 0, scan_end_offset = 0;
    getScanRange(scan_size, scan_begin_offset, scan_end_offset);
    referenceRows(output, output_eid, scan_size, output_column_idxs,
                  scan_begin_offset, scan_end_offset);

    current_idx_in_this_extent++;
    return true;
}

// Get Next Extent with filterKey
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output,
                                   FilteredChunkBuffer &output_buffer,
                                   ExtentID &output_eid,
                                   int64_t &filterKeyColIdx,
                                   duckdb::Value &filterValue,
                                   vector<idx_t> &output_column_idxs,
                                   std::vector<duckdb::LogicalType> &scanSchema,
                                   size_t scan_size,
                                   bool is_output_chunk_initialized)
{
    while (true) {
        if ((current_idx_in_this_extent ==
            ((STORAGE_STANDARD_VECTOR_SIZE + scan_size - 1) / scan_size)) ||
            (num_tuples_in_current_extent[toggle] <
            (current_idx_in_this_extent * scan_size))) {  // END OF EXTENT
            current_idx++;
            current_idx_in_this_extent = 0;
        }
        if (current_idx >= max_idx) {
            if (output_buffer.GetFilteredChunk()->size() > 0) {
                output_buffer.ReferenceAndSwitch(output);
                return true;
            }
            else {
                return false;
            }
        }

        requestIOForDoubleBuffering(context);
        requestFinalizeIO();

        CompressionHeader comp_header;
        vector<idx_t> matched_row_idxs;
        idx_t scan_start_offset, scan_end_offset;
        output_eid = ext_ids_to_iterate[current_idx];

        if (!is_output_chunk_initialized) {
            output.Reset();
            output.Initialize(ext_property_type);
        }

        auto filter_cdf_id = getFilterCDFID(output_eid, filterKeyColIdx);
        bool found_scan_range =
            getScanRange(context, filter_cdf_id, filterValue, scan_size,
                        scan_start_offset, scan_end_offset);
        current_idx_in_this_extent++;  // move to next extent for next iteration

        if (found_scan_range) {
            findMatchedRowsEQFilter(comp_header, findColumnIdx(filter_cdf_id),
                                    scan_start_offset, scan_end_offset, filterValue,
                                    matched_row_idxs);
            if (doFilterBuffer(scan_size, matched_row_idxs.size())) {
                bool is_fully_filled = copyMatchedRowsToBuffer(
                    comp_header, matched_row_idxs, output_column_idxs, output_eid,
                    output_buffer);
                if (is_fully_filled)
                    output_buffer.ReferenceAndSwitch(output);
                else {
                    continue;
                }
            }
            else {
                referenceRows(*(output_buffer.GetSliceBuffer().get()), output_eid,
                            scan_size, output_column_idxs, scan_start_offset,
                            scan_end_offset);
                sliceFilteredRows(*(output_buffer.GetSliceBuffer().get()), output,
                                scan_start_offset, matched_row_idxs);
            }
        }
        return true;
    }
}

// Get Next Extent with range filterKey
// TODO: compact parameter list
bool ExtentIterator::GetNextExtent(
    ClientContext &context, DataChunk &output,
    FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
    int64_t &filterKeyColIdx, duckdb::Value &l_filterValue,
    duckdb::Value &r_filterValue, bool l_inclusive, bool r_inclusive,
    vector<idx_t> &output_column_idxs,
    std::vector<duckdb::LogicalType> &scanSchema, size_t scan_size,
    bool is_output_chunk_initialized)
{
    if ((current_idx_in_this_extent ==
         ((STORAGE_STANDARD_VECTOR_SIZE + scan_size - 1) / scan_size)) ||
        (num_tuples_in_current_extent[toggle] <
         (current_idx_in_this_extent * scan_size))) {  // END OF EXTENT
        current_idx++;
        current_idx_in_this_extent = 0;
    }
    if (current_idx >= max_idx) {
        if (output_buffer.GetFilteredChunk()->size() > 0) {
            output_buffer.ReferenceAndSwitch(output);
            return true;
        }
        else {
            return false;
        }
    }

    requestIOForDoubleBuffering(context);
    requestFinalizeIO();

    CompressionHeader comp_header;
    vector<idx_t> matched_row_idxs;
    idx_t scan_start_offset, scan_end_offset;
    output_eid = ext_ids_to_iterate[current_idx];

    if (!is_output_chunk_initialized) {
        output.Reset();
        output.Initialize(scanSchema);
    }

    auto filter_cdf_id = getFilterCDFID(output_eid, filterKeyColIdx);
    bool found_scan_range = getScanRange(
        context, filter_cdf_id, l_filterValue, r_filterValue, l_inclusive,
        r_inclusive, scan_size, scan_start_offset, scan_end_offset);
    current_idx_in_this_extent++;

    if (found_scan_range) {
        findMatchedRowsRangeFilter(comp_header, findColumnIdx(filter_cdf_id),
                                   scan_start_offset, scan_end_offset,
                                   l_filterValue, r_filterValue, l_inclusive,
                                   r_inclusive, matched_row_idxs);
        if (doFilterBuffer(scan_size, matched_row_idxs.size())) {
            bool is_fully_filled = copyMatchedRowsToBuffer(
                comp_header, matched_row_idxs, output_column_idxs, output_eid,
                output_buffer);
            if (is_fully_filled)
                output_buffer.ReferenceAndSwitch(output);
            else {
                bool end_of_extent = !GetNextExtent(
                    context, output, output_buffer, output_eid, filterKeyColIdx,
                    l_filterValue, r_filterValue, l_inclusive, r_inclusive,
                    output_column_idxs, scanSchema, scan_size);
                if (end_of_extent)
                    output_buffer.ReferenceAndSwitch(output);
            }
        }
        else {
            referenceRows(*(output_buffer.GetSliceBuffer().get()), output_eid,
                          scan_size, output_column_idxs, scan_start_offset,
                          scan_end_offset);
            sliceFilteredRows(*(output_buffer.GetSliceBuffer().get()), output,
                              scan_start_offset, matched_row_idxs);
        }
    }
    return true;
}

//  Get Next Extent with Complex filter
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output,
                                   FilteredChunkBuffer &output_buffer,
                                   ExtentID &output_eid,
                                   ExpressionExecutor &executor,
                                   vector<idx_t> &output_column_idxs,
                                   vector<duckdb::LogicalType> &scanSchema,
                                   size_t scan_size,
                                   bool is_output_chunk_initialized)
{
    while (true) {
        // Do full scan first
        bool scan_success = GetNextExtent(
            context, *(output_buffer.GetSliceBuffer().get()), output_eid,
            output_column_idxs, scan_size, is_output_chunk_initialized);
        if (!scan_success) {
            if (output_buffer.GetFilteredChunk()->size() > 0) {
                output_buffer.ReferenceAndSwitch(output);
                return true;
            }
            else {
                return false;
            }
        }
        // Then apply filter
        SelectionVector sel(STANDARD_VECTOR_SIZE);
        idx_t result_count =
            executor.SelectExpression(*(output_buffer.GetSliceBuffer().get()), sel);

        // Based on the selecitivy, do filter buffering or passing
        CompressionHeader comp_header;
        if (doFilterBuffer(scan_size, result_count)) {
            vector<idx_t> matched_row_idxs;
            idx_t prev_scan_start_offset, prev_scan_end_offset;
            getScanRange(scan_size, current_idx_in_this_extent - 1,
                        prev_scan_start_offset, prev_scan_end_offset);
            selVectorToRowIdxs(sel, result_count, matched_row_idxs,
                            prev_scan_start_offset);
            /* This code does copy on io_buffer. May not so efficient. */
            bool is_fully_filled = copyMatchedRowsToBuffer(
                comp_header, matched_row_idxs, output_column_idxs, output_eid,
                output_buffer);
            if (is_fully_filled)
                output_buffer.ReferenceAndSwitch(output);
            else {
                continue;
            }
        }
        else {
            sliceFilteredRows(*(output_buffer.GetSliceBuffer().get()), output, sel,
                            result_count);
        }
        return true;
    }
}

/**
 * Private Functions for Scan Operation
*/

void ExtentIterator::requestFinalizeIO()
{
    if (current_idx_in_this_extent == 0) {
        for (int i = 0; i < io_requested_cdf_ids[toggle].size(); i++) {
            if (io_requested_cdf_ids[toggle][i] ==
                std::numeric_limits<ChunkDefinitionID>::max())
                continue;
            ChunkCacheManager::ccm->FinalizeIO(io_requested_cdf_ids[toggle][i],
                                               true, false);
        }
    }
}

void ExtentIterator::requestIOForDoubleBuffering(ClientContext &context)
{
    Catalog &cat_instance = context.db->GetCatalog();
    if (support_double_buffering && current_idx < max_idx &&
        current_idx_in_this_extent == 0) {
        if (current_idx != 0)
            toggle = (toggle + 1) % num_data_chunks;
        int next_toggle = (toggle + 1) % num_data_chunks;
        if (current_idx < max_idx - 1) {
            if (!ObtainFromCache(ext_ids_to_iterate[current_idx + 1],
                                 next_toggle)) {
                ExtentCatalogEntry *extent_cat_entry =
                    (ExtentCatalogEntry *)cat_instance.GetEntry(
                        context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
                        DEFAULT_EXTENT_PREFIX +
                            std::to_string(
                                ext_ids_to_iterate[current_idx + 1]));

                // Unpin previous chunks
                for (size_t i = 0; i < io_requested_cdf_ids[next_toggle].size();
                     i++) {
                    if (io_requested_cdf_ids[next_toggle][i] ==
                        std::numeric_limits<ChunkDefinitionID>::max())
                        continue;
                    ChunkCacheManager::ccm->UnPinSegment(
                        io_requested_cdf_ids[next_toggle][i]);
                }

                size_t chunk_size = ext_property_type.empty()
                                        ? extent_cat_entry->chunks.size()
                                        : ext_property_type.size();
                io_requested_cdf_ids[next_toggle].resize(chunk_size);
                io_requested_buf_ptrs[next_toggle].resize(chunk_size);
                io_requested_buf_sizes[next_toggle].resize(chunk_size);

                int j = 0;
                for (int i = 0; i < chunk_size; i++) {
                    if (!ext_property_type.empty() &&
                        ext_property_type[i] == LogicalType::ID) {
                        io_requested_cdf_ids[next_toggle][i] =
                            std::numeric_limits<ChunkDefinitionID>::max();
                        j++;
                        continue;
                    }
                    if (!target_idx.empty() &&
                        (target_idx[j] ==
                         std::numeric_limits<uint64_t>::max())) {
                        io_requested_cdf_ids[next_toggle][i] =
                            std::numeric_limits<ChunkDefinitionID>::max();
                        j++;
                        continue;
                    }
                    ChunkDefinitionID cdf_id =
                        target_idx.empty()
                            ? extent_cat_entry->chunks[i]
                            : extent_cat_entry->chunks[target_idx[j++] -
                                                       target_idxs_offset];
                    io_requested_cdf_ids[next_toggle][i] = cdf_id;
                    string file_path = DiskAioParameters::WORKSPACE +
                                       std::string("/chunk_") +
                                       std::to_string(cdf_id);
                    ChunkCacheManager::ccm->PinSegment(
                        cdf_id, file_path,
                        &io_requested_buf_ptrs[next_toggle][i],
                        &io_requested_buf_sizes[next_toggle][i], true);
                }
                num_tuples_in_current_extent[next_toggle] =
                    extent_cat_entry->GetNumTuplesInExtent();
                PopulateCache(ext_ids_to_iterate[current_idx + 1], next_toggle);
            }
        }
    }
}

ChunkDefinitionID ExtentIterator::getFilterCDFID(ExtentID output_eid,
                                                 int64_t filterKeyColIdx)
{
    ChunkDefinitionID filter_cdf_id = (ChunkDefinitionID)output_eid;
    filter_cdf_id = filter_cdf_id << 32;
    filter_cdf_id = filter_cdf_id + filterKeyColIdx - target_idxs_offset;
    return filter_cdf_id;
}

bool ExtentIterator::getScanRange(size_t scan_size, idx_t idx_in_extent,
                                  idx_t &scan_start_offset,
                                  idx_t &scan_end_offset)
{
    scan_start_offset = idx_in_extent * scan_size;
    scan_end_offset = std::min((idx_in_extent + 1) * scan_size,
                               num_tuples_in_current_extent[toggle]);
    return true;
}

bool ExtentIterator::getScanRange(size_t scan_size, idx_t &scan_begin_offset,
                                  idx_t &scan_end_offset)
{
    scan_begin_offset = current_idx_in_this_extent * scan_size;
    scan_end_offset = std::min((current_idx_in_this_extent + 1) * scan_size,
                               num_tuples_in_current_extent[toggle]);
    return true;
}

bool ExtentIterator::getScanRange(ClientContext &context,
                                  ChunkDefinitionID filter_cdf_id,
                                  Value &filterValue, size_t scan_size,
                                  idx_t &scan_start_offset,
                                  idx_t &scan_end_offset)
{
    Catalog &cat_instance = context.db->GetCatalog();
    ChunkDefinitionCatalogEntry *cdf_cat_entry =
        (ChunkDefinitionCatalogEntry *)cat_instance.GetEntry(
            context, CatalogType::CHUNKDEFINITION_ENTRY, DEFAULT_SCHEMA,
            "cdf_" + std::to_string(filter_cdf_id));

    bool find_block_to_scan = false;
    if (cdf_cat_entry->IsMinMaxArrayExist()) {
        vector<minmax_t> minmax = move(cdf_cat_entry->GetMinMaxArray());
        for (; current_idx_in_this_extent < minmax.size();
             current_idx_in_this_extent++) {
            if (minmax[current_idx_in_this_extent].min <=
                    filterValue.GetValue<idx_t>() &&
                minmax[current_idx_in_this_extent].max >=
                    filterValue.GetValue<idx_t>()) {
                scan_start_offset =
                    current_idx_in_this_extent * MIN_MAX_ARRAY_SIZE;
                scan_end_offset =
                    MIN((current_idx_in_this_extent + 1) * MIN_MAX_ARRAY_SIZE,
                        cdf_cat_entry->GetNumEntriesInColumn());
                find_block_to_scan = true;
                break;
            }
        }
    }
    else {
        find_block_to_scan = true;
        scan_start_offset = current_idx_in_this_extent * scan_size;
        scan_end_offset = std::min((current_idx_in_this_extent + 1) * scan_size,
                                   num_tuples_in_current_extent[toggle]);
    }

    return find_block_to_scan;
}

bool ExtentIterator::getScanRange(ClientContext &context,
                                  ChunkDefinitionID filter_cdf_id,
                                  duckdb::Value &l_filterValue,
                                  duckdb::Value &r_filterValue,
                                  bool l_inclusive, bool r_inclusive,
                                  size_t scan_size, idx_t &scan_start_offset,
                                  idx_t &scan_end_offset)
{
    Catalog &cat_instance = context.db->GetCatalog();
    ChunkDefinitionCatalogEntry *cdf_cat_entry =
        (ChunkDefinitionCatalogEntry *)cat_instance.GetEntry(
            context, CatalogType::CHUNKDEFINITION_ENTRY, DEFAULT_SCHEMA,
            "cdf_" + std::to_string(filter_cdf_id));

    bool find_block_to_scan = false;

    D_ASSERT(l_filterValue.type() == r_filterValue.type());
    idx_t l_val, r_val;
    switch (l_filterValue.type().InternalType()) {
        case PhysicalType::INT32: {
            if (l_filterValue.GetValue<int32_t>() < 0) {
                l_val = 0;
            }
            else {
                l_val = (uint64_t)l_filterValue.GetValue<int32_t>();
            }
            r_val = (uint64_t)r_filterValue.GetValue<int32_t>();
            break;
        }
        case PhysicalType::INT64: {
            if (l_filterValue.GetValue<int64_t>() < 0) {
                l_val = 0;
            }
            else {
                l_val = (uint64_t)l_filterValue.GetValue<int64_t>();
            }
            r_val = (uint64_t)r_filterValue.GetValue<int64_t>();
            break;
        }
        case PhysicalType::UINT32: {
            l_val = l_filterValue.GetValue<uint32_t>();
            if (r_filterValue.GetValue<uint64_t>() >
                std::numeric_limits<uint32_t>::max()) {
                r_val = std::numeric_limits<uint32_t>::max();
            }
            else {
                r_val = r_filterValue.GetValue<uint32_t>();
            }
            break;
        }
        case PhysicalType::UINT64: {
            l_val = l_filterValue.GetValue<uint64_t>();
            r_val = r_filterValue.GetValue<uint64_t>();
            break;
        }
        default: {
            throw NotImplementedException("Not implemented filter type");
        }
    }
    if (cdf_cat_entry->IsMinMaxArrayExist()) {
        vector<minmax_t> minmax = move(cdf_cat_entry->GetMinMaxArray());
        for (; current_idx_in_this_extent < minmax.size();
             current_idx_in_this_extent++) {
            if (l_inclusive && r_inclusive) {
                if (minmax[current_idx_in_this_extent].min <= r_val &&
                    minmax[current_idx_in_this_extent].max >= l_val) {
                    scan_start_offset =
                        current_idx_in_this_extent * MIN_MAX_ARRAY_SIZE;
                    scan_end_offset = MIN(
                        (current_idx_in_this_extent + 1) * MIN_MAX_ARRAY_SIZE,
                        cdf_cat_entry->GetNumEntriesInColumn());
                    find_block_to_scan = true;
                    break;
                }
            }
            else if (l_inclusive && !r_inclusive) {
                if (minmax[current_idx_in_this_extent].min <= r_val &&
                    minmax[current_idx_in_this_extent].max > l_val) {
                    scan_start_offset =
                        current_idx_in_this_extent * MIN_MAX_ARRAY_SIZE;
                    scan_end_offset = MIN(
                        (current_idx_in_this_extent + 1) * MIN_MAX_ARRAY_SIZE,
                        cdf_cat_entry->GetNumEntriesInColumn());
                    find_block_to_scan = true;
                    break;
                }
            }
            else if (!l_inclusive && r_inclusive) {
                if (minmax[current_idx_in_this_extent].min < r_val &&
                    minmax[current_idx_in_this_extent].max >= l_val) {
                    scan_start_offset =
                        current_idx_in_this_extent * MIN_MAX_ARRAY_SIZE;
                    scan_end_offset = MIN(
                        (current_idx_in_this_extent + 1) * MIN_MAX_ARRAY_SIZE,
                        cdf_cat_entry->GetNumEntriesInColumn());
                    find_block_to_scan = true;
                    break;
                }
            }
            else {
                if (minmax[current_idx_in_this_extent].min < r_val &&
                    minmax[current_idx_in_this_extent].max > l_val) {
                    scan_start_offset =
                        current_idx_in_this_extent * MIN_MAX_ARRAY_SIZE;
                    scan_end_offset = MIN(
                        (current_idx_in_this_extent + 1) * MIN_MAX_ARRAY_SIZE,
                        cdf_cat_entry->GetNumEntriesInColumn());
                    find_block_to_scan = true;
                    break;
                }
            }
        }
    }
    else {
        find_block_to_scan = true;
        scan_start_offset = current_idx_in_this_extent * scan_size;
        scan_end_offset = std::min((current_idx_in_this_extent + 1) * scan_size,
                                   num_tuples_in_current_extent[toggle]);
    }

    return find_block_to_scan;
}

void ExtentIterator::getValidOutputMask(vector<idx_t> &output_column_idxs,
                                        vector<bool> &valid_output)
{
    valid_output.resize(target_idx.size());
    idx_t output_idx = 0;
    for (idx_t i = 0; i < target_idx.size(); i++) {
        if (output_column_idxs.size() == 0) {
            valid_output[i] = false;
            continue;
        }
        else {
            if (output_column_idxs[output_idx] == target_idx[i]) {
                valid_output[i] = true;
                output_idx++;
            }
            else {
                valid_output[i] = false;
            }
        }
    }
}

idx_t ExtentIterator::findColumnIdx(ChunkDefinitionID filter_cdf_id)
{
    auto col_idx_find_result =
        std::find(io_requested_cdf_ids[toggle].begin(),
                  io_requested_cdf_ids[toggle].end(), filter_cdf_id);
    if (col_idx_find_result == io_requested_cdf_ids[toggle].end())
        throw InvalidInputException("I/O Error");
    return col_idx_find_result - io_requested_cdf_ids[toggle].begin();
}

void ExtentIterator::findMatchedRowsEQFilter(
    CompressionHeader &comp_header, idx_t col_idx, idx_t scan_start_offset,
    idx_t scan_end_offset, Value &filterValue, vector<idx_t> &matched_row_idxs)
{
    LogicalType column_type = ext_property_type[col_idx];
    if (column_type == LogicalType::FORWARD_ADJLIST ||
        column_type == LogicalType::BACKWARD_ADJLIST) {
        throw InvalidInputException("Filter predicate on ADJLIST column");
    }
    else if (column_type == LogicalType::ID) {
        throw InvalidInputException("Filter predicate on PID column");
    }
    Vector column_vec(column_type,
                      (data_ptr_t)(io_requested_buf_ptrs[toggle][col_idx] +
                                   CompressionHeader::GetSizeWoBitSet()));
    memcpy(&comp_header, io_requested_buf_ptrs[toggle][col_idx],
           CompressionHeader::GetSizeWoBitSet());
    ValidityMask src_validity;
    if (comp_header.HasNullMask()) {
        // auto &validity = FlatVector::Validity(column_vec);
        size_t bitmap_ptr_offset = comp_header.GetNullBitmapOffset();
        src_validity =
            ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][col_idx] +
                                      bitmap_ptr_offset));
    }
    auto value_type = filterValue.type();
    if (column_type == LogicalType::BIGINT) {
        auto bigint_value = filterValue.GetValue<int64_t>();
        auto filter = std::make_unique<common::BigintRange>(
            bigint_value, bigint_value, false);
        evalPredicateSIMD<int64_t, common::BigintRange>(
            column_vec, comp_header.data_len, filter, scan_start_offset,
            scan_end_offset, matched_row_idxs);
    }
    else if (column_type == LogicalType::INTEGER) {
        auto int_value = filterValue.GetValue<int32_t>();
        auto filter = std::make_unique<common::BigintRange>(
            static_cast<int64_t>(int_value), static_cast<int64_t>(int_value),
            false);
        evalPredicateSIMD<int32_t, common::BigintRange>(
            column_vec, comp_header.data_len, filter, scan_start_offset,
            scan_end_offset, matched_row_idxs);
    }
    else if (column_type == LogicalType::HUGEINT) {
        D_ASSERT(false);  //AVX2 not supported for Hugeint
    }
    else if (column_type.id() == LogicalTypeId::DECIMAL &&
             value_type.id() == LogicalTypeId::DECIMAL) {
        switch (column_type.InternalType()) {
            case PhysicalType::INT16: {
                auto int16_value = filterValue.GetValue<int16_t>();
                auto filter = std::make_unique<common::BigintRange>(
                    static_cast<int64_t>(int16_value),
                    static_cast<int64_t>(int16_value), false);
                evalPredicateSIMD<int16_t, common::BigintRange>(
                    column_vec, comp_header.data_len, filter, scan_start_offset,
                    scan_end_offset, matched_row_idxs);
                break;
            }
            case PhysicalType::INT32: {
                auto int32_value = filterValue.GetValue<int32_t>();
                auto filter = std::make_unique<common::BigintRange>(
                    static_cast<int64_t>(int32_value),
                    static_cast<int64_t>(int32_value), false);
                evalPredicateSIMD<int32_t, common::BigintRange>(
                    column_vec, comp_header.data_len, filter, scan_start_offset,
                    scan_end_offset, matched_row_idxs);
                break;
            }
            case PhysicalType::INT64: {
                auto int64_value = filterValue.GetValue<int64_t>();
                auto filter = std::make_unique<common::BigintRange>(
                    static_cast<int64_t>(int64_value),
                    static_cast<int64_t>(int64_value), false);
                evalPredicateSIMD<int64_t, common::BigintRange>(
                    column_vec, comp_header.data_len, filter, scan_start_offset,
                    scan_end_offset, matched_row_idxs);
                break;
            }
            default:
                D_ASSERT(false);
        }
    }
    else if (column_type == LogicalType::DATE) {
        auto date_value = filterValue.GetValue<date_t>();
        auto days = date_value.days;
        auto filter = std::make_unique<common::BigintRange>(days, days, false);
        evalPredicateSIMD<int32_t, common::BigintRange>(
            column_vec, comp_header.data_len, filter, scan_start_offset,
            scan_end_offset, matched_row_idxs);
    }
    else {
        if (comp_header.comp_type == BITPACKING) {
            throw NotImplementedException(
                "Filter predicate on BITPACKING compression is not implemented "
                "yet");
        }
        else {
            for (idx_t input_idx = scan_start_offset;
                 input_idx < scan_end_offset; input_idx++) {
                // if (FlatVector::IsNull(column_vec, input_idx)) continue;
                if (!src_validity.RowIsValid(input_idx))
                    continue;
                if (column_vec.GetValue(input_idx) == filterValue) {
                    matched_row_idxs.push_back(input_idx);
                }
            }
        }
    }
}

void ExtentIterator::findMatchedRowsRangeFilter(
    CompressionHeader &comp_header, idx_t col_idx, idx_t scan_start_offset,
    idx_t scan_end_offset, Value &l_filterValue, Value &r_filterValue,
    bool l_inclusive, bool r_inclusive, vector<idx_t> &matched_row_idxs)
{
    LogicalType column_type = ext_property_type[col_idx];
    if (column_type == LogicalType::FORWARD_ADJLIST ||
        column_type == LogicalType::BACKWARD_ADJLIST) {
        throw InvalidInputException("Range filter predicate on ADJLIST column");
    }
    else if (column_type == LogicalType::ID) {
        throw InvalidInputException("Range filter predicate on PID column");
    }
    Vector column_vec(column_type,
                      (data_ptr_t)(io_requested_buf_ptrs[toggle][col_idx] +
                                   CompressionHeader::GetSizeWoBitSet()));
    memcpy(&comp_header, io_requested_buf_ptrs[toggle][col_idx],
           CompressionHeader::GetSizeWoBitSet());
    if (comp_header.HasNullMask()) {
        auto &validity = FlatVector::Validity(column_vec);
        size_t bitmap_ptr_offset = comp_header.GetNullBitmapOffset();
        validity =
            ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][col_idx] +
                                      bitmap_ptr_offset));
    }
    auto value_type = l_filterValue.type();

    if (column_type == LogicalType::BIGINT) {
        auto l_bigint_value = l_filterValue.GetValue<int64_t>();
        auto r_bigint_value = r_filterValue.GetValue<int64_t>();
        if (!l_inclusive)
            l_bigint_value = l_bigint_value + 1;
        if (!r_inclusive)
            r_bigint_value = r_bigint_value - 1;
        auto filter = std::make_unique<common::BigintRange>(
            l_bigint_value, r_bigint_value, false);
        evalPredicateSIMD<int64_t, common::BigintRange>(
            column_vec, comp_header.data_len, filter, scan_start_offset,
            scan_end_offset, matched_row_idxs);
    }
    else if (column_type == LogicalType::INTEGER) {
        auto l_int_value = l_filterValue.GetValue<int32_t>();
        auto r_int_value = r_filterValue.GetValue<int32_t>();
        if (!l_inclusive)
            l_int_value = l_int_value + 1;
        if (!r_inclusive)
            r_int_value = r_int_value - 1;
        auto filter = std::make_unique<common::BigintRange>(
            static_cast<int64_t>(l_int_value),
            static_cast<int64_t>(r_int_value), false);
        evalPredicateSIMD<int32_t, common::BigintRange>(
            column_vec, comp_header.data_len, filter, scan_start_offset,
            scan_end_offset, matched_row_idxs);
    }
    else if (column_type == LogicalType::HUGEINT) {
        D_ASSERT(false);  //AVX2 not supported for Hugeint
    }
    else if (column_type.id() == LogicalTypeId::DECIMAL &&
             value_type.id() == LogicalTypeId::DECIMAL) {
        switch (column_type.InternalType()) {
            case PhysicalType::INT16: {
                auto l_int16_value = l_filterValue.GetValue<int16_t>();
                auto r_int16_value = r_filterValue.GetValue<int16_t>();
                if (!l_inclusive)
                    l_int16_value = l_int16_value + 1;
                if (!r_inclusive)
                    r_int16_value = r_int16_value - 1;
                auto filter = std::make_unique<common::BigintRange>(
                    static_cast<int64_t>(l_int16_value),
                    static_cast<int64_t>(r_int16_value), false);
                evalPredicateSIMD<int16_t, common::BigintRange>(
                    column_vec, comp_header.data_len, filter, scan_start_offset,
                    scan_end_offset, matched_row_idxs);
                break;
            }
            case PhysicalType::INT32: {
                auto l_int32_value = l_filterValue.GetValue<int32_t>();
                auto r_int32_value = r_filterValue.GetValue<int32_t>();
                if (!l_inclusive)
                    l_int32_value = l_int32_value + 1;
                if (!r_inclusive)
                    r_int32_value = r_int32_value - 1;
                auto filter = std::make_unique<common::BigintRange>(
                    static_cast<int64_t>(l_int32_value),
                    static_cast<int64_t>(r_int32_value), false);
                evalPredicateSIMD<int32_t, common::BigintRange>(
                    column_vec, comp_header.data_len, filter, scan_start_offset,
                    scan_end_offset, matched_row_idxs);
                break;
            }
            case PhysicalType::INT64: {
                auto l_int64_value = l_filterValue.GetValue<int64_t>();
                auto r_int64_value = r_filterValue.GetValue<int64_t>();
                if (!l_inclusive)
                    l_int64_value = l_int64_value + 1;
                if (!r_inclusive)
                    r_int64_value = r_int64_value - 1;
                auto filter = std::make_unique<common::BigintRange>(
                    static_cast<int64_t>(l_int64_value),
                    static_cast<int64_t>(r_int64_value), false);
                evalPredicateSIMD<int64_t, common::BigintRange>(
                    column_vec, comp_header.data_len, filter, scan_start_offset,
                    scan_end_offset, matched_row_idxs);
                break;
            }
            default:
                D_ASSERT(false);
        }
    }
    else if (column_type == LogicalType::DATE) {
        auto l_date_value = l_filterValue.GetValue<date_t>();
        auto l_days = l_date_value.days;
        auto r_date_value = r_filterValue.GetValue<date_t>();
        auto r_days = r_date_value.days;
        if (!l_inclusive)
            l_days = l_days + 1;
        if (!r_inclusive)
            r_days = r_days - 1;
        auto filter =
            std::make_unique<common::BigintRange>(l_days, r_days, false);
        evalPredicateSIMD<int32_t, common::BigintRange>(
            column_vec, comp_header.data_len, filter, scan_start_offset,
            scan_end_offset, matched_row_idxs);
    }
    else {
        if (comp_header.comp_type == BITPACKING) {
            throw NotImplementedException(
                "Range filter predicate on BITPACKING compression is not "
                "implemented yet");
        }
        else {
            for (idx_t input_idx = scan_start_offset;
                 input_idx < scan_end_offset; input_idx++) {
                auto value = column_vec.GetValue(input_idx);
                if (inclusiveAwareRangePredicateCheck(
                        l_filterValue, r_filterValue, l_inclusive, r_inclusive,
                        value))
                    matched_row_idxs.push_back(input_idx);
            }
        }
    }
}

bool ExtentIterator::inclusiveAwareRangePredicateCheck(Value &l_filterValue,
                                                       Value &r_filterValue,
                                                       bool l_inclusive,
                                                       bool r_inclusive,
                                                       Value &filterValue)
{
    if (l_inclusive) {
        if (r_inclusive) {
            if (l_filterValue <= filterValue && filterValue <= r_filterValue) {
                return true;
            }
        }
        else {
            if (l_filterValue <= filterValue && filterValue < r_filterValue) {
                return true;
            }
        }
    }
    else {
        if (r_inclusive) {
            if (l_filterValue < filterValue && filterValue <= r_filterValue) {
                return true;
            }
        }
        else {
            if (l_filterValue < filterValue && filterValue < r_filterValue) {
                return true;
            }
        }
    }
    return false;
}

bool ExtentIterator::copyMatchedRowsToBuffer(CompressionHeader &comp_header,
                                             vector<idx_t> &matched_row_idxs,
                                             vector<idx_t> &output_column_idxs,
                                             ExtentID &output_eid,
                                             FilteredChunkBuffer &output)
{
    size_t current_buffer_size = output.GetFilteredChunk()->size();
    size_t after_copy_size = current_buffer_size + matched_row_idxs.size();
    if (after_copy_size >= STANDARD_VECTOR_SIZE) {
        vector<idx_t> matched_row_idxs_for_current_buffer(
            matched_row_idxs.begin(),
            matched_row_idxs.begin() +
                (STANDARD_VECTOR_SIZE - current_buffer_size));
        vector<idx_t> matched_row_idxs_for_next_buffer(
            matched_row_idxs.begin() +
                (STANDARD_VECTOR_SIZE - current_buffer_size),
            matched_row_idxs.end());
        copyMatchedRows(comp_header, matched_row_idxs_for_current_buffer,
                        output_column_idxs, output_eid,
                        *(output.GetFilteredChunk().get()));
        copyMatchedRows(comp_header, matched_row_idxs_for_next_buffer,
                        output_column_idxs, output_eid,
                        *(output.GetNextFilteredChunk().get()));
        return true;
    }
    else {
        copyMatchedRows(comp_header, matched_row_idxs, output_column_idxs,
                        output_eid, *(output.GetFilteredChunk().get()));
        return false;
    }
}

void ExtentIterator::copyMatchedRows(CompressionHeader &comp_header,
                                     vector<idx_t> &matched_row_idxs,
                                     vector<idx_t> &output_column_idxs,
                                     ExtentID &output_eid, DataChunk &output)
{
    size_t current_size = output.size();
    output.SetCardinality(current_size + matched_row_idxs.size());
    if (matched_row_idxs.size() == 0)
        return;

    for (size_t i = 0; i < output_column_idxs.size(); i++) {
        if (ext_property_type[i].id() == LogicalTypeId::SQLNULL)
            continue;

        auto output_idx = output_column_idxs[i];
        bool has_null = false;
        ValidityMask src_validity;

        if (ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i],
                   CompressionHeader::GetSizeWoBitSet());
            if (comp_header.HasNullMask()) {
                has_null = true;
                size_t bitmap_ptr_offset = comp_header.GetNullBitmapOffset();
                src_validity =
                    ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][i] +
                                              bitmap_ptr_offset));
            }
        }
        auto &validity = FlatVector::Validity(output.data[output_idx]);
        auto comp_header_valid_size = comp_header.GetSizeWoBitSet();
        if (ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);  // See previous commits for the implementation
            }
            else {
                auto strings =
                    FlatVector::GetData<string_t>(output.data[output_idx]);
                string_t *varchar_arr =
                    (string_t *)(io_requested_buf_ptrs[toggle][i] +
                                 comp_header_valid_size);
                if (has_null) {
                    for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                        idx_t seqno = matched_row_idxs[idx];
                        if (src_validity.RowIsValid(seqno)) {
                            strings[idx + current_size] = varchar_arr[seqno];
                        }
                        else {
                            validity.SetInvalid(idx + current_size);
                        }
                    }
                }
                else {
                    for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                        idx_t seqno = matched_row_idxs[idx];
                        strings[idx + current_size] = varchar_arr[seqno];
                    }
                }
            }
        }
        else if (ext_property_type[i].id() == LogicalTypeId::LIST) {
            D_ASSERT(false);
        }
        else if (ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST ||
                 ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
            D_ASSERT(false);
        }
        else if (ext_property_type[i].id() == LogicalTypeId::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column = (idx_t *)output.data[output_idx].GetData();
            for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                idx_t seqno = matched_row_idxs[idx];
                id_column[idx + current_size] = physical_id_base + seqno;
            }
        }
        else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);  // See previous commits for the implementation
            }
            else {
                size_t type_size =
                    GetTypeIdSize(ext_property_type[i].InternalType());
                if (has_null) {
                    for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                        idx_t seqno = matched_row_idxs[idx];
                        if (src_validity.RowIsValid(seqno)) {
                            memcpy(output.data[output_idx].GetData() +
                                       (idx + current_size) * type_size,
                                   io_requested_buf_ptrs[toggle][i] +
                                       comp_header_valid_size +
                                       seqno * type_size,
                                   type_size);
                        }
                        else {
                            validity.SetInvalid(idx + current_size);
                        }
                    }
                }
                else {
                    for (idx_t idx = 0; idx < matched_row_idxs.size(); idx++) {
                        idx_t seqno = matched_row_idxs[idx];
                        memcpy(output.data[output_idx].GetData() +
                                   (idx + current_size) * type_size,
                               io_requested_buf_ptrs[toggle][i] +
                                   comp_header_valid_size + seqno * type_size,
                               type_size);
                    }
                }
            }
        }
    }
}

void ExtentIterator::referenceRows(DataChunk &output, ExtentID output_eid,
                                   size_t scan_size,
                                   vector<idx_t> &output_column_idxs,
                                   idx_t scan_begin_offset,
                                   idx_t scan_end_offset)
{
    CompressionHeader comp_header;

    idx_t j = 0;
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        if ((ext_property_type[i] != LogicalType::ID) &&
            (io_requested_cdf_ids[toggle][i] ==
             std::numeric_limits<ChunkDefinitionID>::max())) {
            j++;  // TODO if we do not want to map output to universal schema, remove this code
            continue;
        }
        if (ext_property_type[i] != LogicalType::ID) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][i],
                   CompressionHeader::GetSizeWoBitSet());
            if (comp_header.HasNullMask()) {
                auto &validity =
                    FlatVector::Validity(output.data[output_column_idxs[j]]);
                size_t bitmap_ptr_offset =
                    comp_header.GetNullBitmapOffset() + (scan_begin_offset / 8);
                validity =
                    ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][i] +
                                              bitmap_ptr_offset));
            }
        }

        auto comp_header_valid_size = comp_header.GetSizeWoBitSet();
        if (ext_property_type[i].id() == LogicalTypeId::VARCHAR) {
            if (comp_header.comp_type == DICTIONARY) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(DICTIONARY, p_type);
                decomp_func.DeCompress(
                    io_requested_buf_ptrs[toggle][i] + comp_header_valid_size,
                    io_requested_buf_sizes[toggle][i] - comp_header_valid_size,
                    output.data[output_column_idxs[j]], comp_header.data_len);
            }
            else {
                FlatVector::SetData(output.data[output_column_idxs[j]],
                                    io_requested_buf_ptrs[toggle][i] +
                                        comp_header_valid_size +
                                        sizeof(string_t) * scan_begin_offset);
            }
        }
        else if (ext_property_type[i].id() == LogicalTypeId::LIST) {
            size_t type_size = sizeof(list_entry_t);
            size_t data_array_offset = comp_header.data_len * type_size;
        }
        else if (ext_property_type[i].id() == LogicalTypeId::FORWARD_ADJLIST ||
                 ext_property_type[i].id() == LogicalTypeId::BACKWARD_ADJLIST) {
        }
        else if (ext_property_type[i].id() == LogicalTypeId::ID) {
            idx_t physical_id_base = (idx_t)output_eid;
            physical_id_base = physical_id_base << 32;
            idx_t *id_column =
                (idx_t *)output.data[output_column_idxs[j]].GetData();
            idx_t output_seqno = 0;
            for (size_t seqno = scan_begin_offset; seqno < scan_end_offset;
                 seqno++)
                id_column[output_seqno++] = physical_id_base + seqno;
        }
        else {
            if (comp_header.comp_type == BITPACKING) {
                D_ASSERT(false);
                PhysicalType p_type = ext_property_type[i].InternalType();
                DeCompressionFunction decomp_func(BITPACKING, p_type);
                decomp_func.DeCompress(
                    io_requested_buf_ptrs[toggle][i] + comp_header_valid_size,
                    io_requested_buf_sizes[toggle][i] - comp_header_valid_size,
                    output.data[output_column_idxs[j]], comp_header.data_len);
            }
            else {
                size_t type_size =
                    GetTypeIdSize(ext_property_type[i].InternalType());
                FlatVector::SetData(output.data[output_column_idxs[j]],
                                    io_requested_buf_ptrs[toggle][i] +
                                        comp_header_valid_size +
                                        scan_begin_offset * type_size);
            }
        }
        j++;
    }

    output.SetCardinality(getNumReferencedRows(scan_size));
}

void ExtentIterator::selVectorToRowIdxs(SelectionVector &sel, size_t sel_size,
                                        vector<idx_t> &row_idxs, idx_t offset)
{
    row_idxs.reserve(sel_size);
    for (size_t i = 0; i < sel_size; i++) {
        row_idxs.push_back(sel.get_index(i) + offset);
    }
}

void ExtentIterator::sliceFilteredRows(DataChunk &input, DataChunk &output,
                                       idx_t scan_start_offset,
                                       vector<idx_t> matched_row_idxs)
{
    SelectionVector sel(
        STANDARD_VECTOR_SIZE);  // TODO: remove this redundant selection vector declaration
    for (auto i = 0; i < matched_row_idxs.size(); i++) {
        D_ASSERT(matched_row_idxs[i] - scan_start_offset <
                 STANDARD_VECTOR_SIZE);
        sel.set_index(i, matched_row_idxs[i] - scan_start_offset);
    }
    output.Slice(input, sel, matched_row_idxs.size());
}

void ExtentIterator::sliceFilteredRows(DataChunk &input, DataChunk &output,
                                       SelectionVector &sel, size_t sel_size)
{
    output.Slice(input, sel, sel_size);
}

// For Seek Operator
// bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, ExtentID target_eid, idx_t target_seqno, bool is_output_chunk_initialized) {
//     D_ASSERT(false);
//     return true;
// }

// For Seek Operator - Bulk Mode
// bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
//                                    ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, vector<uint32_t> &output_column_idxs,
//                                    idx_t start_seqno, idx_t end_seqno, bool is_output_chunk_initialized)
// {
//     D_ASSERT(false); // deprecated
//     return true;
// }

// For Seek Operator - Bulk Mode + Target Seqnos
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output,
                                   ExtentID &output_eid, ExtentID target_eid,
                                   DataChunk &input, idx_t nodeColIdx,
                                   const vector<uint32_t> &output_column_idxs,
                                   vector<uint32_t> &target_seqnos,
                                   vector<idx_t> &cols_to_include,
                                   bool is_output_chunk_initialized)
{
    if (target_eid != current_eid) {
        if (!RequestNextIO(context, output, output_eid,
                           is_output_chunk_initialized))
            return false;
    }
    else {
        output_eid = current_eid;
    }

    idx_t prefetch_unit_size = 32;
    auto &cur_ext_property_type =
        ext_property_types[(*target_idx_per_eid)[current_idx]];
    CompressionHeader *comp_header;
    src_data_seqnos.clear();
    Vector &vids = input.data[nodeColIdx];
    switch (vids.GetVectorType()) {
        case VectorType::DICTIONARY_VECTOR: {
            uint64_t *vids_data = (uint64_t *)vids.GetData();
            auto &sel_vec = DictionaryVector::SelVector(vids);
            for (auto seqno_idx = 0; seqno_idx < target_seqnos.size();
                 seqno_idx++) {
                src_data_seqnos.push_back(
                    vids_data[sel_vec.get_index(target_seqnos[seqno_idx])] &
                    0x00000000FFFFFFFF);
            }
            break;
        }
        case VectorType::FLAT_VECTOR: {
            uint64_t *vids_data = (uint64_t *)vids.GetData();
            for (auto seqno_idx = 0; seqno_idx < target_seqnos.size();
                 seqno_idx++) {
                src_data_seqnos.push_back(vids_data[target_seqnos[seqno_idx]] &
                                          0x00000000FFFFFFFF);
            }
            break;
        }
    }

    for (size_t i = 0; i < cur_ext_property_type.size(); i++) {
        // cols to exclude is for filter seek optimization.
        if (std::find(cols_to_include.begin(), cols_to_include.end(),
                      output_column_idxs[i]) == cols_to_include.end())
            continue;
        bool has_null = false;
        ValidityMask src_validity;
        if (cur_ext_property_type[i] != LogicalType::ID) {
            // memcpy(&comp_header, io_requested_buf_ptrs[toggle][i], CompressionHeader::GetSizeWoBitSet());
            comp_header = (CompressionHeader *)io_requested_buf_ptrs[toggle][i];
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout,
                    "[Seek-Bulk2] Load Column %ld -> %ld, cdf %ld, size = %ld "
                    "%ld, io_req = %ld comp_type = %d -> %d, data_len = %ld, "
                    "target_seqnos.size() = %ld, %p -> %p\n",
                    i, output_column_idxs[i], io_requested_cdf_ids[toggle][i],
                    output.size(), comp_header.data_len,
                    io_requested_buf_sizes[toggle][i],
                    (int)comp_header.comp_type,
                    (int)cur_ext_property_type[i].id(), comp_header.data_len,
                    target_seqnos.size(), io_requested_buf_ptrs[toggle][i],
                    output.data[i].GetData());
#endif
            if (comp_header->HasNullMask()) {
                has_null = true;
                size_t bitmap_ptr_offset = comp_header->GetNullBitmapOffset();
                src_validity =
                    ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][i] +
                                              bitmap_ptr_offset));
            }
        }
        else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk2] Load Column %ld -> %ld\n", i,
                    output_column_idxs[i]);
#endif
        }
        auto comp_header_valid_size = comp_header->GetSizeWoBitSet();
        auto &validity =
            FlatVector::Validity(output.data[output_column_idxs[i]]);
        switch (cur_ext_property_type[i].id()) {
            case LogicalTypeId::VARCHAR: {
                if (comp_header->comp_type == DICTIONARY) {
                    D_ASSERT(false);
                    PhysicalType p_type =
                        cur_ext_property_type[i].InternalType();
                    DeCompressionFunction decomp_func(DICTIONARY, p_type);
                    decomp_func.DeCompress(io_requested_buf_ptrs[toggle][i] +
                                               comp_header_valid_size,
                                           io_requested_buf_sizes[toggle][i] -
                                               comp_header_valid_size,
                                           output.data[i],
                                           comp_header->data_len);
                }
                else {
                    auto strings = FlatVector::GetData<string_t>(
                        output.data[output_column_idxs[i]]);
                    string_t *varchar_arr =
                        (string_t *)(io_requested_buf_ptrs[toggle][i] +
                                     comp_header_valid_size);
                    Vector &vids = input.data[nodeColIdx];
                    switch (vids.GetVectorType()) {
                        case VectorType::DICTIONARY_VECTOR:
                        case VectorType::FLAT_VECTOR: {
                            if (has_null) {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        __builtin_prefetch(
                                            &varchar_arr[src_data_seqnos
                                                             [seqno_idx +
                                                              prefetch_idx]]);
                                    }
#endif
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        if (src_validity.RowIsValid(
                                                src_data_seqnos
                                                    [seqno_idx +
                                                     prefetch_idx])) {
                                            strings[target_seqnos
                                                        [seqno_idx +
                                                         prefetch_idx]] =
                                                varchar_arr[src_data_seqnos
                                                                [seqno_idx +
                                                                 prefetch_idx]];
                                        }
                                        else {
                                            validity.SetInvalid(
                                                target_seqnos[seqno_idx +
                                                              prefetch_idx]);
                                        }
                                    }
                                }
                            }
                            else {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        __builtin_prefetch(
                                            &varchar_arr[src_data_seqnos
                                                             [seqno_idx +
                                                              prefetch_idx]]);
                                    }
#endif
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        strings[target_seqnos[seqno_idx +
                                                              prefetch_idx]] =
                                            varchar_arr
                                                [src_data_seqnos[seqno_idx +
                                                                 prefetch_idx]];
                                    }
                                }
                            }
                            break;
                        }
                        case VectorType::CONSTANT_VECTOR: {
                            uint64_t vid = ((uint64_t *)vids.GetData())[0];
                            idx_t target_seqno = vid & 0x00000000FFFFFFFF;
                            idx_t seqno;
                            if (has_null) {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx++) {
                                    if (src_validity.RowIsValid(seqno_idx)) {
                                        strings[target_seqnos[seqno_idx]] =
                                            varchar_arr[target_seqno];
                                    }
                                    else {
                                        validity.SetInvalid(
                                            target_seqnos[seqno_idx]);
                                    }
                                }
                            }
                            else {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx++) {
                                    strings[target_seqnos[seqno_idx]] =
                                        varchar_arr[target_seqno];
                                }
                            }
                            break;
                        }
                        default: {
                            D_ASSERT(false);
                        }
                    }
                }
                break;
            }
            case LogicalTypeId::LIST: {
                size_t type_size = sizeof(list_entry_t);
                size_t offset_array_size = comp_header->data_len * type_size;

                D_ASSERT(
                    false); /* zero copy for LIST is not implemented in this function */
                break;
            }
            case LogicalTypeId::FORWARD_ADJLIST:
            case LogicalTypeId::BACKWARD_ADJLIST: {
                // TODO
                break;
            }
            case LogicalTypeId::ID: {
                Vector &vids = input.data[nodeColIdx];
                idx_t physical_id_base = (idx_t)output_eid;
                physical_id_base = physical_id_base << 32;
                idx_t *id_column =
                    (idx_t *)output.data[output_column_idxs[i]].GetData();
                switch (vids.GetVectorType()) {
                    case VectorType::DICTIONARY_VECTOR: {
                        for (auto seqno_idx = 0;
                             seqno_idx < target_seqnos.size(); seqno_idx++) {
                            id_column[target_seqnos[seqno_idx]] =
                                physical_id_base + src_data_seqnos[seqno_idx];
                            // D_ASSERT(id_column[target_seqnos[seqno_idx]] == getIdRefFromVectorTemp(vids, target_seqnos[seqno_idx]));
                        }
                        break;
                    }
                    case VectorType::FLAT_VECTOR: {
                        for (auto seqno_idx = 0;
                             seqno_idx < target_seqnos.size(); seqno_idx++) {
                            id_column[target_seqnos[seqno_idx]] =
                                physical_id_base + src_data_seqnos[seqno_idx];
                            // D_ASSERT(id_column[target_seqnos[seqno_idx]] == getIdRefFromVectorTemp(vids, target_seqnos[seqno_idx]));
                        }
                        break;
                    }
                    case VectorType::CONSTANT_VECTOR: {
                        uint64_t vid = ((uint64_t *)vids.GetData())[0];
                        idx_t target_seqno = vid & 0x00000000FFFFFFFF;
                        for (auto seqno_idx = 0;
                             seqno_idx < target_seqnos.size(); seqno_idx++) {
                            id_column[target_seqnos[seqno_idx]] =
                                physical_id_base + target_seqno;
                            // D_ASSERT(id_column[target_seqnos[seqno_idx]] == getIdRefFromVectorTemp(vids, target_seqnos[seqno_idx]));
                        }
                        break;
                    }
                    default: {
                        D_ASSERT(false);
                    }
                }
                break;
            }
            default: {
                size_t type_size =
                    GetTypeIdSize(cur_ext_property_type[i].InternalType());
                Vector &vids = input.data[nodeColIdx];
                auto target_ptr = output.data[output_column_idxs[i]].GetData();
                switch (vids.GetVectorType()) {
                    case VectorType::DICTIONARY_VECTOR:
                    case VectorType::FLAT_VECTOR: {
                        if (has_null) {
                            for (auto seqno_idx = 0;
                                 seqno_idx < target_seqnos.size();
                                 seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                for (auto prefetch_idx = 0;
                                     prefetch_idx < prefetch_unit_size &&
                                     seqno_idx + prefetch_idx <
                                         target_seqnos.size();
                                     prefetch_idx++) {
                                    __builtin_prefetch(
                                        (char *)(io_requested_buf_ptrs[toggle]
                                                                      [i] +
                                                 comp_header_valid_size +
                                                 src_data_seqnos[seqno_idx +
                                                                 prefetch_idx] *
                                                     type_size));
                                }
#endif
                                for (auto prefetch_idx = 0;
                                     prefetch_idx < prefetch_unit_size &&
                                     seqno_idx + prefetch_idx <
                                         target_seqnos.size();
                                     prefetch_idx++) {
                                    if (src_validity.RowIsValid(
                                            src_data_seqnos[seqno_idx +
                                                            prefetch_idx])) {
                                        memcpy(
                                            target_ptr +
                                                target_seqnos[seqno_idx +
                                                              prefetch_idx] *
                                                    type_size,
                                            io_requested_buf_ptrs[toggle][i] +
                                                comp_header_valid_size +
                                                src_data_seqnos[seqno_idx +
                                                                prefetch_idx] *
                                                    type_size,
                                            type_size);
                                    }
                                    else {
                                        validity.SetInvalid(
                                            target_seqnos[seqno_idx +
                                                          prefetch_idx]);
                                    }
                                }
                            }
                        }
                        else {
                            for (auto seqno_idx = 0;
                                 seqno_idx < target_seqnos.size();
                                 seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                for (auto prefetch_idx = 0;
                                     prefetch_idx < prefetch_unit_size &&
                                     seqno_idx + prefetch_idx <
                                         target_seqnos.size();
                                     prefetch_idx++) {
                                    __builtin_prefetch(
                                        (char *)(io_requested_buf_ptrs[toggle]
                                                                      [i] +
                                                 comp_header_valid_size +
                                                 src_data_seqnos[seqno_idx +
                                                                 prefetch_idx] *
                                                     type_size));
                                }
#endif
                                for (auto prefetch_idx = 0;
                                     prefetch_idx < prefetch_unit_size &&
                                     seqno_idx + prefetch_idx <
                                         target_seqnos.size();
                                     prefetch_idx++) {
                                    memcpy(target_ptr +
                                               target_seqnos[seqno_idx +
                                                             prefetch_idx] *
                                                   type_size,
                                           io_requested_buf_ptrs[toggle][i] +
                                               comp_header_valid_size +
                                               src_data_seqnos[seqno_idx +
                                                               prefetch_idx] *
                                                   type_size,
                                           type_size);
                                }
                            }
                        }
                        break;
                    }
                    case VectorType::CONSTANT_VECTOR: {
                        uint64_t vid = ((uint64_t *)vids.GetData())[0];
                        idx_t target_seqno = vid & 0x00000000FFFFFFFF;
                        if (has_null) {
                            for (auto seqno_idx = 0;
                                 seqno_idx < target_seqnos.size();
                                 seqno_idx++) {
                                if (src_validity.RowIsValid(seqno_idx)) {
                                    memcpy(
                                        target_ptr + target_seqnos[seqno_idx] *
                                                         type_size,
                                        io_requested_buf_ptrs[toggle][i] +
                                            comp_header_valid_size +
                                            target_seqno * type_size,
                                        type_size);
                                }
                                else {
                                    validity.SetInvalid(
                                        target_seqnos[seqno_idx]);
                                }
                            }
                        }
                        else {
                            for (auto seqno_idx = 0;
                                 seqno_idx < target_seqnos.size();
                                 seqno_idx++) {
                                memcpy(target_ptr +
                                           target_seqnos[seqno_idx] * type_size,
                                       io_requested_buf_ptrs[toggle][i] +
                                           comp_header_valid_size +
                                           target_seqno * type_size,
                                       type_size);
                            }
                        }
                        break;
                    }
                    default: {
                        D_ASSERT(false);
                    }
                }
            }  // default
        }  // switch
    }  // for loop
    return true;
}

// For Seek Operator - Bulk Mode + Target Seqnos
bool ExtentIterator::GetNextExtentInRowFormat(
    ClientContext &context, DataChunk &output, ExtentID &output_eid,
    ExtentID target_eid, DataChunk &input, idx_t nodeColIdx,
    const vector<uint32_t> &output_column_idxs, Vector &rowcol_vec,
    char *row_major_store, vector<uint32_t> &target_seqnos,
    idx_t out_id_col_idx, idx_t &num_output_tuples,
    bool is_output_chunk_initialized)
{
    if (target_eid != current_eid) {
        if (!RequestNextIO(context, output, output_eid,
                           is_output_chunk_initialized))
            return false;
    }
    else {
        output_eid = current_eid;
    }

    auto &cur_ext_property_type =
        ext_property_types[(*target_idx_per_eid)[current_idx]];
    CompressionHeader comp_header;
    auto comp_header_valid_size = comp_header.GetSizeWoBitSet();

    // at this point, rowcol_arr is not a dictionary
    rowcol_t *rowcol_arr = (rowcol_t *)rowcol_vec.GetData();
    Vector &vids = input.data[nodeColIdx];
    idx_t physical_id_base = (idx_t)output_eid;
    physical_id_base = physical_id_base << 32;

    vector<size_t> type_sizes;
    for (auto i = 0; i < cur_ext_property_type.size(); i++) {
        size_t type_size =
            GetTypeIdSize(cur_ext_property_type[i].InternalType());
        type_sizes.push_back(type_size);
    }

    // Null handling
    // @jhha: this implementation is not discusssed with tslee
    // Though we used row format, we still need to handle null values
    // Therefore, we use the null bitmap as ususal, but this may not efficient
    ValidityMask src_validities[cur_ext_property_type.size()];
    for (size_t i = 0; i < cur_ext_property_type.size(); i++) {
        if (cur_ext_property_type[i] != LogicalType::ID) {
            auto comp_header =
                (CompressionHeader *)io_requested_buf_ptrs[toggle][i];
            if (comp_header->HasNullMask()) {
                size_t bitmap_ptr_offset = comp_header->GetNullBitmapOffset();
                src_validities[i] =
                    ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][i] +
                                              bitmap_ptr_offset));
            }
        }
    }

    idx_t id_col_value;
    for (auto i = 0; i < target_seqnos.size(); i++) {
        idx_t accumulated_bytes = 0;
        idx_t seqno = target_seqnos[i];
        idx_t target_seqno =
            getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
        for (auto j = 0; j < cur_ext_property_type.size(); j++) {
            ValidityMask &src_validity = src_validities[j];
            auto &validity =
                FlatVector::Validity(output.data[output_column_idxs[j]]);

            // Do Seek
            switch (cur_ext_property_type[j].id()) {
                case LogicalTypeId::VARCHAR: {
                    string_t *varchar_arr =
                        (string_t *)(io_requested_buf_ptrs[toggle][j] +
                                     comp_header_valid_size);
                    if (src_validity.RowIsValid(target_seqno)) {
                        memcpy(row_major_store + rowcol_arr[seqno].offset +
                                   accumulated_bytes,
                               &varchar_arr[target_seqno], sizeof(string_t));
                    }
                    else {
                        validity.SetInvalid(seqno);
                    }
                    accumulated_bytes += sizeof(string_t);
                    break;
                }
                case LogicalTypeId::LIST:
                    throw NotImplementedException(
                        "GetNextExtentInRowFormat LIST");
                    break;
                case LogicalTypeId::ID: {
                    // // ID column does not have null, thus no need to be row format
                    Vector &out_id_vec = output.data[out_id_col_idx];
                    D_ASSERT(out_id_vec.GetVectorType() ==
                             VectorType::FLAT_VECTOR);
                    idx_t *id_column = (idx_t *)out_id_vec.GetData();
                    id_col_value = physical_id_base + target_seqno;
                    id_column[seqno] = id_col_value;
                    break;
                }
                default: {
                    if (src_validity.RowIsValid(target_seqno)) {
                        memcpy(row_major_store + rowcol_arr[seqno].offset +
                                   accumulated_bytes,
                               io_requested_buf_ptrs[toggle][j] +
                                   comp_header_valid_size +
                                   target_seqno * type_sizes[j],
                               type_sizes[j]);
                    }
                    else {
                        validity.SetInvalid(seqno);
                    }
                    accumulated_bytes += type_sizes[j];
                    break;
                }
            }
        }
        num_output_tuples++;
    }

    return true;
}

// For Seek Operator - Bulk Mode + Target Seqnos
bool ExtentIterator::GetNextExtent(ClientContext &context, DataChunk &output,
                                   ExtentID &output_eid, ExtentID target_eid,
                                   DataChunk &input, idx_t nodeColIdx,
                                   const vector<uint32_t> &output_column_idxs,
                                   vector<uint32_t> &target_seqnos,
                                   vector<idx_t> &cols_to_include,
                                   idx_t &output_seqno,
                                   bool is_output_chunk_initialized)
{

    if (target_eid != current_eid) {
        if (!RequestNextIO(context, output, output_eid,
                           is_output_chunk_initialized))
            return false;
    }
    else {
        output_eid = current_eid;
    }

    idx_t prefetch_unit_size = 32;
    auto &cur_ext_property_type =
        ext_property_types[(*target_idx_per_eid)[current_idx]];
    CompressionHeader *comp_header;
    src_data_seqnos.clear();
    Vector &vids = input.data[nodeColIdx];
    switch (vids.GetVectorType()) {
        case VectorType::DICTIONARY_VECTOR: {
            uint64_t *vids_data = (uint64_t *)vids.GetData();
            auto sel_vec = DictionaryVector::SelVector(vids);
            for (auto seqno_idx = 0; seqno_idx < target_seqnos.size();
                 seqno_idx++) {
                src_data_seqnos.push_back(
                    vids_data[sel_vec.get_index(target_seqnos[seqno_idx])] &
                    0x00000000FFFFFFFF);
            }
            break;
        }
        case VectorType::FLAT_VECTOR: {
            uint64_t *vids_data = (uint64_t *)vids.GetData();
            for (auto seqno_idx = 0; seqno_idx < target_seqnos.size();
                 seqno_idx++) {
                src_data_seqnos.push_back(vids_data[target_seqnos[seqno_idx]] &
                                          0x00000000FFFFFFFF);
            }
            break;
        }
    }
    idx_t begin_seqno = output_seqno;
    for (size_t i = 0; i < cur_ext_property_type.size(); i++) {
        if (std::find(cols_to_include.begin(), cols_to_include.end(),
                      output_column_idxs[i]) == cols_to_include.end())
            continue;
        D_ASSERT(cur_ext_property_type[i] ==
                 output.data[output_column_idxs[i]].GetType());

        output_seqno = begin_seqno;
        bool has_null = false;
        ValidityMask src_validity;

        if (cur_ext_property_type[i] != LogicalType::ID) {
            comp_header = (CompressionHeader *)io_requested_buf_ptrs[toggle][i];
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout,
                    "[Seek-Bulk2] Load Column %ld -> %ld, cdf %ld, size = %ld "
                    "%ld, io_req = %ld comp_type = %d -> %d, data_len = %ld, "
                    "target_seqnos.size() = %ld, %p -> %p\n",
                    i, output_column_idxs[i], io_requested_cdf_ids[toggle][i],
                    output.size(), comp_header->data_len,
                    io_requested_buf_sizes[toggle][i],
                    (int)comp_header->comp_type,
                    (int)cur_ext_property_type[i].id(), comp_header->data_len,
                    target_seqnos.size(), io_requested_buf_ptrs[toggle][i],
                    output.data[i].GetData());
#endif
            if (comp_header->HasNullMask()) {
                has_null = true;
                size_t bitmap_ptr_offset = comp_header->GetNullBitmapOffset();
                src_validity =
                    ValidityMask((uint64_t *)(io_requested_buf_ptrs[toggle][i] +
                                              bitmap_ptr_offset));
            }
        }
        else {
#ifdef DEBUG_LOAD_COLUMN
            fprintf(stdout, "[Seek-Bulk2] Load Column %ld -> %ld\n", i,
                    output_column_idxs[i]);
#endif
        }
        auto &validity =
            FlatVector::Validity(output.data[output_column_idxs[i]]);
        auto comp_header_valid_size = comp_header->GetSizeWoBitSet();
        switch (cur_ext_property_type[i].id()) {
            case LogicalTypeId::VARCHAR: {
                if (comp_header->comp_type == DICTIONARY) {
                    D_ASSERT(false);
                }
                else {
                    auto strings = FlatVector::GetData<string_t>(
                        output.data[output_column_idxs[i]]);
                    string_t *varchar_arr =
                        (string_t *)(io_requested_buf_ptrs[toggle][i] +
                                     comp_header_valid_size);
                    Vector &vids = input.data[nodeColIdx];
                    switch (vids.GetVectorType()) {
                        case VectorType::DICTIONARY_VECTOR:
                        case VectorType::FLAT_VECTOR: {
                            if (has_null) {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        __builtin_prefetch(
                                            &varchar_arr[src_data_seqnos
                                                             [seqno_idx +
                                                              prefetch_idx]]);
                                    }
#endif
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        if (src_validity.RowIsValid(
                                                src_data_seqnos
                                                    [seqno_idx +
                                                     prefetch_idx])) {
                                            strings[output_seqno++] =
                                                varchar_arr[src_data_seqnos
                                                                [seqno_idx +
                                                                 prefetch_idx]];
                                        }
                                        else {
                                            validity.SetInvalid(output_seqno++);
                                        }
                                    }
                                }
                            }
                            else {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        __builtin_prefetch(
                                            &varchar_arr[src_data_seqnos
                                                             [seqno_idx +
                                                              prefetch_idx]]);
                                    }
#endif
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        strings[output_seqno++] = varchar_arr
                                            [src_data_seqnos[seqno_idx +
                                                             prefetch_idx]];
                                    }
                                }
                            }
                            break;
                        }
                        case VectorType::CONSTANT_VECTOR: {
                            uint64_t vid = ((uint64_t *)vids.GetData())[0];
                            idx_t target_seqno = vid & 0x00000000FFFFFFFF;
                            if (has_null) {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx++) {
                                    if (src_validity.RowIsValid(target_seqno)) {
                                        strings[output_seqno++] =
                                            varchar_arr[target_seqno];
                                    }
                                    else {
                                        validity.SetInvalid(output_seqno++);
                                    }
                                }
                            }
                            else {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx++) {
                                    strings[output_seqno++] =
                                        varchar_arr[target_seqno];
                                }
                            }
                            break;
                        }
                        default: {
                            D_ASSERT(false);
                        }
                    }
                }
                break;
            }
            case LogicalTypeId::LIST: {
                size_t type_size = sizeof(list_entry_t);
                size_t offset_array_size = comp_header->data_len * type_size;

                D_ASSERT(
                    false); /* zero copy for LIST is not implemented in this function */
                break;
            }
            case LogicalTypeId::FORWARD_ADJLIST:
            case LogicalTypeId::BACKWARD_ADJLIST: {
                // TODO
                break;
            }
            case LogicalTypeId::ID: {
                Vector &vids = input.data[nodeColIdx];
                idx_t physical_id_base = (idx_t)output_eid;
                physical_id_base = physical_id_base << 32;
                idx_t *id_column =
                    (idx_t *)output.data[output_column_idxs[i]].GetData();
                switch (vids.GetVectorType()) {
                    case VectorType::DICTIONARY_VECTOR:
                    case VectorType::FLAT_VECTOR: {
                        for (auto seqno_idx = 0;
                             seqno_idx < target_seqnos.size(); seqno_idx++) {
                            id_column[output_seqno] =
                                physical_id_base + src_data_seqnos[seqno_idx];
                            // D_ASSERT(id_column[output_seqno] == getIdRefFromVectorTemp(vids, seqno));
                            output_seqno++;
                        }
                        break;
                    }
                    case VectorType::CONSTANT_VECTOR: {
                        uint64_t vid = ((uint64_t *)vids.GetData())[0];
                        idx_t target_seqno = vid & 0x00000000FFFFFFFF;
                        for (auto seqno_idx = 0;
                             seqno_idx < target_seqnos.size(); seqno_idx++) {
                            id_column[output_seqno] =
                                physical_id_base + target_seqno;
                            // D_ASSERT(id_column[output_seqno] == getIdRefFromVectorTemp(vids, seqno));
                            output_seqno++;
                        }
                        break;
                    }
                    default: {
                        D_ASSERT(false);
                    }
                }
                break;
            }
            default: {
                if (comp_header->comp_type == BITPACKING) {
                    D_ASSERT(false);
                }
                else {
                    size_t type_size =
                        GetTypeIdSize(cur_ext_property_type[i].InternalType());
                    Vector &vids = input.data[nodeColIdx];
                    auto target_ptr =
                        output.data[output_column_idxs[i]].GetData();
                    switch (vids.GetVectorType()) {
                        case VectorType::DICTIONARY_VECTOR:
                        case VectorType::FLAT_VECTOR: {
                            if (has_null) {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        __builtin_prefetch((
                                            char
                                                *)(io_requested_buf_ptrs[toggle]
                                                                        [i] +
                                                   comp_header_valid_size +
                                                   src_data_seqnos
                                                           [seqno_idx +
                                                            prefetch_idx] *
                                                       type_size));
                                    }
#endif
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        if (src_validity.RowIsValid(
                                                src_data_seqnos
                                                    [seqno_idx +
                                                     prefetch_idx])) {
                                            memcpy(target_ptr +
                                                       output_seqno * type_size,
                                                   io_requested_buf_ptrs[toggle]
                                                                        [i] +
                                                       comp_header_valid_size +
                                                       src_data_seqnos
                                                               [seqno_idx +
                                                                prefetch_idx] *
                                                           type_size,
                                                   type_size);
                                            output_seqno++;
                                        }
                                        else {
                                            validity.SetInvalid(output_seqno++);
                                        }
                                    }
                                }
                            }
                            else {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx += prefetch_unit_size) {
#ifdef DO_PREFETCH_FOR_SEEK
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        __builtin_prefetch((
                                            char
                                                *)(io_requested_buf_ptrs[toggle]
                                                                        [i] +
                                                   comp_header_valid_size +
                                                   src_data_seqnos
                                                           [seqno_idx +
                                                            prefetch_idx] *
                                                       type_size));
                                    }
#endif
                                    for (auto prefetch_idx = 0;
                                         prefetch_idx < prefetch_unit_size &&
                                         seqno_idx + prefetch_idx <
                                             target_seqnos.size();
                                         prefetch_idx++) {
                                        memcpy(
                                            target_ptr +
                                                output_seqno * type_size,
                                            io_requested_buf_ptrs[toggle][i] +
                                                comp_header_valid_size +
                                                src_data_seqnos[seqno_idx +
                                                                prefetch_idx] *
                                                    type_size,
                                            type_size);
                                        output_seqno++;
                                    }
                                }
                            }
                            break;
                        }
                        case VectorType::CONSTANT_VECTOR: {
                            uint64_t vid = ((uint64_t *)vids.GetData())[0];
                            idx_t target_seqno = vid & 0x00000000FFFFFFFF;
                            if (has_null) {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx++) {
                                    if (src_validity.RowIsValid(target_seqno)) {
                                        memcpy(
                                            target_ptr +
                                                output_seqno * type_size,
                                            io_requested_buf_ptrs[toggle][i] +
                                                comp_header_valid_size +
                                                target_seqno * type_size,
                                            type_size);
                                        output_seqno++;
                                    }
                                    else {
                                        validity.SetInvalid(output_seqno++);
                                    }
                                }
                            }
                            else {
                                for (auto seqno_idx = 0;
                                     seqno_idx < target_seqnos.size();
                                     seqno_idx++) {
                                    memcpy(
                                        target_ptr + output_seqno * type_size,
                                        io_requested_buf_ptrs[toggle][i] +
                                            comp_header_valid_size +
                                            target_seqno * type_size,
                                        type_size);
                                    output_seqno++;
                                }
                            }
                            break;
                        }
                        default: {
                            D_ASSERT(false);
                        }
                    }
                }
            }
        }
    }
    output_seqno = begin_seqno + target_seqnos.size();
    return true;
}

// For AdjList
bool ExtentIterator::GetExtent(data_ptr_t &chunk_ptr, int target_toggle,
                               bool is_initialized)
{
    D_ASSERT(ext_property_type[0] == LogicalType::FORWARD_ADJLIST ||
             ext_property_type[0] ==
                 LogicalType::BACKWARD_ADJLIST);  // Only for ADJLIIST now..
    // Keep previous values
    int prev_toggle = toggle;
    if (current_idx > max_idx)
        return false;

    // Request chunk cache manager to finalize I/O
    if (!is_initialized) {  // We don't need I/O actually..
        for (int i = 0; i < io_requested_cdf_ids[target_toggle].size(); i++) {
            if (io_requested_cdf_ids[target_toggle][i] ==
                std::numeric_limits<ChunkDefinitionID>::max())
                continue;
            ChunkCacheManager::ccm->FinalizeIO(
                io_requested_cdf_ids[target_toggle][i], true, false);
        }
    }

    D_ASSERT(ext_property_type.size() == 1);
    for (size_t i = 0; i < ext_property_type.size(); i++) {
        chunk_ptr = (data_ptr_t)(io_requested_buf_ptrs[target_toggle][i] +
                                 CompressionHeader::GetSizeWoBitSet());
    }
    return true;
}

bool ExtentIterator::_CheckIsMemoryEnough()
{
    // TODO check memory.. if possible, use double buffering
    // Maybe this code is useless. Leave it to BFM
    bool enough = true;

    return enough;
}

bool ExtentIterator::ObtainFromCache(ExtentID &eid, int buf_idx)
{
    if (io_cache == nullptr)
        return false;
    uint16_t seq_no = GET_EXTENT_SEQNO_FROM_EID(eid);

    // double the size of the cache
    if (seq_no >= io_cache->io_buf_ptrs_cache.size()) {
        IncreaseCacheSize();
        return false;
    }

    // no cache found
    if (io_cache->io_buf_ptrs_cache[seq_no].size() == 0)
        return false;

    // copy the cache to the current buffer
    io_requested_cdf_ids[buf_idx] = io_cache->io_cdf_ids_cache[seq_no];
    io_requested_buf_ptrs[buf_idx] = io_cache->io_buf_ptrs_cache[seq_no];
    io_requested_buf_sizes[buf_idx] = io_cache->io_buf_sizes_cache[seq_no];
    num_tuples_in_current_extent[buf_idx] = io_cache->num_tuples_cache[seq_no];

    return true;
}

void ExtentIterator::PopulateCache(ExtentID &eid, int buf_idx)
{
    if (io_cache == nullptr)
        return;
    uint16_t seq_no = GET_EXTENT_SEQNO_FROM_EID(eid);

    // double the size of the cache
    while (seq_no >= io_cache->io_buf_ptrs_cache.size()) {
        IncreaseCacheSize();
    }
    // copy the current buffer to the cache
    io_cache->io_cdf_ids_cache[seq_no] = io_requested_cdf_ids[buf_idx];
    io_cache->io_buf_ptrs_cache[seq_no] = io_requested_buf_ptrs[buf_idx];
    io_cache->io_buf_sizes_cache[seq_no] = io_requested_buf_sizes[buf_idx];
    io_cache->num_tuples_cache[seq_no] = num_tuples_in_current_extent[buf_idx];
}

void ExtentIterator::IncreaseCacheSize()
{
    D_ASSERT(io_cache != nullptr);
    auto original_size = io_cache->io_buf_ptrs_cache.size();
    io_cache->io_buf_ptrs_cache.resize(original_size * 2);
    io_cache->io_buf_sizes_cache.resize(original_size * 2);
    io_cache->io_cdf_ids_cache.resize(original_size * 2);
    io_cache->num_tuples_cache.resize(original_size * 2);
}

namespace facebook::velox::dwio::common {

template <typename T>
struct NoHook {
  void addValues(
      const int32_t* /*rows*/,
      const T* /*values*/,
      int32_t /*size*/) {}
  void addValueTyped(
    const int32_t /*row*/, 
    const T /*value*/) {}
};

} 

template <typename T, typename TFilter>
void ExtentIterator::evalPredicateSIMD(Vector &column_vec, size_t data_len,
                                         std::unique_ptr<TFilter> &filter,
                                         idx_t scan_start_offset,
                                         idx_t scan_end_offset,
                                         vector<idx_t> &matched_row_idxs)
{
    int32_t num_values_output = 0;
    facebook::velox::dwio::common::NoHook<T> noHook;
    auto scan_length = scan_end_offset - scan_start_offset;
    raw_vector<int32_t> hits(STANDARD_VECTOR_SIZE);
    raw_vector<int32_t> rows(scan_length);
    std::iota(rows.begin(), rows.end(), scan_start_offset);

    auto data_size = data_len * sizeof(T);
    dwio::common::SeekableArrayInputStream input_stream(
        (const char *)FlatVector::GetData(column_vec), data_size);
    const char *bufferStart = (const char *)FlatVector::GetData(column_vec);
    const char *bufferEnd = bufferStart + data_size;

    auto ranged_rows = folly::Range<const int32_t *>(
        (const int32_t *)rows.data(), scan_length);

    auto validity_mask = column_vec.GetValidity();
    if (validity_mask.AllValid()) {
        dwio::common::fixedWidthScan<T, true, false>(
            ranged_rows, nullptr, nullptr, hits.data(), num_values_output,
            input_stream, bufferStart, bufferEnd, *filter, noHook);
    }
    else {
        raw_vector<int32_t> unfiltered_vec;
        vector<int32_t> filtered_vec;
        dwio::common::nonNullRowsFromDense((uint64_t *)validity_mask.GetData(),
                                           data_len, unfiltered_vec);
        std::copy_if(unfiltered_vec.begin(), unfiltered_vec.end(),
                     std::back_inserter(filtered_vec),
                     [scan_start_offset, scan_end_offset](int index) {
                         return index >= scan_start_offset &&
                                index < scan_end_offset;
                     });
        auto non_null_ranged_rows = folly::Range<const int32_t *>(
            filtered_vec.data(), filtered_vec.size());

        dwio::common::fixedWidthScan<T, true, false>(
            non_null_ranged_rows, nullptr, nullptr, hits.data(),
            num_values_output, input_stream, bufferStart, bufferEnd, *filter,
            noHook);
    }

    matched_row_idxs.reserve(num_values_output);
    for (int64_t i = 0; i < num_values_output; i++) {
        matched_row_idxs.push_back(static_cast<idx_t>(hits[i]));
    }
}

template void ExtentIterator::evalPredicateSIMD<int16_t, common::BigintRange>(
    Vector &, size_t, std::unique_ptr<common::BigintRange> &, idx_t, idx_t,
    vector<idx_t> &);
template void ExtentIterator::evalPredicateSIMD<int32_t, common::BigintRange>(
    Vector &, size_t, std::unique_ptr<common::BigintRange> &, idx_t, idx_t,
    vector<idx_t> &);
template void ExtentIterator::evalPredicateSIMD<int64_t, common::BigintRange>(
    Vector &, size_t, std::unique_ptr<common::BigintRange> &, idx_t, idx_t,
    vector<idx_t> &);
    
}  // namespace duckdb1