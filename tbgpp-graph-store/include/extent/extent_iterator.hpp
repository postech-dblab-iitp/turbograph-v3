#ifndef EXTENT_ITERATOR_H
#define EXTENT_ITERATOR_H

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"

#include "common/types.hpp"
#include "common/vector_size.hpp"

#include <limits>

namespace duckdb {

class Value;
class DataChunk;
class LogicalType;
class ClientContext;
class PropertySchemaCatalogEntry;

// TODO currently, only support double buffering
// If possible, change this implementation to support prefetching
#define MAX_NUM_DATA_CHUNKS 2

class ExtentIterator {
public:
    ExtentIterator() {}
    ~ExtentIterator() {}

    // Iterate all extents related to the PropertySchemaCatalogEntry
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry);
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_);
    // void Initialize(ClientContext &context, vector<PropertySchemaCatalogEntry *> &property_schema_cat_entry, vector<LogicalType> &target_types_, vector<vector<int64_t>> &target_idxs_); // TODO is generalizable? more general API..
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid);
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, vector<ExtentID> target_eids);
    int RequestNewIO(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid, ExtentID &evicted_eid);

    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, string filterKey, Value filterValue, vector<string> &output_properties, vector<duckdb::LogicalType> &scanSchema, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, ExtentID target_eid, idx_t target_seqno, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, vector<idx_t> output_col_idx, idx_t start_seqno, idx_t end_seqno, bool is_output_chunk_initialized=true);
    bool GetExtent(data_ptr_t &chunk_ptr, int target_toggle, bool is_initialized);

    bool IsInitialized() {
        return is_initialized;
    }

private:
    bool _CheckIsMemoryEnough();

private:
    vector<ExtentID> ext_ids_to_iterate;
    DataChunk* data_chunks[MAX_NUM_DATA_CHUNKS];
    vector<ChunkDefinitionID> io_requested_cdf_ids[MAX_NUM_DATA_CHUNKS];
    vector<uint8_t*> io_requested_buf_ptrs[MAX_NUM_DATA_CHUNKS];
    vector<size_t> io_requested_buf_sizes[MAX_NUM_DATA_CHUNKS];
    vector<LogicalType> ext_property_types;
    vector<idx_t> target_idxs;
    idx_t current_idx_in_this_extent;
    idx_t current_idx;
    idx_t max_idx;
    ExtentID current_eid = (ExtentID)std::numeric_limits<uint32_t>::max();
    unordered_map<ExtentID, int> eid_to_bufptr_idx_map;
    int num_data_chunks;
    int toggle;
    bool support_double_buffering;
    bool is_initialized = false;
    PropertySchemaCatalogEntry *ps_cat_entry;
};

class AdjacencyListIterator {
public:
    AdjacencyListIterator() {}
    ~AdjacencyListIterator() {}
    bool Initialize(ClientContext &context, int adjColIdx, ExtentID target_eid, LogicalType adjlist_type = LogicalType::FORWARD_ADJLIST);
    void Initialize(ClientContext &context, int adjColIdx, DataChunk &input, idx_t srcColIdx, LogicalType adjlist_type = LogicalType::FORWARD_ADJLIST);
    void getAdjListRange(uint64_t vid, uint64_t *start_idx, uint64_t *end_idx);
    void getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t *&start_ptr, uint64_t *&end_ptr, bool is_initialized);

private:
    bool is_initialized = false;
    ExtentIterator *ext_it = nullptr;
    ExtentID cur_eid = std::numeric_limits<ExtentID>::max();
    unordered_map<ExtentID, int> eid_to_bufptr_idx_map;
    data_ptr_t cur_adj_list;
};

} // namespace duckdb

#endif
