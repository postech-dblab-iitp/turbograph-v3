#ifndef EXTENT_ITERATOR_H
#define EXTENT_ITERATOR_H

#include "common/common.hpp"
#include "common/vector.hpp"

namespace duckdb {

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
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types, vector<idx_t> &target_idxs);

    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid);

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
    idx_t current_idx;
    idx_t max_idx;
    int num_data_chunks;
    int toggle;
    bool support_double_buffering;
};

} // namespace duckdb

#endif
