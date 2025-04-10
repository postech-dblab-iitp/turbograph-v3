#ifndef EXTENT_ITERATOR_H
#define EXTENT_ITERATOR_H

#include "common/typedef.hpp"
#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"
#include "common/types.hpp"
#include "common/vector_size.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/selection_vector.hpp"
#include "storage/extent/compression/compression_function.hpp"
#include "storage/extent/compression/compression_header.hpp"
#include "planner/expression.hpp"
#include "execution/expression_executor.hpp"
#include <limits>
#include <tuple>

namespace duckdb {

inline uint64_t& getIdRefFromVectorTemp(Vector& vector, idx_t index) {
	switch (vector.GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			return ((uint64_t *)vector.GetData())[DictionaryVector::SelVector(vector).get_index(index)];
		}
		case VectorType::FLAT_VECTOR: {
			return ((uint64_t *)vector.GetData())[index];
		}
        case VectorType::CONSTANT_VECTOR: {
            return ((uint64_t *)vector.GetData())[0];
        }
		default: {
			D_ASSERT(false);
		}
	}
}

class Value;
class ClientContext;
class PropertySchemaCatalogEntry;

// TODO currently, only support double buffering
// If possible, change this implementation to support prefetching
#define MAX_NUM_DATA_CHUNKS 2
#define FILTER_BUFFERING_THRESHOLD 0.1

typedef vector<uint8_t*> io_buf_ptrs;
typedef vector<size_t> io_buf_sizes;
typedef vector<ChunkDefinitionID> io_cdf_ids;
typedef size_t num_tuple;

typedef struct IOCache {
    vector<io_buf_ptrs> io_buf_ptrs_cache;
    vector<io_buf_sizes> io_buf_sizes_cache;
    vector<io_cdf_ids> io_cdf_ids_cache;
    vector<num_tuple> num_tuples_cache;
} IOCache;

class ExtentIterator {
public:
    // General constructor
    ExtentIterator(IOCache *io_cache_ = nullptr) : io_cache(io_cache_) {
        src_data_seqnos.reserve(STANDARD_VECTOR_SIZE);
    }
    // For seek
    ExtentIterator(vector<vector<LogicalType>>& _ext_property_types, vector<vector<idx_t>>& _target_idxs, IOCache *io_cache_ = nullptr) : 
        io_cache(io_cache_), ext_property_types(_ext_property_types), target_idxs(_target_idxs) {
        src_data_seqnos.reserve(STANDARD_VECTOR_SIZE);
    }
    ~ExtentIterator() {
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++) {
            delete data_chunks[i];
        }
    }

    // Iterate all extents related to the PropertySchemaCatalogEntry
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry);
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_,
                    vector<idx_t> &target_idxs_);
    void Initialize(ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid);
    void Initialize(ClientContext &context, vector<idx_t> *target_idx_per_eid_, vector<ExtentID> target_eids);
    int RequestNewIO(ClientContext &context, ExtentID target_eid, ExtentID &evicted_eid);
    bool RequestNextIO(ClientContext &context, DataChunk &output, ExtentID &output_eid, bool is_output_chunk_initialized);
    void Rewind();

    /* no filter pushdown */
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, vector<idx_t> &output_column_idxs,
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);

    /* filter pushdown */
    bool GetNextExtent(ClientContext &context, DataChunk &output, FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
                       int64_t &filterKeyColIdx, Value &filterValue, vector<idx_t> &output_column_idxs,
                       vector<duckdb::LogicalType> &scanSchema, size_t scan_size = EXEC_ENGINE_VECTOR_SIZE,
                       bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
                       int64_t &filterKeyColIdx, Value &lfilterValue, Value &rfilterValue, bool l_inclusive, bool r_inclusive,
                       vector<idx_t> &output_column_idxs, vector<duckdb::LogicalType> &scanSchema, 
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
                       ExpressionExecutor& executor, vector<idx_t> &output_column_idxs, vector<duckdb::LogicalType> &scanSchema, 
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);

    /* IdSeek */
    // bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
    //                    ExtentID target_eid, idx_t target_seqno, bool is_output_chunk_initialized=true);
    // bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
    //                    ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<idx_t> &output_column_idxs,
    //                    idx_t start_seqno, idx_t end_seqno, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<uint32_t> &output_column_idxs,
                       vector<uint32_t> &target_seqnos, vector<idx_t> &cols_to_include, bool is_output_chunk_initialized=true);
    bool GetNextExtentInRowFormat(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<uint32_t> &output_column_idxs,
                       Vector &rowcol_vec, char *row_major_store, vector<uint32_t> &target_seqnos, idx_t out_id_col_idx, 
                       idx_t &num_output_tuples, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<uint32_t> &output_column_idxs,
                       vector<uint32_t> &target_seqnos, vector<idx_t> &cols_to_include, idx_t &output_seqno, bool is_output_chunk_initialized=true);
    bool GetExtent(data_ptr_t &chunk_ptr, int target_toggle, bool is_initialized);

    /* Optimization */
    void IncreaseCacheSize();
    bool ObtainFromCache(ExtentID &eid, int buf_idx);
    void PopulateCache(ExtentID &eid, int buf_idx);
    bool IsRewinded() {
        return is_rewinded;
    }

    bool IsInitialized() {
        return is_initialized;
    }

    /* Filter Buffering */
    inline void enableFilterBuffering() {
        is_filter_buffering_enabled = true;
    }
    inline void disableFilterBuffering() {
        is_filter_buffering_enabled = false;
    }

private:
    bool _CheckIsMemoryEnough();

    template <typename T, typename TFilter>
    void evalPredicateSIMD(Vector& column_vec, size_t data_len, std::unique_ptr<TFilter>& filter, 
                            idx_t scan_start_offset, idx_t scan_end_offset, vector<idx_t>& matched_row_idxs);

    idx_t findColumnIdx(ChunkDefinitionID filter_cdf_id);
    ChunkDefinitionID getFilterCDFID(ExtentID output_eid, int64_t filterKeyColIdx);
    void requestIOForDoubleBuffering(ClientContext &context);
    void requestFinalizeIO();

    bool getScanRange(size_t scan_size, idx_t& scan_start_offset, idx_t& scan_end_offset);
    bool getScanRange(size_t scan_size, idx_t idx_in_extent, idx_t& scan_start_offset, idx_t& scan_end_offset);
    bool getScanRange(ClientContext &context, ChunkDefinitionID filter_cdf_id, Value &filterValue, 
                    size_t scan_size, idx_t& scan_start_offset, idx_t& scan_end_offset);
    bool getScanRange(ClientContext &context, ChunkDefinitionID filter_cdf_id, duckdb::Value &l_filterValue, duckdb::Value &r_filterValue, 
                    bool l_inclusive, bool r_inclusive, size_t scan_size, idx_t& scan_start_offset, idx_t& scan_end_offset);
    void selVectorToRowIdxs(SelectionVector& sel, size_t sel_size, vector<idx_t>& row_idxs, idx_t offset);
    void getValidOutputMask(vector<idx_t> &output_column_idxs, vector<bool>& valid_output_mask);
    void findMatchedRowsEQFilter(CompressionHeader& comp_header, idx_t col_idx, idx_t scan_start_offset, idx_t scan_end_offset,
                                Value &filterValue, vector<idx_t>& matched_row_idxs);
    void findMatchedRowsRangeFilter(CompressionHeader& comp_header, idx_t col_idx, idx_t scan_start_offset, idx_t scan_end_offset,
                                Value &l_filterValue, Value &r_filterValue, bool l_inclusive, bool r_inclusive, vector<idx_t>& matched_row_idxs);
    void referenceRows(DataChunk &output, ExtentID output_eid, size_t scan_size, vector<idx_t> &output_column_idxs, idx_t scan_begin_offset, idx_t scan_end_offset);
    bool copyMatchedRowsToBuffer(CompressionHeader& comp_header, vector<idx_t>& matched_row_idxs, vector<idx_t> &output_column_idxs, ExtentID &output_eid, FilteredChunkBuffer &output);
    void copyMatchedRows(CompressionHeader& comp_header, vector<idx_t>& matched_row_idxs, vector<idx_t> &output_column_idxs, ExtentID &output_eid, DataChunk &output);
    bool inclusiveAwareRangePredicateCheck(Value &l_filterValue, Value &r_filterValue, bool l_inclusive, bool r_inclusive, Value &filterValue);

    inline bool doFilterBuffer(size_t scan_size, size_t num_filtered_tuples) {
        if (!is_filter_buffering_enabled) {
            return false;
        }
        else {
            return (double)num_filtered_tuples / scan_size < FILTER_BUFFERING_THRESHOLD;
        }
    }

    inline size_t getNumReferencedRows(size_t scan_size) {
        size_t remain_data_size = num_tuples_in_current_extent[toggle] - (current_idx_in_this_extent * scan_size);
        return std::min((size_t) scan_size, remain_data_size);
    }

    void sliceFilteredRows(DataChunk& input, DataChunk &output, idx_t scan_start_offset, vector<idx_t> matched_row_idxs);
    void sliceFilteredRows(DataChunk& input, DataChunk &output, SelectionVector& sel, size_t sel_size);

private:
    vector<ExtentID> ext_ids_to_iterate;
    DataChunk* data_chunks[MAX_NUM_DATA_CHUNKS];
    io_cdf_ids io_requested_cdf_ids[MAX_NUM_DATA_CHUNKS];
    io_buf_ptrs io_requested_buf_ptrs[MAX_NUM_DATA_CHUNKS];
    io_buf_sizes io_requested_buf_sizes[MAX_NUM_DATA_CHUNKS];
    size_t num_tuples_in_current_extent[MAX_NUM_DATA_CHUNKS];
    vector<LogicalType> ext_property_type;
    vector<vector<LogicalType>> ext_property_types;
    vector<vector<idx_t>> target_idxs;
    vector<idx_t> target_idx;
    vector<idx_t>* target_idx_per_eid;
    vector<uint32_t> src_data_seqnos;
    idx_t current_idx_in_this_extent;
    idx_t current_idx;
    idx_t max_idx;
    ExtentID current_eid = (ExtentID)std::numeric_limits<uint32_t>::max();
    int num_data_chunks;
    int toggle;
    int target_idxs_offset = 0;
    bool support_double_buffering;
    bool is_initialized = false;
    bool is_rewinded = false;
    bool is_filter_buffering_enabled = true;
    PropertySchemaCatalogEntry *ps_cat_entry;

    // Optimization
    IOCache *io_cache;
};

} // namespace duckdb

#endif
