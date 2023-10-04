#ifndef EXTENT_ITERATOR_H
#define EXTENT_ITERATOR_H

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"

#include "common/types.hpp"
#include "common/vector_size.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/selection_vector.hpp"
#include "extent/compression/compression_function.hpp"
#include "extent/compression/compression_header.hpp"

#include <limits>

namespace duckdb {

inline uint64_t& getIdRefFromVectorTemp(Vector& vector, idx_t index) {
	switch( vector.GetVectorType() ) {
		case VectorType::DICTIONARY_VECTOR: {
			return ((uint64_t *)vector.GetData())[DictionaryVector::SelVector(vector).get_index(index)];
		}
		case VectorType::FLAT_VECTOR: {
			return ((uint64_t *)vector.GetData())[index];
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

class ExtentIterator {
public:
    ExtentIterator() {}
    ~ExtentIterator() {}

    // Iterate all extents related to the PropertySchemaCatalogEntry
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry);
    void Initialize(ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_,
                    vector<idx_t> &target_idxs_);
    void Initialize(ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid);
    void Initialize(ClientContext &context, vector<vector<LogicalType>> &target_types_, vector<vector<idx_t>> &target_idxs_,
                    vector<idx_t> &target_idx_per_eid_, vector<ExtentID> target_eids);
    int RequestNewIO(ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_,
                     ExtentID target_eid, ExtentID &evicted_eid);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid, vector<idx_t> &output_column_idxs,
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       int64_t &filterKeyColIdx, Value &filterValue, vector<idx_t> &output_column_idxs,
                       vector<duckdb::LogicalType> &scanSchema, size_t scan_size = EXEC_ENGINE_VECTOR_SIZE,
                       bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, idx_t target_seqno, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, vector<idx_t> &output_column_idxs,
                       idx_t start_seqno, idx_t end_seqno, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, vector<idx_t> &output_column_idxs,
                       vector<idx_t> &target_seqnos, bool is_output_chunk_initialized=true);
    bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       int64_t &filterKeyColIdx, Value &filterValue, ExtentID target_eid, DataChunk &input,
                       idx_t nodeColIdx, vector<idx_t> &output_column_idxs, vector<idx_t> &target_seqnos,
                       idx_t &cur_output_idx, SelectionVector &sel, bool is_output_chunk_initialized=true);
    // bool GetNextExtent(ClientContext &context, DataChunk &output, ExtentID &output_eid,
    //                    ExpressionExecutor &executor, ExtentID target_eid, DataChunk &input,
    //                    idx_t nodeColIdx, vector<idx_t> &output_column_idxs, vector<idx_t> &target_seqnos,
    //                    idx_t &cur_output_idx, SelectionVector &sel, bool is_output_chunk_initialized=true);
    bool GetExtent(data_ptr_t &chunk_ptr, int target_toggle, bool is_initialized);

    bool IsInitialized() {
        return is_initialized;
    }

private:
    bool _CheckIsMemoryEnough();

    void findRowsThatSatisfyPredicate(DataChunk &input, idx_t nodeColIdx, vector<idx_t> &target_seqnos,
                                             Value &filterValue, idx_t col_idx, idx_t scan_start_offset, idx_t scan_end_offset,
                                             vector<idx_t> &matched_row_idxs, SelectionVector &sel) {
        CompressionHeader comp_header;
        Vector &vids = input.data[nodeColIdx];
        auto &cur_ext_property_type = ext_property_types[target_idx_per_eid[current_idx]];
        if (cur_ext_property_type[col_idx] == LogicalType::VARCHAR) {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][col_idx], sizeof(CompressionHeader));
            // size_t comp_header_valid_size = comp_header.GetValidSize();
            size_t comp_header_valid_size = sizeof(CompressionHeader);
            if (comp_header.comp_type == DICTIONARY) {
                throw NotImplementedException("Filter predicate on DICTIONARY compression is not implemented yet");
            } else {
                size_t string_data_offset = comp_header_valid_size + comp_header.data_len * sizeof(uint64_t);
                uint64_t *offset_arr = (uint64_t *)(io_requested_buf_ptrs[toggle][col_idx] + comp_header_valid_size);
                uint64_t string_offset, prev_string_offset;
                for (auto seqno_idx = 0; seqno_idx < target_seqnos.size(); seqno_idx++) {
                    idx_t seqno = target_seqnos[seqno_idx];
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    if (target_seqno < scan_start_offset || target_seqno >= scan_end_offset) continue;
                    prev_string_offset = target_seqno == 0 ? 0 : offset_arr[target_seqno - 1];
                    string_offset = offset_arr[target_seqno];
                    string string_val((char*)(io_requested_buf_ptrs[toggle][col_idx] + string_data_offset + prev_string_offset), string_offset - prev_string_offset);
                    Value str_val(string_val);
                    if (str_val == filterValue) {
                        matched_row_idxs.push_back(seqno);
                    }
                }
            }
        } else if (cur_ext_property_type[col_idx] == LogicalType::FORWARD_ADJLIST || cur_ext_property_type[col_idx] == LogicalType::BACKWARD_ADJLIST) {
            throw InvalidInputException("Filter predicate on ADJLIST column");
        } else if (cur_ext_property_type[col_idx] == LogicalType::ID) {
            throw InvalidInputException("Filter predicate on PID column");
        } else {
            memcpy(&comp_header, io_requested_buf_ptrs[toggle][col_idx], sizeof(CompressionHeader));
            // size_t comp_header_valid_size = comp_header.GetValidSize();
            size_t comp_header_valid_size = sizeof(CompressionHeader);
            if (comp_header.comp_type == BITPACKING) {
                throw NotImplementedException("Filter predicate on BITPACKING compression is not implemented yet");
            } else {
                LogicalType column_type = cur_ext_property_type[col_idx];
                Vector column_vec(column_type, (data_ptr_t)(io_requested_buf_ptrs[toggle][col_idx] + comp_header_valid_size));
                for (auto seqno_idx = 0; seqno_idx < target_seqnos.size(); seqno_idx++) {
                    idx_t seqno = target_seqnos[seqno_idx];
                    idx_t target_seqno = getIdRefFromVectorTemp(vids, seqno) & 0x00000000FFFFFFFF;
                    if (target_seqno < scan_start_offset || target_seqno >= scan_end_offset) continue;
                    if (column_vec.GetValue(target_seqno) == filterValue) {
                        matched_row_idxs.push_back(seqno);
                    }
                }
            }
        }
    }

private:
    vector<ExtentID> ext_ids_to_iterate;
    DataChunk* data_chunks[MAX_NUM_DATA_CHUNKS];
    vector<ChunkDefinitionID> io_requested_cdf_ids[MAX_NUM_DATA_CHUNKS];
    vector<uint8_t*> io_requested_buf_ptrs[MAX_NUM_DATA_CHUNKS];
    vector<size_t> io_requested_buf_sizes[MAX_NUM_DATA_CHUNKS];
    size_t num_tuples_in_current_extent[MAX_NUM_DATA_CHUNKS];
    vector<LogicalType> ext_property_type;
    vector<vector<LogicalType>> ext_property_types;
    vector<idx_t> target_idx;
    vector<vector<idx_t>> target_idxs;
    vector<idx_t> target_idx_per_eid;
    idx_t current_idx_in_this_extent;
    idx_t current_idx;
    idx_t max_idx;
    ExtentID current_eid = (ExtentID)std::numeric_limits<uint32_t>::max();
    unordered_map<ExtentID, int> eid_to_bufptr_idx_map;
    int num_data_chunks;
    int toggle;
    int target_idxs_offset = 0;
    bool support_double_buffering;
    bool is_initialized = false;
    PropertySchemaCatalogEntry *ps_cat_entry;
};

} // namespace duckdb

#endif
