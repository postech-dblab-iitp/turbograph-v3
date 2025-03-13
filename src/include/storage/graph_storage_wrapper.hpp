#pragma once

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/filter_value.hpp"
#include "common/types/filtered_data_chunk.hpp"
#include "common/types/resizable_bool_vector.hpp"
#include "common/typedefs.hpp"
#include "common/types/expand_direction.hpp"
#include "execution/expression_executor.hpp"
#include "common/boost_typedefs.hpp"
#include "planner/expression.hpp"
#include <boost/timer/timer.hpp>
#include <queue>
#include <unordered_map>
#include <bitset>

#define END_OF_QUEUE nullptr

namespace duckdb {

class ExtentIterator;
class AdjacencyListIterator;
class ClientContext;
class IOCache;

enum class StoreAPIResult { OK, DONE, ERROR };

class GraphStorageWrapper {
public:
	GraphStorageWrapper(ClientContext &client);

public:
 //! Initialize Scan Operation
 StoreAPIResult InitializeScan(
     std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids,
     vector<vector<uint64_t>> &projection_mapping,
     vector<vector<duckdb::LogicalType>> &scanSchemas,
     bool enable_filter_buffering = true);

 //! Initialize Scan Operation
 StoreAPIResult InitializeScan(
     std::queue<ExtentIterator *> &ext_its, PropertySchemaID_vector *oids,
     vector<vector<uint64_t>> &projection_mapping,
     vector<vector<duckdb::LogicalType>> &scanSchemas,
     bool enable_filter_buffering = true);

 // Non filter
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       std::vector<duckdb::LogicalType> &scanSchema);
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx,
                       bool is_output_initialized = true);

 // Filter related
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       FilteredChunkBuffer &output_buffer,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx, int64_t &filterKeyColIdx,
                       duckdb::Value &filterValue);
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       FilteredChunkBuffer &output_buffer,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx, int64_t &filterKeyColIdx,
                       duckdb::RangeFilterValue &rangeFilterValue);
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       FilteredChunkBuffer &output_buffer,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx, ExpressionExecutor &expr);

 StoreAPIResult InitializeVertexIndexSeek(
     ExtentIterator *&ext_it, vector<vector<uint64_t>> &projection_mapping, DataChunk &input,
     idx_t nodeColIdx, vector<vector<LogicalType>> &scanSchemas,
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent,
     vector<idx_t> &mapping_idxs, vector<idx_t> &null_tuples_idx,
     vector<idx_t> &eid_to_mapping_idx, IOCache *io_cache);
 StoreAPIResult doVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, 
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent,
     vector<idx_t> &cols_to_include, idx_t current_pos,
     const vector<uint32_t> &output_col_idx);
 StoreAPIResult doVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, 
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent, idx_t current_pos,
     idx_t out_id_col_idx, Vector &rowcol_vec, char *row_major_store, 
     const vector<uint32_t> &output_col_idx, idx_t &num_output_tuples);
 StoreAPIResult doVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, 
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent,
     vector<idx_t> &cols_to_include, idx_t current_pos,
     const vector<uint32_t> &output_col_idx, idx_t &num_tuples_per_chunk);
 void getAdjColIdxs(idx_t index_cat_oid, vector<int> &adjColIdxs,
                    vector<LogicalType> &adjColTypes);
 StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter,
                                  int adjColIdx, ExtentID &prev_eid,
                                  uint64_t vid, uint64_t *&start_ptr,
                                  uint64_t *&end_ptr,
                                  ExpandDirection expand_dir);

 void fillEidToMappingIdx(vector<uint64_t> &oids,
                          vector<idx_t> &eid_to_mapping_idx,
                          bool union_schema = false);

private:
	inline void _fillTargetSeqnosVecAndBoundaryPosition(idx_t i, ExtentID prev_eid);

private:
	ClientContext &client;
	ResizableBoolVector target_eid_flags;
	vector<vector<uint32_t>> target_seqnos_per_extent_map;
	vector<idx_t> boundary_position;
	vector<idx_t> tmp_vec;
	vector<idx_t> target_seqnos_per_extent_map_cursors;
	idx_t boundary_position_cursor;
	idx_t tmp_vec_cursor;
};

}