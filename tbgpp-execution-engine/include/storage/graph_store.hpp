#pragma once

//#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "typedef.hpp"

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"
#include "common/types/data_chunk.hpp"
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

class GraphStore { 

public:
	// define APIs here
	// TODO further need to be re-defined upon discussion

	// ! Scan used by scan operators
	StoreAPIResult InitializeScan(ExtentIterator *&ext_it, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk &output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema, std::string filterKey, duckdb::Value filterValue) { return StoreAPIResult::OK; }
	StoreAPIResult doIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	bool isNodeInLabelset(u_int64_t id, LabelSet labels) { return true; }
	void getAdjColIdxs(LabelSet labels, vector<int> &adjColIdxs) {}
	StoreAPIResult getAdjListRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t* start_idx, uint64_t* end_idx) { return StoreAPIResult::OK; }
	StoreAPIResult getAdjListFromRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output, idx_t *&adjListBase) { return StoreAPIResult::OK; }
	StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr) { return StoreAPIResult::OK; }

};

class iTbgppGraphStore: GraphStore {
public:
	iTbgppGraphStore(ClientContext &client);

public:

	//! Initialize Scan Operation
	StoreAPIResult InitializeScan(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, vector<vector<duckdb::LogicalType>> &scanSchemas);

	//! Initialize Scan Operation
	StoreAPIResult InitializeScan(std::queue<ExtentIterator *> &ext_its, PropertySchemaID_vector *oids, vector<vector<uint64_t>> &projection_mapping, vector<vector<duckdb::LogicalType>> &scanSchemas);
	
	// Non filter
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping,
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, bool is_output_initialized = true);

	// Filter related
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, FilteredChunkBuffer &output_buffer, vector<vector<uint64_t>> &projection_mapping,
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, int64_t &filterKeyColIdx, duckdb::Value &filterValue);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, FilteredChunkBuffer &output_buffer, vector<vector<uint64_t>> &projection_mapping, 
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, int64_t &filterKeyColIdx, duckdb::RangeFilterValue &rangeFilterValue);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, FilteredChunkBuffer &output_buffer, vector<vector<uint64_t>> &projection_mapping, 
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, ExpressionExecutor& expr);

	StoreAPIResult InitializeVertexIndexSeek(ExtentIterator * &ext_it, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, 
											 DataChunk &input, idx_t nodeColIdx, vector<vector<LogicalType>> &scanSchemas, vector<ExtentID> &target_eids,
											 vector<vector<uint32_t>> &target_seqnos_per_extent, vector<idx_t> &mapping_idxs, 
											 vector<idx_t> &null_tuples_idx, vector<idx_t> &eid_to_mapping_idx, IOCache* io_cache);
	StoreAPIResult doVertexIndexSeek(ExtentIterator * &ext_it, DataChunk& output, DataChunk &input, 
									 idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									 vector<vector<uint32_t>> &target_seqnos_per_extent, vector<idx_t> &cols_to_include,
									 idx_t current_pos, vector<idx_t> output_col_idx);
	StoreAPIResult doVertexIndexSeek(ExtentIterator * &ext_it, DataChunk& output, DataChunk &input, 
									 idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									 vector<vector<uint32_t>> &target_seqnos_per_extent, idx_t current_pos, Vector &rowcol_vec,
									 char *row_major_store);
	StoreAPIResult doVertexIndexSeek(ExtentIterator * &ext_it, DataChunk& output, DataChunk &input, 
									 idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									 vector<vector<uint32_t>> &target_seqnos_per_extent, idx_t current_pos, vector<idx_t> output_col_idx,
									 idx_t &num_tuples_per_chunk);
	StoreAPIResult InitializeEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult InitializeEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position);
	StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx);
	bool isNodeInLabelset(u_int64_t id, LabelSet labels);
	void getAdjColIdxs(idx_t index_cat_oid, vector<int> &adjColIdxs, vector<LogicalType> &adjColTypes);
	StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, ExtentID &prev_eid, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr, ExpandDirection expand_dir);

	void fillEidToMappingIdx(vector<uint64_t>& oids, vector<idx_t>& eid_to_mapping_idx);

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