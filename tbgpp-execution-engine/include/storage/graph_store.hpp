#pragma once

//#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "typedef.hpp"

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"
#include "common/types/data_chunk.hpp"

#include <boost/timer/timer.hpp>
#include <queue>


namespace duckdb {

struct RangeFilterValue {
	Value l_value;
	Value r_value;
	bool l_inclusive;
	bool r_inclusive;
};

class ExtentIterator;
class AdjacencyListIterator;
class ClientContext;
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
	// TODO ! Scan with storage predicate
	// StoreAPIResult doScan(ChunkCollection output, LabelSet labels, LoadAdjListOption loadAdj, PropertyKeys properties);

	// StoreAPIResult getNodeLabelSet(LabelSet& output, VertexID vid);
	// StoreAPIResult getEdgeLabelSet(LabelSet& output, EdgeID eid, PropertyKeys properties);

	// StoreAPIResult getNodeProperty(duckdb::ChunkCollection& output, PropertyKeys properties);
	// StoreAPIResult getEdgeProperties(duckdb::ChunkCollection& output, PropertyKeys properties);

};

class iTbgppGraphStore: GraphStore {
public:
	iTbgppGraphStore(ClientContext &client);

public:

	StoreAPIResult InitializeScan(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, vector<vector<duckdb::LogicalType>> &scanSchemas);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping,
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping,
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t &filterKeyColIdx, duckdb::Value &filterValue);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping,
						  std::vector<duckdb::LogicalType> &scanSchema, int64_t &filterKeyColIdx, duckdb::RangeFilterValue &rangeFilterValue);
	StoreAPIResult InitializeVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, 
											 DataChunk &input, idx_t nodeColIdx, vector<LogicalType> &scanSchema, vector<ExtentID> &target_eids,
											 vector<idx_t> &boundary_position);
	StoreAPIResult InitializeVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, 
											 DataChunk &input, idx_t nodeColIdx, vector<vector<LogicalType>> &scanSchemas, vector<ExtentID> &target_eids,
											 vector<vector<idx_t>> &target_seqnos_per_extent, std::unordered_map<idx_t, idx_t> &ps_oid_to_projection_mapping,
											 vector<idx_t> &mapping_idxs);
	StoreAPIResult doVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels,
									 std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties,
									 std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, 
									 idx_t current_pos, vector<idx_t> output_col_idx);
	StoreAPIResult doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input, 
									 idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									 vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx);
	StoreAPIResult doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input, 
									 idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									 vector<vector<idx_t>> &target_seqnos_per_extent, idx_t current_pos, vector<idx_t> output_col_idx);
	StoreAPIResult doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
									 idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									 vector<vector<idx_t>> &target_seqnos_per_extent, idx_t current_pos, vector<idx_t> output_col_idx,
									 idx_t &output_idx, SelectionVector &sel, int64_t &filterKeyColIdx, duckdb::Value &filterValue);
	StoreAPIResult InitializeEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult InitializeEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position);
	// StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx);
	bool isNodeInLabelset(u_int64_t id, LabelSet labels);
	void getAdjColIdxs(idx_t index_cat_oid, vector<int> &adjColIdxs, vector<LogicalType> &adjColTypes);
	// StoreAPIResult getAdjListRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t* start_idx, uint64_t* end_idx);
	// StoreAPIResult getAdjListFromRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output, idx_t *&adjListBase);
	StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, ExtentID &prev_eid, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr, ExpandDirection expand_dir);

	// operator definition for getAdjListRange
	// StoreAPIResult getAdjListRange(uint64_t vid, uint64_t* start_idx, uint64_t* end_idx);
	// StoreAPIResult getAdjListFromRange(uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output );

private:
	void _fillTargetSeqnosVecAndBoundaryPosition(idx_t i, ExtentID prev_eid, unordered_map<ExtentID, vector<idx_t>> &target_seqnos_per_extent_map, vector<idx_t> &boundary_position);

private:
	ClientContext &client;
};

}