#pragma once

//#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "typedef.hpp"

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/types/data_chunk.hpp"
//#include "common/types/chunk_collection.hpp"

#include <boost/timer/timer.hpp>
#include <queue>


namespace duckdb {

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
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk &output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema, std::string filterKey, duckdb::Value fitlerValue) { return StoreAPIResult::OK; }
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

	StoreAPIResult InitializeScan(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<int64_t>> &projection_mapping, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult InitializeScan(std::queue<ExtentIterator *> &ext_its, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema, std::string filterKey, duckdb::Value fitlerValue);
	StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<int64_t>> projection_mapping, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult InitializeVertexIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult InitializeVertexIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position);
	StoreAPIResult doVertexIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult doVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx);
	StoreAPIResult InitializeEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult InitializeEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position);
	// StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema);
	StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx);
	bool isNodeInLabelset(u_int64_t id, LabelSet labels);
	void getAdjColIdxs(LabelSet src_labels, LabelSet edge_labels, ExpandDirection expand_dir, vector<int> &adjColIdxs, vector<LogicalType> &adjColTypes);
	StoreAPIResult initgetAdjList(AdjacencyListIterator &adj_iter, DataChunk &input, idx_t srcColIdx, int adjColIdx, ExpandDirection expand_dir);
	StoreAPIResult getAdjListRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t* start_idx, uint64_t* end_idx);
	StoreAPIResult getAdjListFromRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output, idx_t *&adjListBase);
	StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr, ExpandDirection expand_dir);

	// operator definition for getAdjListRange
	StoreAPIResult getAdjListRange(uint64_t vid, uint64_t* start_idx, uint64_t* end_idx);
	StoreAPIResult getAdjListFromRange(uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output );

private:
	ClientContext &client;
};
/*
class LiveGraphStore: GraphStore {

public:

	LiveGraphStore(livegraph::Graph* graph, LiveGraphCatalog* catalog);

public:
	// APIs

	//! | vid | adj-ls1-in | adj-ls2-in | ... | adj-ls1-out | adj-ls2-out | prop1 | prop2 | ...
	StoreAPIResult doScan(duckdb::ChunkCollection& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult doIndexSeek(duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	bool isNodeInLabelset(u_int64_t id, LabelSet labels);

	// StoreAPIResult getNodeLabelSet(LabelSet& output, VertexID vid);
	// StoreAPIResult getEdgeLabelSet(LabelSet& output, EdgeID eid, PropertyKeys properties);

	// StoreAPIResult getNodeProperty(duckdb::ChunkCollection& output, PropertyKeys properties);
	// StoreAPIResult getEdgeProperties(duckdb::ChunkCollection& output, PropertyKeys properties);


private:
	livegraph::Graph* graph;
	LiveGraphCatalog* catalog;

};	*/
}