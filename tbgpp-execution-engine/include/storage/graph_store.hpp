#pragma once

//#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "typedef.hpp"

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/types/data_chunk.hpp"
//#include "common/types/chunk_collection.hpp"
namespace duckdb {

class ExtentIterator;
class ClientContext;

class GraphStore { 

public:
	// define APIs here
	// TODO further need to be re-defined upon discussion

	// ! Scan used by scan operators
	StoreAPIResult InitializeScan(ExtentIterator *&ext_it, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	bool isNodeInLabelset(u_int64_t id, LabelSet labels) { return true; }
	void getAdjColIdxs(LabelSet labels, vector<int> &adjColIdxs) {}

};

class iTbgppGraphStore: GraphStore {
public:
	iTbgppGraphStore(ClientContext &client);

public:

	StoreAPIResult InitializeScan(ExtentIterator *&ext_it, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk &output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult doIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	bool isNodeInLabelset(u_int64_t id, LabelSet labels);
	void getAdjColIdxs(LabelSet labels, vector<int> &adjColIdxs);

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