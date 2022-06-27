#pragma once

#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "typedef.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

class GraphStore { 

public:
	// define APIs here
	// TODO further need to be re-defined upon discussion

	// ! Scan used by scan operators
	StoreAPIResult doScan(duckdb::ChunkCollection& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	StoreAPIResult doIndexSeek(duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema);
	bool isNodeInLabelset(u_int64_t id, LabelSet labels);
	// TODO ! Scan with storage predicate
	// StoreAPIResult doScan(ChunkCollection output, LabelSet labels, LoadAdjListOption loadAdj, PropertyKeys properties);

	// StoreAPIResult getNodeLabelSet(LabelSet& output, VertexID vid);
	// StoreAPIResult getEdgeLabelSet(LabelSet& output, EdgeID eid, PropertyKeys properties);

	// StoreAPIResult getNodeProperty(duckdb::ChunkCollection& output, PropertyKeys properties);
	// StoreAPIResult getEdgeProperties(duckdb::ChunkCollection& output, PropertyKeys properties);

};

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

};	