#include <cassert>

#include "typedef.hpp"

#include "storage/graph_store.hpp"

#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

LiveGraphStore::LiveGraphStore(livegraph::Graph& graph, LiveGraphCatalog& catalog) {
	this->graph = &graph;
	this->catalog = &catalog;
}

// function for materializing adjlist
	// TODO import boost

// APIs
StoreAPIResult LiveGraphStore::doScan(duckdb::ChunkCollection& output, LabelSet labels, LoadAdjListOption loadAdj, PropertyKeys properties) {

	// assert if empty
	assert( output.Count() == 0 && "");
	// access catalog and find all ranges satisfying labels
	

	// access properties and select indices


	// for each item,
		// parse properties
		// append
	
	// for each item,
		// edge iterator
		// materialize
	
	return StoreAPIResult::ERROR;
}
