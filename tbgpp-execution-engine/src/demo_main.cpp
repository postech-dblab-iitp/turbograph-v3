#include <iostream>
#include <cassert> 
#include <filesystem>

#include "livegraph.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"

#include "duckdb/common/types/chunk_collection.hpp"

#include "typedef.hpp"


int main(int argc, char** argv) {

	livegraph::Graph graph = livegraph::Graph("data/storage/block", "data/storage/wal");
	LiveGraphCatalog catalog;

	// parse and insert LDBC data to graph
	std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_interactive_impls/cypher/test-data/converted";
	LDBCInsert(graph, catalog, ldbc_path);

	catalog.printCatalog();

	// make a storage using livegraph and its catalog
		// copy prohibit
	LiveGraphStore graphstore(graph, catalog);

	// TODO generate chunk collection for each scan, and call storage API when first calling..
	
	auto fooCC = duckdb::ChunkCollection();
	auto fooadjopt = LoadAdjListOption::NONE;

	auto foopp = PropertyKeys();
	auto aa = LabelSet();

	// TODO test.
	//graphstore.doScan(fooCC, aa, fooadjopt, foopp);


	// generate plans and run on storage

}