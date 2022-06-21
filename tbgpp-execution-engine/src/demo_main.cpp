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
	std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_datagen_neo4j_SF1/converted";
	//std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_interactive_impls/cypher/test-data/converted";
	LDBCInsert(graph, catalog, ldbc_path);

	catalog.printCatalog();

	// make a storage using livegraph and its catalog
		// copy prohibit
	LiveGraphStore graphstore(graph, catalog);

	// TODO generate chunk collection for each scan, and call storage API when first calling..
	
	auto fooadjopt = LoadAdjListOption::NONE;

	PropertyKeys foopp;
	foopp.push_back("url");
	foopp.push_back("name");
	foopp.push_back("id");

	auto aa = LabelSet();
	aa.insert("Country");
	aa.insert("Place");

	auto fooCC = duckdb::ChunkCollection();

	// define scan chunk schema
	std::vector<duckdb::LogicalType> scanSchema;
	scanSchema.push_back(duckdb::LogicalType::UBIGINT); // pid
	// TODO need to write for adjlist when needed.
	scanSchema.push_back(duckdb::LogicalType::VARCHAR); // url
	scanSchema.push_back(duckdb::LogicalType::VARCHAR); // name
	scanSchema.push_back(duckdb::LogicalType::BIGINT); // id

	// call scan API
	graphstore.doScan(fooCC, aa, fooadjopt, foopp, scanSchema);
	// when card = 0, fooCC have no type at all.

	// generate plans and run on storage

}