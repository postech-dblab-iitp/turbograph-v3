#include <iostream>
#include <cassert> 
#include <filesystem>

#include "livegraph.hpp"
#include "demo_plans.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"

#include "duckdb/common/types/chunk_collection.hpp"

#include "typedef.hpp"


int main(int argc, char** argv) {

	livegraph::Graph graph = livegraph::Graph("data/storage/block", "data/storage/wal");
	LiveGraphCatalog catalog;

	// parse and insert LDBC data to graph
	//std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_datagen_neo4j_SF1/converted";
	std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_interactive_impls/cypher/test-data/converted";
	LDBCInsert(graph, catalog, ldbc_path);

	catalog.printCatalog();

	// make a storage using livegraph and its catalog
	LiveGraphStore graphstore(graph, catalog);

	// load plans
	std::cout << "load plans" << std::endl;
	auto queryPlans = QueryPlanSuite();
	// get vector of pipelines

	// execute query
	std::cout << "execute query" << std::endl;
	// TODO call executequery
	

}