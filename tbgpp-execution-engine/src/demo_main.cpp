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

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/node_scan.hpp"
#include "execution/physical_operator/physical_dummy_operator.hpp"
#include "execution/physical_operator/produce_results.hpp"

int main(int argc, char** argv) {

	// TODO change hard coding
	livegraph::Graph graph = livegraph::Graph("/home/jhko/dev/turbograph-v3/tbgpp-execution-engine/data/storage/block",
		"/home/jhko/dev/turbograph-v3/tbgpp-execution-engine/data/storage/wal");
	LiveGraphCatalog catalog;

	// parse and insert LDBC data to graph
	//std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_datagen_neo4j_SF1/converted";
	std::string ldbc_path = "/home/jhko/dev/ldbc-benchmark/ldbc_snb_interactive_impls/cypher/test-data/converted";
	LDBCInsert(graph, catalog, ldbc_path);

	catalog.printCatalog();

	// make a storage using livegraph and its catalog
	LiveGraphStore graphstore(&graph, &catalog);

	// load plans
	std::cout << "load plan suite" << std::endl;
	auto suite = QueryPlanSuite((GraphStore*)&graphstore);

	// execute query
	std::cout << "execute query" << std::endl;

	// Run q1
	auto q1_executors = suite.Test1();
	for( auto exec : q1_executors ) { exec->ExecutePipeline(); }

}	