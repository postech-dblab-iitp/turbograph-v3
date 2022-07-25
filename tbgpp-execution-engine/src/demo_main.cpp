#include <iostream>
#include <cassert> 

//#include "livegraph.hpp"
#include "demo_plans.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"

//#include "common/types/chunk_collection.hpp"

#include "typedef.hpp"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/node_scan.hpp"
#include "execution/physical_operator/physical_dummy_operator.hpp"
#include "execution/physical_operator/produce_results.hpp"

#include "main/client_context.hpp"

using namespace duckdb;

int main(int argc, char** argv) {

	std::cout << "start of main" << std::endl;

	// auto db = std::shared_ptr<duckdb::DatabaseInstance>();
	// std::shared_ptr<ClientContext> client = 
    // 	std::make_shared<ClientContext>(db->shared_from_this());
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(nullptr);

  	std::shared_ptr<ClientContext> client = 
    	std::make_shared<ClientContext>(database->instance->shared_from_this());

	iTbgppGraphStore graphstore(*client);

	// load plans
	std::cout << "load plan suite" << std::endl;
	auto suite = QueryPlanSuite((GraphStore*)&graphstore, *client);

	// execute query
	std::cout << "execute query" << std::endl;

	// Run q1
	// auto q1_executors = suite.Test1();
	// for( auto exec : q1_executors ) { 
	// 	std::cout << "[Pipeline 1]" << std::endl;	// only 1 pipe. so ok
	// 	std::cout << exec->pipeline->toString() << std::endl;
	// 	exec->ExecutePipeline();
	// }

}	