
#include "main/database.hpp"
#include "main/client_context.hpp"

//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"

#include "storage/graph_store.hpp"
#pragma once

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/physical_operator/physical_node_scan.hpp"
#include "execution/physical_operator/physical_filter.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/physical_operator/physical_produce_results.hpp"

#include "planner/expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"

#include "icecream.hpp"	

namespace duckdb {

class ClientContext;
class QueryPlanSuite {

public:
	QueryPlanSuite(ClientContext& context): context(context) {};

	std::vector<CypherPipelineExecutor*> getTest(string key) {
		IC();
		if( key.compare("t1") == 0 ) { return Test1(); }
		if( key.compare("t2") == 0 ) { return Test2(); }
		if( key.compare("t3") == 0 ) { return Test3(); }
		// if( key.compare("t4") == 0 ) { return Test4(); }
		// if( key.compare("t5") == 0 ) { return Test5(); }
		// if( key.compare("t6") == 0 ) { return Test6(); }
		if( key.compare("") == 0 ) { return std::vector<CypherPipelineExecutor*>(); }
		else { return std::vector<CypherPipelineExecutor*>(); }
	}
	// returns root pipeline
	std::vector<CypherPipelineExecutor*> Test1();	// 
	std::vector<CypherPipelineExecutor*> Test2();	// 
	std::vector<CypherPipelineExecutor*> Test3();	// 
	std::vector<CypherPipelineExecutor*> Test4();	// 
	std::vector<CypherPipelineExecutor*> Test5();	// 
	std::vector<CypherPipelineExecutor*> Test6();	// 
	
	std::vector<CypherPipelineExecutor*> LDBCShort1();

	// std::vector<CypherPipelineExecutor*> TC();			// Triangle Counting

private:

	ClientContext &context;

};
}
