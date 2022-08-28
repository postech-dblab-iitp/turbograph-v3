
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
#include "execution/physical_operator/physical_sort.hpp"
#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "execution/physical_operator/physical_node_id_seek.hpp"
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
	QueryPlanSuite(ClientContext& context): context(context) {

		LDBC_SF=1; // TODO change SF
	};

	std::vector<CypherPipelineExecutor*> getTest(string key) {
		IC();
		if( key.compare("t1") == 0 ) { return Test1(); }		// Scan
		if( key.compare("t2") == 0 ) { return Test2(); }		// Scan + Filter
		if( key.compare("t3") == 0 ) { return Test3(); }		// Scan + Projection
		// if( key.compare("t4") == 0 ) { return Test4(); }
		if( key.compare("t5") == 0 ) { return Test5(); }		// Scan + NodeIdSeek (adding columns to node where data partially exists)
		// if( key.compare("t6") == 0 ) { return Test6(); }
		// if( key.compare("s1") == 0 ) { return LDBC_IS1(); }	// TODO error in the storage
		if( key.compare("s4") == 0 ) { return LDBC_IS4(); }
		// if( key.compare("s5") == 0 ) { return LDBC_IS5(); }
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
	
	std::vector<CypherPipelineExecutor*> LDBC_IS1();
	std::vector<CypherPipelineExecutor*> LDBC_IS2();
	std::vector<CypherPipelineExecutor*> LDBC_IS3();
	std::vector<CypherPipelineExecutor*> LDBC_IS4();
	std::vector<CypherPipelineExecutor*> LDBC_IS5();
	std::vector<CypherPipelineExecutor*> LDBC_IS6();
	std::vector<CypherPipelineExecutor*> LDBC_IS7();

	std::vector<CypherPipelineExecutor*> LDBC_IC1();
	std::vector<CypherPipelineExecutor*> LDBC_IC2();
	std::vector<CypherPipelineExecutor*> LDBC_IC3();
	std::vector<CypherPipelineExecutor*> LDBC_IC4();
	std::vector<CypherPipelineExecutor*> LDBC_IC5();
	std::vector<CypherPipelineExecutor*> LDBC_IC6();
	std::vector<CypherPipelineExecutor*> LDBC_IC7();
	std::vector<CypherPipelineExecutor*> LDBC_IC8();
	std::vector<CypherPipelineExecutor*> LDBC_IC9();
	std::vector<CypherPipelineExecutor*> LDBC_IC10();
	std::vector<CypherPipelineExecutor*> LDBC_IC11();
	std::vector<CypherPipelineExecutor*> LDBC_IC12();
	std::vector<CypherPipelineExecutor*> LDBC_IC13();
	std::vector<CypherPipelineExecutor*> LDBC_IC14();
	
	// std::vector<CypherPipelineExecutor*> TC();			// Triangle Counting

private:

	ClientContext &context;
	int64_t LDBC_SF;

};
}
