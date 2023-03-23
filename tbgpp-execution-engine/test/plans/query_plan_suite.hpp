#pragma once

#include <cstdlib>
#include <string>

#include "main/database.hpp"

#include "main/client_context.hpp"

//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"

#include "storage/graph_store.hpp"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/physical_operator/physical_node_scan.hpp"
#include "execution/physical_operator/physical_filter.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/physical_operator/physical_sort.hpp"
#include "execution/physical_operator/physical_top.hpp"
#include "execution/physical_operator/physical_top_n_sort.hpp"
#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "execution/physical_operator/physical_hash_aggregate.hpp"
#include "execution/physical_operator/physical_produce_results.hpp"
#include "execution/physical_operator/physical_unwind.hpp"
#include "execution/physical_operator/physical_varlen_adjidxjoin.hpp"

#include "common/constants.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"

#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"

#include "function/aggregate_function.hpp"

#include "icecream.hpp"	

namespace duckdb {

class ClientContext;
class QueryPlanSuite {

public:
	QueryPlanSuite(ClientContext& context): context(context) {
		
		// search for SF environment variable
		char* sf_str = std::getenv("SF");
		// default scale factor as 1
		LDBC_SF = 1;
		TPCH_SF = 1;

		// overwrite scalefactor from SF
		if( sf_str != NULL ){
			try {
			int conv = std::stoi((string)sf_str);
			// overwrite
			LDBC_SF = conv;
			TPCH_SF = conv;
			} catch (const std::string& expn) {
				std::cout << "Cannot find valid envvar SF. Setting to default environment variable SF=1" << std::endl;
			}
		}
	
		std::cout << "QueryPlanSuite : using SF=" << LDBC_SF << std::endl;
		
	}

	std::vector<CypherPipelineExecutor*> getTest(string key) {
		//IC();
		/* Test queries */
		// if( key.compare("t1") == 0 ) { return Test1(); }		// Scan
		// if( key.compare("t2") == 0 ) { return Test2(); }		// Scan + Filter
		// if( key.compare("t3") == 0 ) { return Test3(); }		// Scan + Projection
		// if( key.compare("t4") == 0 ) { return Test4(); }
		// if( key.compare("t5") == 0 ) { return Test5(); }		// Scan + NodeIdSeek (adding columns to node where data partially exists)
		// if( key.compare("t6") == 0 ) { return Test6(); }		// Scan + TopNSort
		// if( key.compare("t7") == 0 ) { return Test7(); }		// Scan + TopNSort (string sort key)
		// if( key.compare("t8") == 0 ) { return Test8(); }		// Scan + Order(without grouping; count(col))
		// if( key.compare("t9") == 0 ) { return Test9(); }		// Scan + Order(with grouping; + collect)
		// if( key.compare("t10") == 0 ) { return Test10(); }		// Scan + Order(with grouping; + collect) + Unwind : result same as scan
		// if( key.compare("t11") == 0 ) { return Test11(); }		// Scan + Expand (incoming) ( using backward edge )
		// if( key.compare("t11-1") == 0 ) { return Test11_1(); }		// Scan + Expand (outgoing ; reversed of test 11)
		// if( key.compare("t12") == 0 ) { return Test12(); }		// Scan + Expand (outgoing ; reversed of test 11)


		/* LDBC queries */
		// if( key.compare("s1") == 0 ) { return LDBC_IS1(); }
		if( key.compare("s2") == 0 ) { return LDBC_IS2(); }
		// if( key.compare("s3") == 0 ) { return LDBC_IS3(); }
		// if( key.compare("s4") == 0 ) { return LDBC_IS4(); }
		// if( key.compare("s5") == 0 ) { return LDBC_IS5(); }
		if( key.compare("s6") == 0 ) { return LDBC_IS6(); }
		// if( key.compare("s7") == 0 ) { return LDBC_IS7(); }
		// if( key.compare("c2") == 0 ) { return LDBC_IC2(); }
		// if( key.compare("c4") == 0 ) { return LDBC_IC4(); }
		// if( key.compare("c8") == 0 ) { return LDBC_IC8(); }

		/* TPC-H queries */
		// if( key.compare("q4") == 0 ) { return TPCH_Q4(); }
		// if( key.compare("q10") == 0 ) { return TPCH_Q10(); }
		// if( key.compare("q10-1") == 0 ) { return TPCH_Q10_1(); }
		// if( key.compare("q10-1-test") == 0 ) { return TPCH_Q10_1_TEST(); }
		// if( key.compare("q10-1-test2") == 0 ) { return TPCH_Q10_1_TEST2(); }
		// if( key.compare("q13") == 0 ) { return TPCH_Q13(); }

		/* COCO dataset */
		// if( key.compare("coco1") == 0 ) { return COCO_Q1(); }
		// if( key.compare("coco2") == 0 ) { return COCO_Q2(); }
		// if( key.compare("coco3a") == 0 ) { return COCO_Q3A(); }
		// if( key.compare("coco3b") == 0 ) { return COCO_Q3B(); }
		// if( key.compare("coco3c") == 0 ) { return COCO_Q3C(); }
		// if( key.compare("coco3d") == 0 ) { return COCO_Q3D(); }


		/* Empty plan at last */
		return std::vector<CypherPipelineExecutor*>();
	}
	// returns root pipeline
	// std::vector<CypherPipelineExecutor*> Test1();	// 
	// std::vector<CypherPipelineExecutor*> Test2();	// 
	// std::vector<CypherPipelineExecutor*> Test3();	// 
	// std::vector<CypherPipelineExecutor*> Test4();	// 
	// std::vector<CypherPipelineExecutor*> Test5();	// 
	// std::vector<CypherPipelineExecutor*> Test6();	// 
	// std::vector<CypherPipelineExecutor*> Test7();	// 
	// std::vector<CypherPipelineExecutor*> Test8();	// 
	// std::vector<CypherPipelineExecutor*> Test9();	// 
	// std::vector<CypherPipelineExecutor*> Test10();	// 
	// std::vector<CypherPipelineExecutor*> Test11();	// 
	// std::vector<CypherPipelineExecutor*> Test11_1();	// 
	// std::vector<CypherPipelineExecutor*> Test12();	// 
	
	// std::vector<CypherPipelineExecutor*> LDBC_IS1();
	std::vector<CypherPipelineExecutor*> LDBC_IS2();
	// std::vector<CypherPipelineExecutor*> LDBC_IS3();
	// std::vector<CypherPipelineExecutor*> LDBC_IS4();
	// std::vector<CypherPipelineExecutor*> LDBC_IS5();
	std::vector<CypherPipelineExecutor*> LDBC_IS6();
	// std::vector<CypherPipelineExecutor*> LDBC_IS7();

	// std::vector<CypherPipelineExecutor*> LDBC_IC1();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC2();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC3();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC4();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC5();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC6();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC7();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC8();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC9();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC10();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC11();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC12();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC13();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> LDBC_IC14();	//   |   |   |

	// std::vector<CypherPipelineExecutor*> TPCH_Q3();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> TPCH_Q4();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> TPCH_Q10();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> TPCH_Q10_1();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> TPCH_Q10_1_TEST();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> TPCH_Q10_1_TEST2();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> TPCH_Q13();	//   |   |   |

	// std::vector<CypherPipelineExecutor*> COCO_Q1();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> COCO_Q2();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> COCO_Q3A();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> COCO_Q3B();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> COCO_Q3C();	//   |   |   |
	// std::vector<CypherPipelineExecutor*> COCO_Q3D();	//   |   |   |
	
	// std::vector<CypherPipelineExecutor*> TC();			// Triangle Counting
	ClientContext &context;
	int64_t LDBC_SF;
	int64_t TPCH_SF;

};
}
