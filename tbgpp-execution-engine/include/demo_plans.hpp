
#include "main/database.hpp"
#include "main/client_context.hpp"

//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"

#include "storage/graph_store.hpp"

#include "execution/cypher_pipeline_executor.hpp"

namespace duckdb {

class ClientContext;

class QueryPlanSuite{

public:
	QueryPlanSuite(ClientContext& context);

	// returns root pipeline
	// std::vector<CypherPipelineExecutor*> Test1();	// NodeScan
	// std::vector<CypherPipelineExecutor*> Test1_1();	// NodeScan + Projection
	// std::vector<CypherPipelineExecutor*> Test1_2();	// NodeSCan + Filter + Projection
	// std::vector<CypherPipelineExecutor*> Test1_3();	// NodeScan + Limit
	// std::vector<CypherPipelineExecutor*> Test2();
	// std::vector<CypherPipelineExecutor*> Test3();
	// std::vector<CypherPipelineExecutor*> Test4();

	// std::vector<CypherPipelineExecutor*> Test5();	// Scan Comment
	// std::vector<CypherPipelineExecutor*> Test5_1();	// Scan Comment + Filter + Projection + Limit
	
	std::vector<CypherPipelineExecutor*> LDBCShort1();	// full support
	// std::vector<CypherPipelineExecutor*> LDBCShort3();	// slight mod
	// std::vector<CypherPipelineExecutor*> LDBCShort4();  // 2 plans
	// CypherPipelineExecutor* ldbc_s4_comment();
	// CypherPipelineExecutor* ldbc_s4_post();
	// std::vector<CypherPipelineExecutor*> LDBCShort5();	// 2 plans
	// CypherPipelineExecutor* ldbc_s5_comment();
	// CypherPipelineExecutor* ldbc_s5_post();

	// std::vector<CypherPipelineExecutor*> LDBCComplex2();

	// std::vector<CypherPipelineExecutor*> TC();			// Triangle Counting

private:

	ClientContext &context;

};
}
