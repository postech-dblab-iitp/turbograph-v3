
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
	QueryPlanSuite(GraphStore* graphstore, ClientContext& context);

	// returns root pipeline
	std::vector<CypherPipelineExecutor*> Test1();	// NodeScan
	std::vector<CypherPipelineExecutor*> Test1_1();	// NodeScan + Projection
	std::vector<CypherPipelineExecutor*> Test1_2();	// NodeSCan + Filter + Projection
	std::vector<CypherPipelineExecutor*> Test1_3();	// NodeScan + Limit
	std::vector<CypherPipelineExecutor*> Test2();
	std::vector<CypherPipelineExecutor*> Test3();
	std::vector<CypherPipelineExecutor*> Test4();
	
	std::vector<CypherPipelineExecutor*> LDBCShort1();

private:

	GraphStore *graphstore;
	ClientContext &context;

};
}