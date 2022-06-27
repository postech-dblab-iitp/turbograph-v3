
#include "main/database.hpp"
#include "main/client_context.hpp"

//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"

#include "storage/graph_store.hpp"

#include "execution/cypher_pipeline_executor.hpp"

namespace duckdb {
class QueryPlanSuite{

public:
	QueryPlanSuite(GraphStore* graphstore);

	// returns root pipeline
	std::vector<CypherPipelineExecutor*> Test1();
	std::vector<CypherPipelineExecutor*> Test2();
	std::vector<CypherPipelineExecutor*> Test3();
	std::vector<CypherPipelineExecutor*> Test4();
	//std::vector<duckdb::Pipeline*> LDBCShort1();

private:

	GraphStore* graphstore;

};
}