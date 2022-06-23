
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/executor.hpp"

class QueryPlanSuite{

public:
	QueryPlanSuite();

	// returns root pipeline
	duckdb::Pipeline* getLDBCShort1();

private:
	// std::shared_ptr<duckdb::DatabaseInstance> nullDB;
	// duckdb::ClientContext* nullClient;
	// duckdb::Executor* nullExecutor;
};