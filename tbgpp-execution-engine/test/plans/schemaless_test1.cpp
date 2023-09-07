#include "plans/query_plan_suite.hpp"

namespace duckdb {

CypherPipelineExecutor *sch1_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST1() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch1_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch1_pipe1(QueryPlanSuite& suite) {
	// scan Metabolite
	Schema schema1;
	vector<LogicalType> tmp_schema1 {LogicalType::ID, LogicalType::VARCHAR};
	schema1.setStoredTypes(move(tmp_schema1));

	vector<idx_t> oids1 = {305};
	vector<vector<uint64_t>> projection_mapping1;
	projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1});

	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR};
	schema2.setStoredTypes(move(tmp_schema2));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schema1, move(oids1), move(projection_mapping1)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

}