#include "plans/query_plan_suite.hpp"


namespace duckdb {

CypherPipelineExecutor* coco1_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::COCO_1() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = ic2_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* ic2_pipe1(QueryPlanSuite& suite) {

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
	//ops
	// sink
	ops.push_back( new PhysicalTopNSort(sch6, move(orders), (idx_t) 20, (idx_t)0)); // 

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


}