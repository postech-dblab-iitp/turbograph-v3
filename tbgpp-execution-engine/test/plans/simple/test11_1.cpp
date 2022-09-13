
#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test11_1() {
	icecream::ic.disable();

	CypherSchema schema;
	schema.addNode("p");

	// expand (person -> comment)
	CypherSchema schema2 = schema;
	schema2.addNode("c");

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	ops.push_back(new PhysicalNodeScan(schema, LabelSet("Person"), PropertyKeys()) );
	// operators
	ops.push_back(new PhysicalAdjIdxJoin(schema2, "p", LabelSet("Person"), LabelSet("LIKES"), ExpandDirection::OUTGOING, LabelSet("Comment")));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2));

	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

}