
#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test11() {
	icecream::ic.disable();

	CypherSchema schema;
	schema.addNode("c");

	// expand (com -> person) ; inverted
	CypherSchema schema2 = schema;
	schema2.addNode("p");

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	ops.push_back(new PhysicalNodeScan(schema, LabelSet("Comment"), PropertyKeys()) );
	// operators
	ops.push_back(new PhysicalAdjIdxJoin(schema2, "c", LabelSet("Comment"), LabelSet("LIKES"), ExpandDirection::INCOMING, LabelSet("Person")));
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