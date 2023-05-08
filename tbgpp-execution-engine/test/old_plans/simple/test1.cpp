
#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1() {
	icecream::ic.disable();
	Schema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	schema.addPropertyIntoNode("n", "url", duckdb::LogicalType::VARCHAR);
	// scan params
	IC();
	LabelSet scan_labels;
	scan_labels.insert("Organisation");
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("url");
	IC();
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	IC();
	// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys) );
	IC();
	// operators
	// sink
	ops.push_back(new PhysicalProduceResults(schema));
	IC();
	auto pipe1 = new CypherPipeline(ops);
	IC();
	auto ctx1 = new ExecutionContext(&context);
	IC();
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	IC();
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	IC();
	return result;
}

}