
#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test5() {

	CypherSchema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	
	// scan params
	LabelSet scan_labels;
	scan_labels.insert("Organisation");
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");

	// seek ; add url column
	CypherSchema seekSchema = schema;
	seekSchema.addPropertyIntoNode("n", "url", duckdb::LogicalType::VARCHAR );
	PropertyKeys seek_propertyKeys;
	seek_propertyKeys.push_back("url");

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys) );
	// operators
	ops.push_back(new PhysicalNodeIdSeek(seekSchema, "n", scan_labels, seek_propertyKeys) );

	// sink
	ops.push_back(new PhysicalProduceResults(seekSchema));

	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);

	return result;
}

}