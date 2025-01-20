#include "plans/query_plan_suite.hpp"


namespace s62 {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS4() {

	Schema schema;
	schema.addNode("m");
	schema.addPropertyIntoNode("m", "id", s62::LogicalType::UBIGINT);
	schema.addPropertyIntoNode("m", "content", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("m", "creationDate", s62::LogicalType::BIGINT);
	
	// scan params
	LabelSet scan_labels;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Post");	// TODO this should originally by Message which is integration with Post and Comment
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("content");
	scan_propertyKeys.push_back("creationDate");

	// filter predcs
	Schema filter_schema = schema;
	s62::Value filter_val;
	if(LDBC_SF==1) { filter_val = s62::Value::UBIGINT(2199029886840); }
	if(LDBC_SF==10) { filter_val = s62::Value::UBIGINT(58929); }
	if(LDBC_SF==100) { filter_val = s62::Value::UBIGINT(19560); }
		
	// Project
	Schema project_schema;
	project_schema.addColumn("content", s62::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", s62::LogicalType::BIGINT);
	vector<unique_ptr<Expression>> proj_exprs;
	{	//  pid name id url => pid id name
		auto c1 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 2);
		auto c2 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3);
		proj_exprs.push_back(std::move(c1));
		proj_exprs.push_back(std::move(c2));
	}

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys, "id", filter_val) );
		//operators
	ops.push_back(new PhysicalProjection(project_schema, std::move(proj_exprs)));
		// sink
	ops.push_back(new PhysicalProduceResults(project_schema));

	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

}