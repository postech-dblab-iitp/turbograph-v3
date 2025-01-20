
#include "plans/query_plan_suite.hpp"

#include "icecream.hpp"

namespace s62 {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test3() {

	Schema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "name", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", s62::LogicalType::UBIGINT);
	schema.addPropertyIntoNode("n", "url", s62::LogicalType::VARCHAR);
	// scan params
	LabelSet scan_labels;
	scan_labels.insert("Organisation");
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("url");
	// filter preds
	s62::Value filter_val; // person key
	if(LDBC_SF==1) { filter_val = s62::Value::UBIGINT(14); }
	if(LDBC_SF==10) { filter_val = s62::Value::UBIGINT(14); }
	if(LDBC_SF==100) { filter_val = s62::Value::UBIGINT(14); }
	
	// projection
	Schema pj_schema;
	pj_schema.addNode("n");
	pj_schema.addPropertyIntoNode("n", "id", s62::LogicalType::UBIGINT);
	pj_schema.addPropertyIntoNode("n", "name", s62::LogicalType::VARCHAR);
	//proj pred
	vector<unique_ptr<Expression>> proj_exprs;
	{	//  pid name id url => pid id name
		auto c1 = make_unique<BoundReferenceExpression>(LogicalType::ID, 0);	// vid(ID) 0->0
		auto c2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);	// id 2->1
		auto c3 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// name 1->2
		proj_exprs.push_back(std::move(c1));
		proj_exprs.push_back(std::move(c2));
		proj_exprs.push_back(std::move(c3));
	}
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys, "id", filter_val) );
	// operators
	ops.push_back(new PhysicalProjection(pj_schema, std::move(proj_exprs)));
	// sink
	ops.push_back(new PhysicalProduceResults(pj_schema));
	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);

	return result;
}

}