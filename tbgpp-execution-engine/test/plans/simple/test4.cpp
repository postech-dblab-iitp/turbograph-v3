
#include "plans/query_plan_suite.hpp"

#include "icecream.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test4() {
	// TODO scan - project - sort
	CypherSchema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	schema.addPropertyIntoNode("n", "url", duckdb::LogicalType::VARCHAR);
	// scan params
	LabelSet scan_labels;
	scan_labels.insert("Organisation");
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("url");
	// filter preds
	vector<unique_ptr<Expression>> predicates;
	{
		// expression space
		unique_ptr<Expression> filter_expr1;
		auto lhs = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);	// pid name "id" -> index 2
		auto rhsval = duckdb::Value::UBIGINT(0);
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
		predicates.push_back(std::move(filter_expr1));
	}
	
	// projection
	CypherSchema pj_schema;
	pj_schema.addNode("n");
	pj_schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	pj_schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
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
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys) );
	// operators
	//ops.push_back(new PhysicalFilter(schema, std::move(predicates)));
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