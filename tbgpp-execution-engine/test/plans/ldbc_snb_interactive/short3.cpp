#include "plans/query_plan_suite.hpp"


namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS3() {

	CypherSchema schema;
	schema.addNode("n");
	
	// scan params
	LabelSet scan_labels;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");

	// filter predcs
	CypherSchema filter_schema = schema;
	vector<unique_ptr<Expression>> predicates;
	unique_ptr<Expression> filter_expr1;
	{
		auto lhs = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1);	// id
		duckdb::Value rhsval;
		if(LDBC_SF==1) { rhsval = duckdb::Value::UBIGINT(57459); }
		if(LDBC_SF==10) { rhsval = duckdb::Value::UBIGINT(58929); }
		if(LDBC_SF==100) { rhsval = duckdb::Value::UBIGINT(19560); }
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
	}
	predicates.push_back(std::move(filter_expr1));
		
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("content", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);
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
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys));
		//operators
	ops.push_back(new PhysicalFilter(filter_schema, std::move(predicates)));
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