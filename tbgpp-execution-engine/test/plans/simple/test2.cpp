
#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test2() {

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
	unique_ptr<Expression> filter_expr1;
	{
		auto lhs = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);
		auto rhsval = duckdb::Value::UBIGINT(0);
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
	}
	predicates.push_back(std::move(filter_expr1));

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys) );
	// operators
	ops.push_back(new PhysicalFilter(schema, std::move(predicates)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema));
	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

}