#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS1() {

	// scan schema
	CypherSchema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "birthday", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "firstName", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "lastName", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "gender", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "browserUsed", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "locationIP", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "creationDate", duckdb::LogicalType::BIGINT);
	
	// scan params
	LabelSet scan_labels;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");
	scan_propertyKeys.push_back("birthday");
	scan_propertyKeys.push_back("firstName");
	scan_propertyKeys.push_back("lastName");
	scan_propertyKeys.push_back("gender");
	scan_propertyKeys.push_back("browserUsed");
	scan_propertyKeys.push_back("locationIP");
	scan_propertyKeys.push_back("creationDate");
	
	// Filter
	vector<unique_ptr<Expression>> predicates;
	unique_ptr<Expression> filter_expr1;
	{
		auto lhs = make_unique<BoundColumnRefExpression>("id", LogicalType::UBIGINT, ColumnBinding());
		duckdb::Value rhsval;
		if(LDBC_SF==1) { rhsval = duckdb::Value::UBIGINT(57459); }
		if(LDBC_SF==10) { rhsval = duckdb::Value::UBIGINT(58929); }
		if(LDBC_SF==100) { rhsval = duckdb::Value::UBIGINT(19560); }
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
	}
	predicates.push_back(std::move(filter_expr1));

	// Expand
	CypherSchema expandschema = schema;
	expandschema.addNode("p");
	
	// FetchId
	CypherSchema schema3 = expandschema;
	schema3.addPropertyIntoNode("p", "id", duckdb::LogicalType::UBIGINT );
	PropertyKeys seek_propertyKeys;
	seek_propertyKeys.push_back("id");

	// 0 1 2 3 4 5 6 7 8 9
	// _ b f l g b l c _ i

	// Project
	CypherSchema project_schema;
	project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("birthday", duckdb::LogicalType::BIGINT);
	project_schema.addColumn("locationIP", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("browserUsed", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("cityId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("gender", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 2)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 9)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 7)) );
	}
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys, move(predicates)));
		//operators
	ops.push_back(new PhysicalAdjIdxJoin(expandschema, "n", LabelSet("Person"), LabelSet("IS_LOCATED_IN"), ExpandDirection::OUTGOING, LabelSet("City"), JoinType::INNER, false, true));
	ops.push_back(new PhysicalNodeIdSeek(schema3, "p", LabelSet("Place"), seek_propertyKeys));
	ops.push_back(new PhysicalProjection(project_schema, move(proj_exprs)));
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