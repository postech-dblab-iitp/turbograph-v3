#include "plans/query_plan_suite.hpp"


namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC3() {


	// projection
	vector<unique_ptr<Expression>> expr_a;
	{	// friend
		auto e0= make_unique<BoundReferenceExpression>(LogicalType::ID, 1);	// friend._id
		auto e1 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1);	// friend.id
		auto e2 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// friend.firstName
		auto e3 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// friend.lastName
		expr_a.push_back(std::move(e0));expr_a.push_back(std::move(e1));expr_a.push_back(std::move(e2));expr_a.push_back(std::move(e3));
	}
	{
		// case when country = countryX THEN 1 ELSE 0 END AS messageX
		auto e_when_lhs = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);
		auto e_when_rhs = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(e_when_lhs), move(e_when_rhs));
		auto e_then = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		auto e_else = make_unique<BoundConstantExpression>(Value::BIGINT(0));
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		expr_a.push_back(std::move(e));
	}
	{
		// case when country = countryX THEN 1 ELSE 0 END AS messageY
		auto e_when_lhs = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);
		auto e_when_rhs = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(e_when_lhs), move(e_when_rhs));
		auto e_then = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		auto e_else = make_unique<BoundConstantExpression>(Value::BIGINT(0));
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		expr_a.push_back(std::move(e));
	}	

}

}