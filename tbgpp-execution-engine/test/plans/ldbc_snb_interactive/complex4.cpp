#include "plans/query_plan_suite.hpp"


namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC4() {


	// projection
	vector<unique_ptr<Expression>> expr_pj1;
	{	// tag
		auto e0 = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);	// tag._id
		auto e1 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// tag.name
		expr_pj1.push_back(std::move(e0));
		expr_pj1.push_back(std::move(e1));
	}
	{
		// case when BIGINT > post.creationDate >= BIGINT then 1 else 0 AS valid
		auto e_when_ll = make_unique<BoundConstantExpression>(Value::BIGINT(1277856000000));
		auto e_when_lr = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);
		auto e_when_l = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(e_when_ll), move(e_when_lr));
		auto e_when_r = make_unique<BoundConstantExpression>(Value::BIGINT(1275350400000));
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(e_when_l), move(e_when_r));
		auto e_then = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		auto e_else = make_unique<BoundConstantExpression>(Value::BIGINT(0));
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		expr_pj1.push_back(std::move(e));
	}
	{
		// case when BIGINT > post.creationDate then 1 else 0 AS invalid
		auto e_when_l = make_unique<BoundConstantExpression>(Value::BIGINT(1275350400000));
		auto e_when_r = make_unique<BoundReferenceExpression>(LogicalType::ID, 1);
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(e_when_l), move(e_when_r));
		auto e_then = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		auto e_else = make_unique<BoundConstantExpression>(Value::BIGINT(0));
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		expr_pj1.push_back(std::move(e));
	}
	

}

}