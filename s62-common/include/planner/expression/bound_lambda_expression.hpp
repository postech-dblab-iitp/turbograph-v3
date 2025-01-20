//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_lambda_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression.hpp"
#include "parser/expression/lambda_expression.hpp"

namespace s62 {

class BoundLambdaExpression : public Expression {
public:
	BoundLambdaExpression(ExpressionType type_p, LogicalType return_type_p, unique_ptr<Expression> lambda_expr_p,
	                      idx_t parameter_count_p);

	unique_ptr<Expression> lambda_expr;
	vector<unique_ptr<Expression>> captures;
	idx_t parameter_count;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace s62
