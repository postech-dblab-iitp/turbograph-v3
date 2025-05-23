#include "planner/expression/bound_case_expression.hpp"
#include "parser/expression/case_expression.hpp"

namespace duckdb {

BoundCaseExpression::BoundCaseExpression(LogicalType type)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, move(type)) {
}

BoundCaseExpression::BoundCaseExpression(unique_ptr<Expression> when_expr, unique_ptr<Expression> then_expr,
                                         unique_ptr<Expression> else_expr_p)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, then_expr->return_type),
      else_expr(move(else_expr_p)) {
	BoundCaseCheck check;
	check.when_expr = move(when_expr);
	check.then_expr = move(then_expr);
	case_checks.push_back(move(check));
}

string BoundCaseExpression::ToString() const {
	return CaseExpression::ToString<BoundCaseExpression, Expression>(*this);
}

bool BoundCaseExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = (BoundCaseExpression &)*other_p;
	if (case_checks.size() != other.case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < case_checks.size(); i++) {
		if (!Expression::Equals(case_checks[i].when_expr.get(), other.case_checks[i].when_expr.get())) {
			return false;
		}
		if (!Expression::Equals(case_checks[i].then_expr.get(), other.case_checks[i].then_expr.get())) {
			return false;
		}
	}
	if (!Expression::Equals(else_expr.get(), other.else_expr.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCaseExpression::Copy() {
	auto new_case = make_unique<BoundCaseExpression>(return_type);
	for (auto &check : case_checks) {
		BoundCaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		new_case->case_checks.push_back(move(new_check));
	}
	new_case->else_expr = else_expr->Copy();

	new_case->CopyProperties(*this);
	return move(new_case);
}

} // namespace duckdb
