#include "planner/joinside.hpp"

#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_subquery_expression.hpp"
#include "planner/expression_iterator.hpp"

namespace duckdb {

unique_ptr<Expression> JoinCondition::CreateExpression(JoinCondition cond) {
	auto bound_comparison = make_unique<BoundComparisonExpression>(cond.comparison, move(cond.left), move(cond.right));
	return move(bound_comparison);
}

unique_ptr<Expression> JoinCondition::CreateExpression(vector<JoinCondition> conditions) {
	unique_ptr<Expression> result;
	for (auto &cond : conditions) {
		auto expr = CreateExpression(move(cond));
		if (!result) {
			result = move(expr);
		} else {
			auto conj =
			    make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(expr), move(result));
			result = move(conj);
		}
	}
	return result;
}

// JoinSide JoinSide::CombineJoinSide(JoinSide left, JoinSide right) {
// 	if (left == JoinSide::NONE) {
// 		return right;
// 	}
// 	if (right == JoinSide::NONE) {
// 		return left;
// 	}
// 	if (left != right) {
// 		return JoinSide::BOTH;
// 	}
// 	return left;
// }

// JoinSide JoinSide::GetJoinSide(idx_t table_binding, unordered_set<idx_t> &left_bindings,
//                                unordered_set<idx_t> &right_bindings) {
// 	if (left_bindings.find(table_binding) != left_bindings.end()) {
// 		// column references table on left side
// 		D_ASSERT(right_bindings.find(table_binding) == right_bindings.end());
// 		return JoinSide::LEFT;
// 	} else {
// 		// column references table on right side
// 		D_ASSERT(right_bindings.find(table_binding) != right_bindings.end());
// 		return JoinSide::RIGHT;
// 	}
// }

// JoinSide JoinSide::GetJoinSide(Expression &expression, unordered_set<idx_t> &left_bindings,
//                                unordered_set<idx_t> &right_bindings) {
// 	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
// 		auto &colref = (BoundColumnRefExpression &)expression;
// 		if (colref.depth > 0) {
// 			throw Exception("Non-inner join on correlated columns not supported");
// 		}
// 		return GetJoinSide(colref.binding.table_index, left_bindings, right_bindings);
// 	}
// 	D_ASSERT(expression.type != ExpressionType::BOUND_REF);
// 	if (expression.type == ExpressionType::SUBQUERY) {
// 		D_ASSERT(expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
// 		auto &subquery = (BoundSubqueryExpression &)expression;
// 		JoinSide side = JoinSide::NONE;
// 		if (subquery.child) {
// 			side = GetJoinSide(*subquery.child, left_bindings, right_bindings);
// 		}
// 		// correlated subquery, check the side of each of correlated columns in the subquery
// 		for (auto &corr : subquery.binder->correlated_columns) {
// 			if (corr.depth > 1) {
// 				// correlated column has depth > 1
// 				// it does not refer to any table in the current set of bindings
// 				return JoinSide::BOTH;
// 			}
// 			auto correlated_side = GetJoinSide(corr.binding.table_index, left_bindings, right_bindings);
// 			side = CombineJoinSide(side, correlated_side);
// 		}
// 		return side;
// 	}
// 	JoinSide join_side = JoinSide::NONE;
// 	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
// 		auto child_side = GetJoinSide(child, left_bindings, right_bindings);
// 		join_side = CombineJoinSide(child_side, join_side);
// 	});
// 	return join_side;
// }

// JoinSide JoinSide::GetJoinSide(const unordered_set<idx_t> &bindings, unordered_set<idx_t> &left_bindings,
//                                unordered_set<idx_t> &right_bindings) {
// 	JoinSide side = JoinSide::NONE;
// 	for (auto binding : bindings) {
// 		side = CombineJoinSide(side, GetJoinSide(binding, left_bindings, right_bindings));
// 	}
// 	return side;
// }

} // namespace duckdb
