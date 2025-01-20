//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/parsed_expression.hpp"
#include "planner/expression.hpp"

namespace s62 {

//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression.
//! It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts
//! when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	BoundExpression(unique_ptr<Expression> expr)
	    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(move(expr)) {
	}

	unique_ptr<Expression> expr;

public:
	string ToString() const override {
		if (!expr) {
			throw InternalException("ToString(): BoundExpression does not have a child");
		}
		return expr->ToString();
	}

	bool Equals(const BaseExpression *other) const override {
		return false;
	}
	hash_t Hash() const override {
		return 0;
	}

	unique_ptr<ParsedExpression> Copy() const override {
		throw SerializationException("Cannot copy or serialize bound expression");
	}

	void Serialize(FieldWriter &writer) const override {
		throw SerializationException("Cannot copy or serialize bound expression");
	}
};

} // namespace s62
