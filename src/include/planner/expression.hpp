//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/base_expression.hpp"
#include "common/types.hpp"

namespace duckdb {
class BaseStatistics;

//!  The Expression class represents a bound Expression with a return type
class Expression : public BaseExpression {
public:
	Expression(ExpressionType type, ExpressionClass expression_class, LogicalType return_type);
	~Expression() override;

	//! The return type of the expression
	LogicalType return_type;
	//! Expression statistics (if any) - ONLY USED FOR VERIFICATION
	//unique_ptr<BaseStatistics> verification_stats;

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;
	virtual bool HasSideEffects() const;
	virtual bool PropagatesNullValues() const;
	virtual bool IsFoldable() const;

	hash_t Hash() const override;

	bool Equals(const BaseExpression *other) const override {
		if (!BaseExpression::Equals(other)) {
			return false;
		}
		return return_type == ((Expression *)other)->return_type;
	}

	static bool Equals(Expression *left, Expression *right) {
		return BaseExpression::Equals((BaseExpression *)left, (BaseExpression *)right);
	}
	//! Create a copy of this expression
	virtual unique_ptr<Expression> Copy() = 0;

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(Expression &other) {
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
		return_type = other.return_type;
	}
};

} // namespace duckdb
