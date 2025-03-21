//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/subquery_type.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/query/regular_query.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public ParsedExpression {
public:
	SubqueryExpression();

	//! The actual subquery
	unique_ptr<RegularQuery> subquery;
	//! The subquery type
	SubqueryType subquery_type;

public:
	bool HasSubquery() const override {
		return true;
	}
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	static bool Equals(const SubqueryExpression *a, const SubqueryExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
