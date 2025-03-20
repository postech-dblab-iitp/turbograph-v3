//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/case_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "common/vector.hpp"

namespace duckdb {

struct CaseCheck {
	unique_ptr<ParsedExpression> when_expr;
	unique_ptr<ParsedExpression> then_expr;
};

//! The CaseExpression represents a CASE expression in the query
class CaseExpression : public ParsedExpression {
public:
	DUCKDB_API CaseExpression();

	unique_ptr<ParsedExpression> optional_case_expr;
	vector<CaseCheck> case_checks;
	unique_ptr<ParsedExpression> else_expr;

public:
	string ToString() const override;

	static bool Equals(const CaseExpression *a, const CaseExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		string case_str = "CASE ";
		if (entry.optional_case_expr) {
			case_str += "(" + entry.optional_case_expr->ToString() + ")";
		}
		for (auto &check : entry.case_checks) {
			case_str += " WHEN (" + check.when_expr->ToString() + ")";
			case_str += " THEN (" + check.then_expr->ToString() + ")";
		}
		if (entry.else_expr) {
			case_str += " ELSE (" + entry.else_expr->ToString() + ")";
		}
		case_str += " END";

		if (entry.HasAlias()) {
			case_str += " AS " + entry.GetAlias();
		}
		
		return case_str;
	}
};
} // namespace duckdb
