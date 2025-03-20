#pragma once

#include "parser/parsed_expression.hpp"
#include "common/vector.hpp"

namespace duckdb {

class VariableExpression : public ParsedExpression {
public:
	DUCKDB_API VariableExpression(string variable_name);

	string variable_name;

public:
	const string &GetVariableName() const;
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equals(const VariableExpression *a, const VariableExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};

}