#pragma once

#include "parser/parsed_expression.hpp"
#include "common/vector.hpp"

namespace duckdb {

class PropertyExpression : public ParsedExpression {
public:
	DUCKDB_API PropertyExpression(string property_name, vector<unique_ptr<ParsedExpression>> children);
	DUCKDB_API PropertyExpression(string property_name, unique_ptr<ParsedExpression> child);

	string property_name;
	vector<unique_ptr<ParsedExpression>> children;

public:
	const string &GetPropertyName() const;
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equals(const PropertyExpression *a, const PropertyExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};

}