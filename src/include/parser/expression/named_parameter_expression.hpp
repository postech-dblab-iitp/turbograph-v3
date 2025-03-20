#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {
class NamedParameterExpression : public ParsedExpression {
public:
	NamedParameterExpression(std::string parameter_name);

	std::string parameter_name;

public:
	bool IsScalar() const override {
		return true;
	}
	bool HasParameter() const override {
		return true;
	}

	string ToString() const override;

	static bool Equals(const NamedParameterExpression *a, const NamedParameterExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;
	hash_t Hash() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
