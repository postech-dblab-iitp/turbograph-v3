#include "parser/expression/named_parameter_expression.hpp"

#include "common/exception.hpp"
#include "common/field_writer.hpp"
#include "common/types/hash.hpp"
#include "common/to_string.hpp"
#include "common/types/string_type.hpp"

namespace duckdb {

NamedParameterExpression::NamedParameterExpression(string parameter_name)
    : ParsedExpression(ExpressionType::VALUE_PARAMETER, ExpressionClass::PARAMETER), parameter_name(move(parameter_name)) {
}

string NamedParameterExpression::ToString() const {
	return "$" + parameter_name;
}

unique_ptr<ParsedExpression> NamedParameterExpression::Copy() const {
	auto copy = make_unique<NamedParameterExpression>(parameter_name);
	copy->CopyProperties(*this);
	return move(copy);
}

bool NamedParameterExpression::Equals(const NamedParameterExpression *a, const NamedParameterExpression *b) {
	return a->parameter_name == b->parameter_name;
}

hash_t NamedParameterExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	return CombineHash(duckdb::Hash(duckdb::string_t(parameter_name)), result);
}

void NamedParameterExpression::Serialize(FieldWriter &writer) const {
	writer.WriteString(parameter_name);
}

unique_ptr<ParsedExpression> NamedParameterExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto param_name = reader.ReadRequired<string>();
	auto expression = make_unique<NamedParameterExpression>(param_name);
	return move(expression);
}

} // namespace duckdb
