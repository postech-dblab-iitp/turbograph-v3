#include "parser/expression/variable_expression.hpp"

#include "common/field_writer.hpp"
#include "common/types/hash.hpp"
#include "parser/qualified_name.hpp"

namespace duckdb {

VariableExpression::VariableExpression(string variable_name)
    : ParsedExpression(ExpressionType::VARIABLE, ExpressionClass::VARIABLE), variable_name(move(variable_name)) {
}

const string &VariableExpression::GetVariableName() const {
	return variable_name;
}

string VariableExpression::GetName() const {
	return !alias.empty() ? alias : variable_name;
}

string VariableExpression::ToString() const {
    return variable_name;
}

bool VariableExpression::Equals(const VariableExpression *a, const VariableExpression *b) {
    if (a->variable_name != b->variable_name) {
        return false;
    }
    return true;
}

unique_ptr<ParsedExpression> VariableExpression::Copy() const {
    auto copy = make_unique<VariableExpression>(variable_name);
    copy->CopyProperties(*this);
	return move(copy);
}

void VariableExpression::Serialize(FieldWriter &writer) const {
    writer.WriteString(variable_name);
}

unique_ptr<ParsedExpression> VariableExpression::Deserialize(ExpressionType type, FieldReader &reader) {
    auto variable_name = reader.ReadRequired<string>();
    return make_unique<VariableExpression>(move(variable_name));
}

}