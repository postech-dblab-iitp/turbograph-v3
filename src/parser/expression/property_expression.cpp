#include "parser/expression/property_expression.hpp"

#include "common/field_writer.hpp"
#include "common/types/hash.hpp"
#include "parser/qualified_name.hpp"

namespace duckdb {

PropertyExpression::PropertyExpression(string property_name, vector<unique_ptr<ParsedExpression>> children_p)
    : ParsedExpression(ExpressionType::PROPERTY, ExpressionClass::PROPERTY), property_name(move(property_name)),
      children(move(children_p)) {
}

PropertyExpression::PropertyExpression(string property_name, unique_ptr<ParsedExpression> child)
    : ParsedExpression(ExpressionType::PROPERTY, ExpressionClass::PROPERTY), property_name(move(property_name)) {
    children.push_back(move(child));
}

const string &PropertyExpression::GetPropertyName() const {
	return property_name;
}

string PropertyExpression::GetName() const {
	return !alias.empty() ? alias : property_name;
}

string PropertyExpression::ToString() const {
	string result = children[0]->ToString();
    for (idx_t i = 1; i < children.size(); i++) {
        result += "." + children[i]->ToString();
    }
    result += "." + property_name;
    if (HasAlias()) {
        result += " AS " + alias;
    }
    return result;
}

bool PropertyExpression::Equals(const PropertyExpression *a, const PropertyExpression *b) {
    if (a->property_name != b->property_name) {
        return false;
    }
    return ExpressionUtil::SetEquals(a->children, b->children);
}

unique_ptr<ParsedExpression> PropertyExpression::Copy() const {
    vector<unique_ptr<ParsedExpression>> copy_children;
    for (auto &expr : children) {
        copy_children.push_back(expr->Copy());
    }
    auto copy = make_unique<PropertyExpression>(property_name, move(copy_children));
	copy->CopyProperties(*this);
	return move(copy);
}

void PropertyExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(children);
    writer.WriteString(property_name);
}

unique_ptr<ParsedExpression> PropertyExpression::Deserialize(ExpressionType type, FieldReader &reader) {
    auto property_name = reader.ReadRequired<string>();
    auto children = reader.ReadRequiredSerializableList<ParsedExpression>();
    return make_unique<PropertyExpression>(move(property_name), move(children));
}

}