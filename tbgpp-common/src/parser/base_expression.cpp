#include "parser/base_expression.hpp"


namespace duckdb {

void BaseExpression::Print() const {
	//Printer::Print(ToString());
}

string BaseExpression::GetName() const {
	return !alias.empty() ? alias : ToString();
}

bool BaseExpression::Equals(const BaseExpression *other) const {
	if (!other) {
		return false;
	}
	if (this->expression_class != other->expression_class || this->type != other->type) {
		return false;
	}
	return true;
}

void BaseExpression::Verify() const {
}

} // namespace duckdb
