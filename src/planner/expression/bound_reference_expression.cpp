#include "planner/expression/bound_reference_expression.hpp"

#include "common/serializer.hpp"
#include "common/types/hash.hpp"
#include "common/to_string.hpp"

namespace duckdb {
BoundReferenceExpression::BoundReferenceExpression(string alias, LogicalType type, idx_t index)
    : Expression(ExpressionType::BOUND_REF, ExpressionClass::BOUND_REF, move(type)), index(index) {
	this->alias = move(alias);
}
BoundReferenceExpression::BoundReferenceExpression(LogicalType type, storage_t index)
    : BoundReferenceExpression(string(), move(type), index) {
}
BoundReferenceExpression::BoundReferenceExpression(LogicalType type, storage_t index, bool is_inner)
    : BoundReferenceExpression(string(), move(type), index) {
	this->is_inner = is_inner;
}


string BoundReferenceExpression::ToString() const {
	if (!alias.empty()) {
		return alias;
	}
	return "#" + to_string(index);
}

bool BoundReferenceExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundReferenceExpression *)other_p;
	return other->index == index;
}

hash_t BoundReferenceExpression::Hash() const {
	return CombineHash(Expression::Hash(), duckdb::Hash<idx_t>(index));
}

unique_ptr<Expression> BoundReferenceExpression::Copy() {
	return make_unique<BoundReferenceExpression>(alias, return_type, index);
}

} // namespace duckdb
