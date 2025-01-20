#include "parser/expression/positional_reference_expression.hpp"

#include "common/exception.hpp"
#include "common/field_writer.hpp"
#include "common/types/hash.hpp"
#include "common/to_string.hpp"

namespace s62 {

PositionalReferenceExpression::PositionalReferenceExpression(idx_t index)
    : ParsedExpression(ExpressionType::POSITIONAL_REFERENCE, ExpressionClass::POSITIONAL_REFERENCE), index(index) {
}

string PositionalReferenceExpression::ToString() const {
	return "#" + to_string(index);
}

bool PositionalReferenceExpression::Equals(const PositionalReferenceExpression *a,
                                           const PositionalReferenceExpression *b) {
	return a->index == b->index;
}

unique_ptr<ParsedExpression> PositionalReferenceExpression::Copy() const {
	auto copy = make_unique<PositionalReferenceExpression>(index);
	copy->CopyProperties(*this);
	return move(copy);
}

hash_t PositionalReferenceExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	return CombineHash(s62::Hash(index), result);
}

void PositionalReferenceExpression::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(index);
}

unique_ptr<ParsedExpression> PositionalReferenceExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto expression = make_unique<PositionalReferenceExpression>(reader.ReadRequired<idx_t>());
	return move(expression);
}

} // namespace s62
