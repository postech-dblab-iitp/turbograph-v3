#include "parser/expression/subquery_expression.hpp"

#include "common/exception.hpp"
#include "common/field_writer.hpp"

namespace duckdb {

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID) {
}

string SubqueryExpression::ToString() const {
	switch (subquery_type) {
	case SubqueryType::EXISTS:
		return "EXISTS { " + subquery->ToString() + " }";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS { " + subquery->ToString() + " }";
	case SubqueryType::SCALAR:
		return "( " + subquery->ToString() + " )";
	case SubqueryType::COUNT:
		return "COUNT { " + subquery->ToString() + " }";
	default:
		throw InternalException("Unrecognized type for subquery");
	}
}

bool SubqueryExpression::Equals(const SubqueryExpression *a, const SubqueryExpression *b) {
	if (!a->subquery || !b->subquery) {
		return false;
	}
	return a->subquery_type == b->subquery_type &&
	       a->subquery->Equals(b->subquery.get());
}

unique_ptr<ParsedExpression> SubqueryExpression::Copy() const {
	auto copy = make_unique<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = unique_ptr_cast<CypherStatement, RegularQuery>(subquery->Copy());
	copy->subquery_type = subquery_type;
	return move(copy);
}

void SubqueryExpression::Serialize(FieldWriter &writer) const {
	auto &serializer = writer.GetSerializer();

	writer.WriteField<SubqueryType>(subquery_type);
	// FIXME: Implement serializer for RegularQuery
}

unique_ptr<ParsedExpression> SubqueryExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	// FIXME: this shouldn't use a source
	auto &source = reader.GetSource();

	auto subquery_type = reader.ReadRequired<SubqueryType>();
	auto expression = make_unique<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	return move(expression);
}

} // namespace duckdb
