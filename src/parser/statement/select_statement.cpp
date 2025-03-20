#include "parser/statement/select_statement.hpp"

#include "common/serializer.hpp"

namespace duckdb {

SelectStatement::SelectStatement(const SelectStatement &other) : CypherStatement(other), node(other.node->Copy()) {
}

unique_ptr<CypherStatement> SelectStatement::Copy() const {
	return unique_ptr<SelectStatement>(new SelectStatement(*this));
}

void SelectStatement::Serialize(Serializer &serializer) const {
	node->Serialize(serializer);
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto result = make_unique<SelectStatement>();
	result->node = QueryNode::Deserialize(source);
	return result;
}

bool SelectStatement::Equals(const CypherStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (SelectStatement *)other_p;
	return node->Equals(other->node.get());
}

string SelectStatement::ToString() const {
	return node->ToString();
}

} // namespace duckdb
