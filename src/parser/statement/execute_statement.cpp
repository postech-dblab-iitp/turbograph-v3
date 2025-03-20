#include "parser/statement/execute_statement.hpp"

namespace duckdb {

ExecuteStatement::ExecuteStatement() : CypherStatement(StatementType::EXECUTE_STATEMENT) {
}

ExecuteStatement::ExecuteStatement(const ExecuteStatement &other) : CypherStatement(other), name(other.name) {
	for (const auto &value : other.values) {
		values.push_back(value->Copy());
	}
}

unique_ptr<CypherStatement> ExecuteStatement::Copy() const {
	return unique_ptr<ExecuteStatement>(new ExecuteStatement(*this));
}

} // namespace duckdb
