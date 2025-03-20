#include "parser/statement/call_statement.hpp"

namespace duckdb {

CallStatement::CallStatement() : CypherStatement(StatementType::CALL_STATEMENT) {
}

CallStatement::CallStatement(const CallStatement &other) : CypherStatement(other), function(other.function->Copy()) {
}

unique_ptr<CypherStatement> CallStatement::Copy() const {
	return unique_ptr<CallStatement>(new CallStatement(*this));
}

} // namespace duckdb
