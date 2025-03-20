#include "parser/statement/prepare_statement.hpp"

namespace duckdb {

PrepareStatement::PrepareStatement() : CypherStatement(StatementType::PREPARE_STATEMENT), statement(nullptr), name("") {
}

PrepareStatement::PrepareStatement(const PrepareStatement &other)
    : CypherStatement(other), statement(other.statement->Copy()), name(other.name) {
}

unique_ptr<CypherStatement> PrepareStatement::Copy() const {
	return unique_ptr<PrepareStatement>(new PrepareStatement(*this));
}

} // namespace duckdb
