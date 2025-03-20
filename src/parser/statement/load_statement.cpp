#include "parser/statement/load_statement.hpp"

namespace duckdb {

LoadStatement::LoadStatement() : CypherStatement(StatementType::LOAD_STATEMENT) {
}

LoadStatement::LoadStatement(const LoadStatement &other) : CypherStatement(other), info(other.info->Copy()) {
}

unique_ptr<CypherStatement> LoadStatement::Copy() const {
	return unique_ptr<LoadStatement>(new LoadStatement(*this));
}

} // namespace duckdb
