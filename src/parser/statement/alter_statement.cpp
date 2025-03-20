#include "parser/statement/alter_statement.hpp"

namespace duckdb {

AlterStatement::AlterStatement() : CypherStatement(StatementType::ALTER_STATEMENT) {
}

AlterStatement::AlterStatement(const AlterStatement &other) : CypherStatement(other), info(other.info->Copy()) {
}

unique_ptr<CypherStatement> AlterStatement::Copy() const {
	return unique_ptr<AlterStatement>(new AlterStatement(*this));
}

} // namespace duckdb
