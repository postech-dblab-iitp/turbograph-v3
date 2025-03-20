#include "parser/statement/copy_statement.hpp"

namespace duckdb {

CopyStatement::CopyStatement() : CypherStatement(StatementType::COPY_STATEMENT), info(make_unique<CopyInfo>()) {
}

CopyStatement::CopyStatement(const CopyStatement &other) : CypherStatement(other), info(other.info->Copy()) {
	if (other.select_statement) {
		select_statement = other.select_statement->Copy();
	}
}

unique_ptr<CypherStatement> CopyStatement::Copy() const {
	return unique_ptr<CopyStatement>(new CopyStatement(*this));
}

} // namespace duckdb
