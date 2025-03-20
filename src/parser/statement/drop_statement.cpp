#include "parser/statement/drop_statement.hpp"

namespace duckdb {

DropStatement::DropStatement() : CypherStatement(StatementType::DROP_STATEMENT), info(make_unique<DropInfo>()) {
}

DropStatement::DropStatement(const DropStatement &other) : CypherStatement(other), info(other.info->Copy()) {
}

unique_ptr<CypherStatement> DropStatement::Copy() const {
	return unique_ptr<DropStatement>(new DropStatement(*this));
}

} // namespace duckdb
