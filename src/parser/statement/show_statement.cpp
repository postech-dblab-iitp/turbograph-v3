#include "parser/statement/show_statement.hpp"

namespace duckdb {

ShowStatement::ShowStatement() : CypherStatement(StatementType::SHOW_STATEMENT), info(make_unique<ShowSelectInfo>()) {
}

ShowStatement::ShowStatement(const ShowStatement &other) : CypherStatement(other), info(other.info->Copy()) {
}

unique_ptr<CypherStatement> ShowStatement::Copy() const {
	return unique_ptr<ShowStatement>(new ShowStatement(*this));
}

} // namespace duckdb
