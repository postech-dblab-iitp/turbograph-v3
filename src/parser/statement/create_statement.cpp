#include "parser/statement/create_statement.hpp"

namespace duckdb {

CreateStatement::CreateStatement() : CypherStatement(StatementType::CREATE_STATEMENT) {
}

CreateStatement::CreateStatement(const CreateStatement &other) : CypherStatement(other), info(other.info->Copy()) {
}

unique_ptr<CypherStatement> CreateStatement::Copy() const {
	return unique_ptr<CreateStatement>(new CreateStatement(*this));
}

} // namespace duckdb
