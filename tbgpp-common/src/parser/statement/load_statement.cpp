#include "parser/statement/load_statement.hpp"

namespace s62 {

LoadStatement::LoadStatement() : SQLStatement(StatementType::LOAD_STATEMENT) {
}

LoadStatement::LoadStatement(const LoadStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> LoadStatement::Copy() const {
	return unique_ptr<LoadStatement>(new LoadStatement(*this));
}

} // namespace s62
