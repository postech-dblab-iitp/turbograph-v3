#include "parser/statement/alter_statement.hpp"

namespace s62 {

AlterStatement::AlterStatement() : SQLStatement(StatementType::ALTER_STATEMENT) {
}

AlterStatement::AlterStatement(const AlterStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> AlterStatement::Copy() const {
	return unique_ptr<AlterStatement>(new AlterStatement(*this));
}

} // namespace s62
