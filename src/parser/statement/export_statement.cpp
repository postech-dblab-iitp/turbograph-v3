#include "parser/statement/export_statement.hpp"

namespace duckdb {

ExportStatement::ExportStatement(unique_ptr<CopyInfo> info)
    : SQLStatement(StatementType::EXPORT_STATEMENT), info(move(info)) {
}

ExportStatement::ExportStatement(const ExportStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> ExportStatement::Copy() const {
	return unique_ptr<ExportStatement>(new ExportStatement(*this));
}

} // namespace duckdb
