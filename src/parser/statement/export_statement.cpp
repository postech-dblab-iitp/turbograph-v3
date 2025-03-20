#include "parser/statement/export_statement.hpp"

namespace duckdb {

ExportStatement::ExportStatement(unique_ptr<CopyInfo> info)
    : CypherStatement(StatementType::EXPORT_STATEMENT), info(move(info)) {
}

ExportStatement::ExportStatement(const ExportStatement &other) : CypherStatement(other), info(other.info->Copy()) {
}

unique_ptr<CypherStatement> ExportStatement::Copy() const {
	return unique_ptr<ExportStatement>(new ExportStatement(*this));
}

} // namespace duckdb
