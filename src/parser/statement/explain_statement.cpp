#include "parser/statement/explain_statement.hpp"

namespace duckdb {

ExplainStatement::ExplainStatement(unique_ptr<CypherStatement> stmt, ExplainType explain_type)
    : CypherStatement(StatementType::EXPLAIN_STATEMENT), stmt(move(stmt)), explain_type(explain_type) {
}

ExplainStatement::ExplainStatement(const ExplainStatement &other)
    : CypherStatement(other), stmt(other.stmt->Copy()), explain_type(other.explain_type) {
}

unique_ptr<CypherStatement> ExplainStatement::Copy() const {
	return unique_ptr<ExplainStatement>(new ExplainStatement(*this));
}

} // namespace duckdb
