#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/cypher_statement.hpp"

namespace duckdb {

enum class ExplainType : uint8_t { EXPLAIN_STANDARD, EXPLAIN_ANALYZE };

class ExplainStatement : public CypherStatement {
public:
	explicit ExplainStatement(unique_ptr<CypherStatement> stmt, ExplainType explain_type = ExplainType::EXPLAIN_STANDARD);

	unique_ptr<CypherStatement> stmt;
	ExplainType explain_type;

protected:
	ExplainStatement(const ExplainStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
