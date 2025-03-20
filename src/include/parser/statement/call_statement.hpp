//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/call_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/cypher_statement.hpp"
#include "common/vector.hpp"

namespace duckdb {

class CallStatement : public CypherStatement {
public:
	CallStatement();

	unique_ptr<ParsedExpression> function;

protected:
	CallStatement(const CallStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};
} // namespace duckdb
