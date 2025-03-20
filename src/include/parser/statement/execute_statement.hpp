//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/cypher_statement.hpp"
#include "common/vector.hpp"

namespace duckdb {

class ExecuteStatement : public CypherStatement {
public:
	ExecuteStatement();

	string name;
	vector<unique_ptr<ParsedExpression>> values;

protected:
	ExecuteStatement(const ExecuteStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};
} // namespace duckdb
