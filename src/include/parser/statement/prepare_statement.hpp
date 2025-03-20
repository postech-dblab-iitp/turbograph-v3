//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/prepare_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/cypher_statement.hpp"

namespace duckdb {

class PrepareStatement : public CypherStatement {
public:
	PrepareStatement();

	unique_ptr<CypherStatement> statement;
	string name;

protected:
	PrepareStatement(const PrepareStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};
} // namespace duckdb
