//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/prepare_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class PrepareStatement : public SQLStatement {
public:
	PrepareStatement();

	unique_ptr<SQLStatement> statement;
	string name;

protected:
	PrepareStatement(const PrepareStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
