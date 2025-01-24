//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"
#include "common/vector.hpp"

namespace duckdb {

class ExecuteStatement : public SQLStatement {
public:
	ExecuteStatement();

	string name;
	vector<unique_ptr<ParsedExpression>> values;

protected:
	ExecuteStatement(const ExecuteStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
