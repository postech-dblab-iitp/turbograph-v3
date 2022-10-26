//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/call_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"
#include "common/vector.hpp"

namespace duckdb {

class CallStatement : public SQLStatement {
public:
	CallStatement();

	unique_ptr<ParsedExpression> function;

protected:
	CallStatement(const CallStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
