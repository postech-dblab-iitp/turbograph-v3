//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/cypher_statement.hpp"
#include "parser/tableref.hpp"
#include "parser/query_node.hpp"

namespace duckdb {

class DeleteStatement : public CypherStatement {
public:
	DeleteStatement();

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
	vector<unique_ptr<TableRef>> using_clauses;
	vector<unique_ptr<ParsedExpression>> returning_list;
	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	DeleteStatement(const DeleteStatement &other);

public:
	string ToString() const override;
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
