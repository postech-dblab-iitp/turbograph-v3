//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/select_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/query_node.hpp"
#include "parser/cypher_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

class QueryNode;

//! SelectStatement is a typical SELECT clause
class SelectStatement : public CypherStatement {
public:
	SelectStatement() : CypherStatement(StatementType::SELECT_STATEMENT) {
	}

	//! The main query node
	unique_ptr<QueryNode> node;

protected:
	SelectStatement(const SelectStatement &other);

public:
	//! Convert the SELECT statement to a string
	string ToString() const override;
	//! Create a copy of this SelectStatement
	unique_ptr<CypherStatement> Copy() const override;
	//! Serializes a SelectStatement to a stand-alone binary blob
	void Serialize(Serializer &serializer) const;
	//! Deserializes a blob back into a SelectStatement, returns nullptr if
	//! deserialization is not possible
	static unique_ptr<SelectStatement> Deserialize(Deserializer &source);
	//! Whether or not the statements are equivalent
	bool Equals(const CypherStatement *other) const;
};
} // namespace duckdb
