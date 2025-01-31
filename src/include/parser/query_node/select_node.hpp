//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"
#include "parser/parsed_data/sample_options.hpp"
#include "parser/group_by_node.hpp"
#include "common/enums/aggregate_handling.hpp"

namespace duckdb {

//! SelectNode represents a standard SELECT statement
class SelectNode : public QueryNode {
public:
	SelectNode();

	//! The projection list
	vector<unique_ptr<ParsedExpression>> select_list;
	//! The FROM clause
	unique_ptr<TableRef> from_table;
	//! The WHERE clause
	unique_ptr<ParsedExpression> where_clause;
	//! list of groups
	GroupByNode groups;
	//! HAVING clause
	unique_ptr<ParsedExpression> having;
	//! QUALIFY clause
	unique_ptr<ParsedExpression> qualify;
	//! Aggregate handling during binding
	AggregateHandling aggregate_handling;
	//! The SAMPLE clause
	unique_ptr<SampleOptions> sample;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return select_list;
	}

public:
	//! Convert the query node to a string
	string ToString() const override;

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() const override;

	//! Serializes a QueryNode to a stand-alone binary blob
	void Serialize(FieldWriter &writer) const override;
	//! Deserializes a blob back into a QueryNode
	static unique_ptr<QueryNode> Deserialize(FieldReader &reader);
};

} // namespace duckdb
