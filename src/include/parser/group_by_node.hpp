//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/group_by_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "common/set.hpp"
#include "common/vector.hpp"

namespace duckdb {

using GroupingSet = set<idx_t>;

class GroupByNode {
public:
	//! The total set of all group expressions
	vector<unique_ptr<ParsedExpression>> group_expressions;
	//! The different grouping sets as they map to the group expressions
	vector<GroupingSet> grouping_sets;
};

} // namespace duckdb
