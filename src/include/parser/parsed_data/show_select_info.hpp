//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/show_select_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/parse_info.hpp"
#include "parser/query_node.hpp"

namespace duckdb {

struct ShowSelectInfo : public ParseInfo {
	//! Types of projected columns
	vector<LogicalType> types;
	//! The QueryNode of select query
	unique_ptr<QueryNode> query;
	//! Aliases of projected columns
	vector<string> aliases;
	//! Whether or not we are requesting a summary or a describe
	bool is_summary;

	unique_ptr<ShowSelectInfo> Copy() {
		auto result = make_unique<ShowSelectInfo>();
		result->types = types;
		result->query = query->Copy();
		result->aliases = aliases;
		result->is_summary = is_summary;
		return result;
	}
};

} // namespace duckdb
