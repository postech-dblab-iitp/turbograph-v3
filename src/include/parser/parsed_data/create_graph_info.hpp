#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/unordered_set.hpp"
//#include "parser/column_definition.hpp"
//#include "parser/constraint.hpp"
//#include "parser/statement/select_statement.hpp"

namespace duckdb {

struct CreateGraphInfo : public CreateInfo {
	CreateGraphInfo() : CreateInfo(CatalogType::GRAPH_ENTRY, INVALID_SCHEMA) {
	}
	CreateGraphInfo(string schema, string name) : CreateInfo(CatalogType::TABLE_ENTRY, schema), graph(name) {
	}

	//! Graph name to insert to
	string graph;
	//! CREATE GRAPH from QUERY // TODO Cypher needs to be extended to support graph creation
	//unique_ptr<SelectStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateGraphInfo>(schema, graph);
		CopyProperties(*result);
		//if (query) {
		//	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
		//}
		return move(result);
	}
};

} // namespace duckdb
