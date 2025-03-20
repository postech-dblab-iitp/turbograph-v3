//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/unordered_set.hpp"
#include "parser/column_definition.hpp"
#include "parser/constraint.hpp"
#include "parser/statement/select_statement.hpp"
//#include "catalog/catalog_entry/column_dependency_manager.hpp"

namespace duckdb {

struct CreateTableInfo : public CreateInfo {
	CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY, INVALID_SCHEMA) {
	}
	CreateTableInfo(string schema, string name) : CreateInfo(CatalogType::TABLE_ENTRY, schema), table(name) {
	}

	//! Table name to insert to
	string table;
	//! List of columns of the table
	vector<ColumnDefinition> columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! CREATE TABLE from QUERY
	unique_ptr<SelectStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateTableInfo>(schema, table);
		CopyProperties(*result);
		for (auto &column : columns) {
			result->columns.push_back(column.Copy());
		}
		for (auto &constraint : constraints) {
			result->constraints.push_back(constraint->Copy());
		}
		if (query) {
			result->query = unique_ptr_cast<CypherStatement, SelectStatement>(query->Copy());
		}
		return move(result);
	}
};

} // namespace duckdb
