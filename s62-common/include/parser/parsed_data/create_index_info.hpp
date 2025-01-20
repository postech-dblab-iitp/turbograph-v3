//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/enums/index_type.hpp"
#include "common/vector.hpp"
#include "parser/tableref/basetableref.hpp"
#include "parser/parsed_expression.hpp"

namespace s62 {

struct CreateIndexInfo : public CreateInfo {
	CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY) {
	}

	CreateIndexInfo(string schema, string name, IndexType itype, idx_t partition_oid, idx_t propertyschema_oid, idx_t adj_col_idx, vector<int64_t> idx_column_ids) : CreateInfo(CatalogType::INDEX_ENTRY, schema), 
		index_name(name), index_type(itype), partition_oid(partition_oid), propertyschema_oid(propertyschema_oid), adj_col_idx(adj_col_idx) {
		column_ids = move(idx_column_ids);
	}

	//! Index Type (e.g., B+-tree, Skip-List, ...)
	IndexType index_type;
	//! Name of the Index
	string index_name;
	//! Index Constraint Type
	// IndexConstraintType constraint_type;
	//! The table to create the index on
	// unique_ptr<BaseTableRef> table;
	//! Set of expressions to index by
	// vector<unique_ptr<ParsedExpression>> expressions;
	// vector<unique_ptr<ParsedExpression>> parsed_expressions;
	//! Partition OID
	idx_t partition_oid;
	idx_t propertyschema_oid;
	idx_t adj_col_idx;

	vector<int64_t> column_ids;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateIndexInfo>();
		CopyProperties(*result);
		result->index_type = index_type;
		result->index_name = index_name;
		// result->constraint_type = constraint_type;
		// result->table = unique_ptr_cast<TableRef, BaseTableRef>(table->Copy());
		// for (auto &expr : expressions) {
		// 	result->expressions.push_back(expr->Copy());
		// }
		result->column_ids = column_ids;
		return move(result);
	}
};

} // namespace s62
