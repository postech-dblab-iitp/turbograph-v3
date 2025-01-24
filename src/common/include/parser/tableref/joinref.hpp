//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/join_type.hpp"
#include "common/unordered_set.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/tableref.hpp"
#include "common/vector.hpp"

namespace duckdb {
//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
public:
	JoinRef() : TableRef(TableReferenceType::JOIN), is_natural(false) {
	}

	//! The left hand side of the join
	unique_ptr<TableRef> left;
	//! The right hand side of the join
	unique_ptr<TableRef> right;
	//! The join condition
	unique_ptr<ParsedExpression> condition;
	//! The join type
	JoinType type;
	//! Natural join
	bool is_natural;
	//! The set of USING columns (if any)
	vector<string> using_columns;

public:
	string ToString() const override;
	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a JoinRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a JoinRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);
};
} // namespace duckdb
