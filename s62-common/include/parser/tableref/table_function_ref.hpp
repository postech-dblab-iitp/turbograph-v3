//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/table_function_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/tableref.hpp"
#include "common/vector.hpp"
#include "parser/statement/select_statement.hpp"
//#include "main/external_dependencies.hpp"

namespace s62 {

//! Represents a Table producing function
class TableFunctionRef : public TableRef {
public:
	DUCKDB_API TableFunctionRef();

	unique_ptr<ParsedExpression> function;
	vector<string> column_name_alias;

	// if the function takes a subquery as argument its in here
	unique_ptr<SelectStatement> subquery;

	// External dependencies of this table funcion
	// unique_ptr<ExternalDependency> external_dependency;

public:
	string ToString() const override;

	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a BaseTableRef
	void Serialize(FieldWriter &serializer) const override;
	//! Deserializes a blob back into a BaseTableRef
	static unique_ptr<TableRef> Deserialize(FieldReader &source);
};
} // namespace s62
