//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/alter_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/sql_statement.hpp"

namespace s62 {

class AlterStatement : public SQLStatement {
public:
	AlterStatement();

	unique_ptr<AlterInfo> info;

protected:
	AlterStatement(const AlterStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace s62
