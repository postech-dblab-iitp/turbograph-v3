//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/drop_info.hpp"
#include "parser/sql_statement.hpp"

namespace s62 {

class DropStatement : public SQLStatement {
public:
	DropStatement();

	unique_ptr<DropInfo> info;

protected:
	DropStatement(const DropStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace s62
