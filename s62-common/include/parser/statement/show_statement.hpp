//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/show_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.hpp"
#include "parser/parsed_data/show_select_info.hpp"

namespace s62 {

class ShowStatement : public SQLStatement {
public:
	ShowStatement();

	unique_ptr<ShowSelectInfo> info;

protected:
	ShowStatement(const ShowStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace s62
