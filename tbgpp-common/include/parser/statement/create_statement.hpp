//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "parser/sql_statement.hpp"

namespace s62 {

class CreateStatement : public SQLStatement {
public:
	CreateStatement();

	unique_ptr<CreateInfo> info;

protected:
	CreateStatement(const CreateStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace s62
