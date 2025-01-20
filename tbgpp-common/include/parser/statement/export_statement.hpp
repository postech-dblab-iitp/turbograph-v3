//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/export_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace s62 {

class ExportStatement : public SQLStatement {
public:
	explicit ExportStatement(unique_ptr<CopyInfo> info);

	unique_ptr<CopyInfo> info;

protected:
	ExportStatement(const ExportStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace s62
