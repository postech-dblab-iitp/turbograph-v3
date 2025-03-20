//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/export_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/cypher_statement.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace duckdb {

class ExportStatement : public CypherStatement {
public:
	explicit ExportStatement(unique_ptr<CopyInfo> info);

	unique_ptr<CopyInfo> info;

protected:
	ExportStatement(const ExportStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
