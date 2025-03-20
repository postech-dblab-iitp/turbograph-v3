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
#include "parser/cypher_statement.hpp"

namespace duckdb {

class AlterStatement : public CypherStatement {
public:
	AlterStatement();

	unique_ptr<AlterInfo> info;

protected:
	AlterStatement(const AlterStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
