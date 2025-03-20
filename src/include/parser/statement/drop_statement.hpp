//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/drop_info.hpp"
#include "parser/cypher_statement.hpp"

namespace duckdb {

class DropStatement : public CypherStatement {
public:
	DropStatement();

	unique_ptr<DropInfo> info;

protected:
	DropStatement(const DropStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
