//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/load_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/cypher_statement.hpp"
#include "parser/parsed_data/load_info.hpp"

namespace duckdb {

class LoadStatement : public CypherStatement {
public:
	LoadStatement();

protected:
	LoadStatement(const LoadStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;

	unique_ptr<LoadInfo> info;
};
} // namespace duckdb
