//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "parser/cypher_statement.hpp"

namespace duckdb {

class CreateStatement : public CypherStatement {
public:
	CreateStatement();

	unique_ptr<CreateInfo> info;

protected:
	CreateStatement(const CreateStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
