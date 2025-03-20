//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/show_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/cypher_statement.hpp"
#include "parser/parsed_data/show_select_info.hpp"

namespace duckdb {

class ShowStatement : public CypherStatement {
public:
	ShowStatement();

	unique_ptr<ShowSelectInfo> info;

protected:
	ShowStatement(const ShowStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};

} // namespace duckdb
