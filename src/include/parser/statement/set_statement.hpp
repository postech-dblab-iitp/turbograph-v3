//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/set_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/set_scope.hpp"
#include "parser/sql_statement.hpp"
#include "common/types/value.hpp"

namespace duckdb {

class SetStatement : public SQLStatement {
public:
	SetStatement(std::string name_p, Value value_p, SetScope scope_p);

protected:
	SetStatement(const SetStatement &other) = default;

public:
	unique_ptr<SQLStatement> Copy() const override;

	std::string name;
	Value value;
	SetScope scope;
};
} // namespace duckdb
