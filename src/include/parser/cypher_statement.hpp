#pragma once

#include "common/common.hpp"
#include "common/enums/statement_type.hpp"
#include "common/exception.hpp"

namespace duckdb {

//! CypherStatement is the base class of any type of Cypher statement.
class CypherStatement {
public:
	explicit CypherStatement(StatementType type) : type(type) {};
	virtual ~CypherStatement() = default;

	//! The statement type
	StatementType type;
	//! The statement location within the query string
	idx_t stmt_location = 0;
	//! The statement length within the query string
	idx_t stmt_length = 0;
	//! The number of prepared statement parameters (if any)
	idx_t n_param = 0;
	//! The query text that corresponds to this SQL statement
	string query;

protected:
	CypherStatement(const CypherStatement &other) = default;

public:
	virtual string ToString() const {
		throw InternalException("ToString not supported for this type of CypherStatement");
	}
	//! Create a copy of this Statement
	virtual unique_ptr<CypherStatement> Copy() const = 0;
};
} // namespace duckdb
