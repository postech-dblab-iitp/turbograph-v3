//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/transaction_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/cypher_statement.hpp"
#include "parser/parsed_data/transaction_info.hpp"

namespace duckdb {

class TransactionStatement : public CypherStatement {
public:
	explicit TransactionStatement(TransactionType type);

	unique_ptr<TransactionInfo> info;

protected:
	TransactionStatement(const TransactionStatement &other);

public:
	unique_ptr<CypherStatement> Copy() const override;
};
} // namespace duckdb
