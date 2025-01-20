//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/transaction_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.hpp"
#include "parser/parsed_data/transaction_info.hpp"

namespace s62 {

class TransactionStatement : public SQLStatement {
public:
	explicit TransactionStatement(TransactionType type);

	unique_ptr<TransactionInfo> info;

protected:
	TransactionStatement(const TransactionStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace s62
