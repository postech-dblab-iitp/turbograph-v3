#include "parser/statement/transaction_statement.hpp"

namespace duckdb {

TransactionStatement::TransactionStatement(TransactionType type)
    : CypherStatement(StatementType::TRANSACTION_STATEMENT), info(make_unique<TransactionInfo>(type)) {
}

TransactionStatement::TransactionStatement(const TransactionStatement &other)
    : CypherStatement(other), info(make_unique<TransactionInfo>(other.info->type)) {
}

unique_ptr<CypherStatement> TransactionStatement::Copy() const {
	return unique_ptr<TransactionStatement>(new TransactionStatement(*this));
}

} // namespace duckdb
