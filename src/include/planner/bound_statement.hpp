#pragma once

#include "common/enums/statement_type.hpp"
#include "common/exception.hpp"
#include "planner/bound_statement_result.hpp"

namespace duckdb {

class BoundStatement {
public:
    BoundStatement(StatementType type, std::shared_ptr<BoundStatementResult> statementResult)
        : type{type}, statementResult{std::move(statementResult)} {}

    virtual ~BoundStatement() = default;

    StatementType getStatementType() const { return type; }

    std::shared_ptr<BoundStatementResult> getStatementResult() const { return statementResult; }

    BoundStatementResult* getStatementResultUnsafe() { return statementResult.get(); }

private:
    StatementType type;
    std::shared_ptr<BoundStatementResult> statementResult;
};

}