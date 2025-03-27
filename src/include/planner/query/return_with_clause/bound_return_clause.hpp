#pragma once


#include "planner/bound_statement_result.hpp"
#include "planner/query/return_with_clause/bound_projection_body.hpp"

namespace duckdb {

class BoundReturnClause {
public:
    explicit BoundReturnClause(std::shared_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{std::move(projectionBody)} {}
    BoundReturnClause(std::shared_ptr<BoundProjectionBody> projectionBody, 
        std::shared_ptr<BoundStatementResult> statementResult)
        : projectionBody{std::move(projectionBody)}, statementResult{std::move(statementResult)} {}
    virtual ~BoundReturnClause() = default;

    inline std::shared_ptr<BoundProjectionBody> getProjectionBody() const { return projectionBody; }

    inline std::shared_ptr<BoundStatementResult> getStatementResult() const { return statementResult; }

protected:
    std::shared_ptr<BoundProjectionBody> projectionBody;
    std::shared_ptr<BoundStatementResult> statementResult;
};

}