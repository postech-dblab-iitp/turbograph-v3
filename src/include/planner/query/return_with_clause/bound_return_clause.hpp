#pragma once


#include "planner/bound_statement_result.hpp"
#include "planner/query/return_with_clause/bound_projection_body.hpp"

namespace duckdb {

class BoundReturnClause {
public:
    explicit BoundReturnClause(std::unique_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{std::move(projectionBody)} {}
    BoundReturnClause(std::unique_ptr<BoundProjectionBody> projectionBody, 
        std::unique_ptr<BoundStatementResult> statementResult)
        : projectionBody{std::move(projectionBody)}, statementResult{std::move(statementResult)} {}
    virtual ~BoundReturnClause() = default;

    inline const BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    inline const BoundStatementResult* getStatementResult() const { return statementResult.get(); }

protected:
    std::unique_ptr<BoundProjectionBody> projectionBody;
    std::unique_ptr<BoundStatementResult> statementResult;
};

}