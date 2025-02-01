#pragma once

#include "kuzu/binder/bound_statement_result.h"
#include "kuzu/binder/query/return_with_clause/bound_projection_body.h"

namespace kuzu {
namespace binder {

class BoundReturnClause {
public:
    explicit BoundReturnClause(unique_ptr<BoundProjectionBody> projectionBody)
        : projectionBody{std::move(projectionBody)}, statementResult{nullptr} {}
    BoundReturnClause(unique_ptr<BoundProjectionBody> projectionBody,
        unique_ptr<BoundStatementResult> statementResult)
        : projectionBody{std::move(projectionBody)}, statementResult{std::move(statementResult)} {}
    virtual ~BoundReturnClause() = default;

    inline BoundProjectionBody* getProjectionBody() { return projectionBody.get(); }

    inline BoundStatementResult* getStatementResult() const { return statementResult.get(); }

protected:
    unique_ptr<BoundProjectionBody> projectionBody;
    unique_ptr<BoundStatementResult> statementResult;
};

} // namespace binder
} // namespace kuzu
