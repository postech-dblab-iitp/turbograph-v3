#pragma once

#include "kuzu/binder/query/return_with_clause/bound_return_clause.h"

namespace kuzu {
namespace binder {

/**
 * BoundWithClause may not have whereExpression
 */
class BoundWithClause : public BoundReturnClause {
public:
    explicit BoundWithClause(unique_ptr<BoundProjectionBody> projectionBody)
        : BoundReturnClause{std::move(projectionBody)} {}

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

private:
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace kuzu
