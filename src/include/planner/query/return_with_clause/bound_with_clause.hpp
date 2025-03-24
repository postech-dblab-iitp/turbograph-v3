#pragma once

#include "planner/query/return_with_clause/bound_return_clause.hpp"

namespace duckdb {

class BoundWithClause final : public BoundReturnClause {
public:
    explicit BoundWithClause(std::unique_ptr<BoundProjectionBody> projectionBody)
        : BoundReturnClause{std::move(projectionBody)} {}

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

private:
    std::shared_ptr<Expression> whereExpression;
};

}