#pragma once

#include "planner/expression.hpp"
#include "planner/query/reading_clause/bound_reading_clause.hpp"

namespace duckdb {

class BoundUnwindClause final : public BoundReadingClause {
public:
    BoundUnwindClause(std::shared_ptr<Expression> inExpr, std::shared_ptr<Expression> outExpr,
        std::shared_ptr<Expression> idExpr)
        : BoundReadingClause{ClauseType::UNWIND}, inExpr{std::move(inExpr)},
          outExpr{std::move(outExpr)}, idExpr{std::move(idExpr)} {}

    std::shared_ptr<Expression> getInExpr() const { return inExpr; }
    std::shared_ptr<Expression> getOutExpr() const { return outExpr; }
    std::shared_ptr<Expression> getIDExpr() const { return idExpr; }

private:
    std::shared_ptr<Expression> inExpr;
    std::shared_ptr<Expression> outExpr;
    std::shared_ptr<Expression> idExpr;
};

}