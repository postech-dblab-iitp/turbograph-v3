#pragma once

#include "planner/expression.hpp"
#include "common/enums/clause_type.hpp"

namespace duckdb {

class BoundReadingClause {
public:
    explicit BoundReadingClause(ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundReadingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

    void setPredicate(std::shared_ptr<Expression> predicate_) { predicate = std::move(predicate_); }
    bool hasPredicate() const { return predicate != nullptr; }
    std::shared_ptr<Expression> getPredicate() const { return predicate; }

private:
    ClauseType clauseType;
    std::shared_ptr<Expression> predicate;
};

}