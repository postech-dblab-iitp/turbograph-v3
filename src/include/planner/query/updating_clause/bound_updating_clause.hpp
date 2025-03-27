#pragma once

#include "common/enums/clause_type.hpp"

namespace duckdb {

class BoundUpdatingClause {
public:
    explicit BoundUpdatingClause(ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundUpdatingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

private:
    ClauseType clauseType;
};

}