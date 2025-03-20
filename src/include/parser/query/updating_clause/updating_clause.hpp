#pragma once

#include "common/enums/clause_type.hpp"
#include "common/exception.hpp"

namespace duckdb {

class UpdatingClause {
public:
    explicit UpdatingClause(ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~UpdatingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

    std::string ToString() const {
        throw ParserException("ToString not supported for this type of UpdatingClause");
    }

private:
    ClauseType clauseType;
};

}