#pragma once

#include "common/enums/clause_type.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {

class ReadingClause {
public:
    explicit ReadingClause(ClauseType clauseType) : clauseType{clauseType} {};
    virtual ~ReadingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

    void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    bool hasWherePredicate() const { return wherePredicate != nullptr; }
    const ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

    virtual std::string ToString() const {
        string result = ClauseTypeToString(clauseType);

        if (wherePredicate) {
            result += " WHERE " + wherePredicate->ToString();
        }

        return result;
    }


private:
    ClauseType clauseType;
    std::unique_ptr<ParsedExpression> wherePredicate;
};
} 
