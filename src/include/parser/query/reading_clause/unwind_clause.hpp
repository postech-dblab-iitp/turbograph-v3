#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/query/reading_clause/reading_clause.hpp"

namespace duckdb {

class UnwindClause : public ReadingClause {
    static constexpr ClauseType clauseType_ = ClauseType::UNWIND;

public:
    UnwindClause(std::unique_ptr<ParsedExpression> expression, std::string listAlias)
        : ReadingClause{clauseType_}, expression{std::move(expression)},
          alias{std::move(listAlias)} {}

    const ParsedExpression* getExpression() const { return expression.get(); }

    std::string getAlias() const { return alias; }

    string ToString() const override {
        return "UNWIND " + expression->ToString() + " AS " + alias;
    }

private:
    std::unique_ptr<ParsedExpression> expression;
    std::string alias;
};

}