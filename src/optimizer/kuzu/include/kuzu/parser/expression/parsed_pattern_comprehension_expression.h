#pragma once

#include "kuzu/common/type_utils.h"
#include "kuzu/parser/query/reading_clause/match_clause.h"


namespace kuzu {
namespace parser {

class ParsedPatternComprehensionExpression : public ParsedExpression {
public:
    ParsedPatternComprehensionExpression(unique_ptr<MatchClause> match_clause, unique_ptr<ParsedExpression> expr, string rawName)
        : ParsedExpression(PATTERN_COMPREHENSION, nullptr, rawName), match_clause(std::move(match_clause)), expression(std::move(expr)) {}

    inline void setExpression(unique_ptr<ParsedExpression> expression) {
        expression = std::move(expression);
    }
    inline bool hasExpression() const { return expression != nullptr; }
    inline ParsedExpression *getExpression() const { return expression.get(); }
private:
    unique_ptr<MatchClause> match_clause;
    unique_ptr<ParsedExpression> expression;
};

} // namespace parser
} // namespace kuzu
