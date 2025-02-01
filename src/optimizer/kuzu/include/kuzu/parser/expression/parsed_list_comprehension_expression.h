#pragma once

#include "kuzu/common/type_utils.h"


namespace kuzu {
namespace parser {

class ParsedListComprehensionExpression : public ParsedExpression {
public:
    ParsedListComprehensionExpression(unique_ptr<ParsedExpression> filter_expr, string rawName)
        : ParsedExpression(LIST_COMPREHENSION, std::move(filter_expr), rawName) {}

    inline void setExpression(unique_ptr<ParsedExpression> expression) {
        expression = std::move(expression);
    }
    inline bool hasExpression() const { return expression != nullptr; }
    inline ParsedExpression *getExpression() const { return expression.get(); }
private:
    unique_ptr<ParsedExpression> expression;
};

} // namespace parser
} // namespace kuzu
