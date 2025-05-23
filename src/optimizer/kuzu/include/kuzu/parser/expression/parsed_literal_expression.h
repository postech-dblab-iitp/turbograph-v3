#pragma once

#include "kuzu/common/type_utils.h"

namespace kuzu {
namespace parser {

class ParsedLiteralExpression : public ParsedExpression {
public:
    ParsedLiteralExpression(unique_ptr<Literal> literal, string raw)
        : ParsedExpression{LITERAL, std::move(raw)}, literal{std::move(literal)} {}

    inline Literal* getLiteral() const { return literal.get(); }

private:
    unique_ptr<Literal> literal;
};

} // namespace parser
} // namespace kuzu
