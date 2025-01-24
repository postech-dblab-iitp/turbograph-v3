#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class IdInCollExpression : public Expression {
   public:
    IdInCollExpression(string variable_name,
                                shared_ptr<Expression> expr, string name)
        : Expression{LIST_COMPREHENSION, kuzu::common::ANY, name},
          variable_name(variable_name),
          expr(std::move(expr))
    {}

    string getVariableName() const { return variable_name; }
    shared_ptr<Expression> getExpr() const { return expr; }

   private:
    string variable_name;
    shared_ptr<Expression> expr;
};

} // namespace binder
} // namespace kuzu
