#pragma once

#include "kuzu/binder/expression/expression.h"

namespace kuzu {
namespace binder {

class ListComprehensionExpression : public Expression {
   public:
    ListComprehensionExpression(shared_ptr<Expression> filterExpression,
                                shared_ptr<Expression> expr, string name)
        : Expression{LIST_COMPREHENSION, kuzu::common::DataTypeID::LIST, name},
          filterExpression(std::move(filterExpression)),
          expr(std::move(expr))
    {}

    shared_ptr<Expression> getFilterExpression() const { return filterExpression; }
    shared_ptr<Expression> getExpr() const { return expr; }

   private:
    shared_ptr<Expression> filterExpression;
    shared_ptr<Expression> expr;
};

} // namespace binder
} // namespace kuzu
