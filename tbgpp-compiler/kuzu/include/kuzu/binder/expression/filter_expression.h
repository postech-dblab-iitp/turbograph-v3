#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

class FilterExpression : public Expression {
   public:
    FilterExpression(shared_ptr<Expression> id_in_coll_expression,
                                shared_ptr<Expression> where_expression, string name)
        : Expression{FILTER, kuzu::common::BOOLEAN, name}, // TODO data type?
          id_in_coll_expression(std::move(id_in_coll_expression)),
          where_expression(std::move(where_expression))
    {}
    
    shared_ptr<Expression> getIdInCollExpression() const { return id_in_coll_expression; }
    shared_ptr<Expression> getWhereExpression() const { return where_expression; }

   private:
    shared_ptr<Expression> id_in_coll_expression;
    shared_ptr<Expression> where_expression;
};

} // namespace binder
} // namespace kuzu
