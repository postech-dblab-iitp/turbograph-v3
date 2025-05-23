#pragma once

#include "kuzu/binder/query/reading_clause/bound_match_clause.h"
#include "kuzu/binder/expression/expression.h"

namespace kuzu {
namespace binder {

class ExistentialSubqueryExpression : public Expression {
public:
    ExistentialSubqueryExpression(
        unique_ptr<QueryGraphCollection> queryGraphCollection, const string& name)
        : Expression{EXISTENTIAL_SUBQUERY, kuzu::common::DataTypeID::BOOLEAN, name}, queryGraphCollection{
                                                            std::move(queryGraphCollection)} {}

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }
    inline bool hasWhereExpression() const { return whereExpression != nullptr; }
    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    expression_vector getChildren() const override;

private:
    unique_ptr<QueryGraphCollection> queryGraphCollection;
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace kuzu
