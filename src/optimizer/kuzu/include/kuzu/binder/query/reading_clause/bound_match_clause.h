#pragma once

#include "kuzu/binder/expression/expression.h"
#include "kuzu/binder/query/reading_clause/bound_reading_clause.h"
#include "kuzu/binder/query/reading_clause/query_graph.h"

namespace kuzu {
namespace binder {

/**
 * BoundMatchClause may not have whereExpression
 */
class BoundMatchClause : public BoundReadingClause {

public:
    explicit BoundMatchClause(
        unique_ptr<QueryGraphCollection> queryGraphCollection, bool isOptional)
        : BoundReadingClause{ClauseType::MATCH},
          queryGraphCollection{std::move(queryGraphCollection)}, isOptional{isOptional} {}

    BoundMatchClause(const BoundMatchClause& other)
        : BoundReadingClause(ClauseType::MATCH),
          queryGraphCollection{other.queryGraphCollection->copy()},
          whereExpression(other.whereExpression), isOptional{other.isOptional} {}

    ~BoundMatchClause() = default;

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

    inline bool getIsOptional() const { return isOptional; }

    inline expression_vector getSubPropertyExpressions() const override {
        expression_vector expressions;
        for (auto& rel : queryGraphCollection->getQueryRels()) {
            if (rel->hasInternalIDProperty()) {
                expressions.push_back(rel->getInternalIDProperty());
            }
        }
        if (this->hasWhereExpression()) {
            for (auto& property : this->getWhereExpression()->getSubPropertyExpressions()) {
                expressions.push_back(property);
            }
        }
        return expressions;
    }

    inline unique_ptr<BoundReadingClause> copy() override {
        return make_unique<BoundMatchClause>(*this);
    }

    std::string getName() override { return "[BoundMatchClause]"; }
    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        result.push_back((ParseTreeNode*)queryGraphCollection.get());
        //result.push_back((ParseTreeNode*)whereExpression.get());
        return result;
    }

private:
    unique_ptr<QueryGraphCollection> queryGraphCollection;
    shared_ptr<Expression> whereExpression;
    bool isOptional;
};

} // namespace binder
} // namespace kuzu
