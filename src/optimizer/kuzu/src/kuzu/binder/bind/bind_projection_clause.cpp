#include "kuzu/binder/binder.h"
#include "kuzu/binder/expression/literal_expression.h"

namespace kuzu {
namespace binder {

unique_ptr<BoundWithClause> Binder::bindWithClause(const WithClause& withClause) {
    auto projectionBody = withClause.getProjectionBody();
    auto boundProjectionExpressions = bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->containsStar());
    validateProjectionColumnsInWithClauseAreAliased(boundProjectionExpressions);
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), std::move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    // validateOrderByFollowedBySkipOrLimitInWithClause(*boundProjectionBody);
    variablesInScope.clear();
    addExpressionsToScope(boundProjectionBody->getProjectionExpressions());
    auto boundWithClause = make_unique<BoundWithClause>(std::move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause->setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

unique_ptr<BoundReturnClause> Binder::bindReturnClause(const ReturnClause& returnClause) {
    auto projectionBody = returnClause.getProjectionBody();
    auto boundProjectionExpressions = bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->containsStar());
    validateProjectionColumnHasNoInternalType(boundProjectionExpressions);
    // expand node/rel to all of its properties.
    auto statementResult = make_unique<BoundStatementResult>();
    for (auto& expression : boundProjectionExpressions) {
        auto dataType = expression->getDataType();
        if (dataType.typeID == common::DataTypeID::NODE || dataType.typeID == common::DataTypeID::REL) {
            // statementResult->addColumn(expression, rewriteNodeOrRelExpression(*expression));
            statementResult->addColumn(expression, expression_vector{expression});
            auto &nodeOrRel = (NodeOrRelExpression &)(*expression);
            nodeOrRel.markAllColumnsAsUsed();
        }
        else if (dataType.typeID == common::DataTypeID::PATH ) {
            statementResult->addColumn(expression, rewritePathExpression(*expression));
        }
        else {
            statementResult->addColumn(expression, expression_vector{expression});
        }
    }
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), statementResult->getExpressionsToCollect());
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    return make_unique<BoundReturnClause>(
        std::move(boundProjectionBody), std::move(statementResult));
}

expression_vector Binder::bindProjectionExpressions(
    const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool containsStar) {
    expression_vector boundProjectionExpressions;
    for (auto& expression : projectionExpressions) {
        boundProjectionExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    if (containsStar) {
        if (variablesInScope.empty()) {
            throw BinderException(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& pair : variablesInScope) {
            auto& name = pair.first;
            auto& expression = pair.second;
            boundProjectionExpressions.push_back(expression);
        }
    }
    resolveAnyDataTypeWithDefaultType(boundProjectionExpressions);
    validateProjectionColumnNamesAreUnique(boundProjectionExpressions);
    return boundProjectionExpressions;
}

expression_vector Binder::rewriteNodeOrRelExpression(const Expression& expression) {
    expression_vector result;
    auto &nodeOrRel = (NodeOrRelExpression &)expression;
    nodeOrRel.markAllColumnsAsUsed();
    for (auto &property : nodeOrRel.getPropertyExpressions()) {
        result.push_back(property->copy());
    }
    return result;
}

expression_vector Binder::rewritePathExpression(const Expression& expression) {
    expression_vector result;
    auto& path = (PathExpression&)expression;
    // Node
    for (auto& nodeExpr: path.getQueryNodes()) {
        auto nodeOrRel = (NodeOrRelExpression*)nodeExpr.get();
        // _id columns only
        result.push_back(nodeOrRel->getPropertyExpressions()[0]->copy());
    }
    // // Length
    // auto length_property = make_shared<PropertyExpression>(DataTypeID::UBIGINT, "length", 0, path);

    // Edge
    for (auto& relExpr: path.getQueryRels()) {
        auto nodeOrRel = (NodeOrRelExpression*)relExpr.get();
        for (auto& property : nodeOrRel->getPropertyExpressions()) {
            result.push_back(property->copy());
        }
    }
    return result;
}

void Binder::bindOrderBySkipLimitIfNecessary(
    BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody) {
    if (projectionBody.hasOrderByExpressions()) {
        // Cypher rule of ORDER BY expression scope: if projection contains aggregation, only
        // expressions in projection are available. Otherwise, expressions before projection are
        // also available
        if (boundProjectionBody.hasAggregationExpressions()) {
            variablesInScope.clear();
        }
        addExpressionsToScope(boundProjectionBody.getProjectionExpressions());
        boundProjectionBody.setOrderByExpressions(
            bindOrderByExpressions(projectionBody.getOrderByExpressions()),
            projectionBody.getSortOrders());
    }
    if (projectionBody.hasSkipExpression()) {
        boundProjectionBody.setSkipNumber(
            bindSkipLimitExpression(*projectionBody.getSkipExpression()));
    }
    if (projectionBody.hasLimitExpression()) {
        boundProjectionBody.setLimitNumber(
            bindSkipLimitExpression(*projectionBody.getLimitExpression()));
    }
}

expression_vector Binder::bindOrderByExpressions(
    const vector<unique_ptr<ParsedExpression>>& orderByExpressions) {
    expression_vector boundOrderByExpressions;
    for (auto& expression : orderByExpressions) {
        auto boundExpression = expressionBinder.bindExpression(*expression);
        if (boundExpression->dataType.typeID == DataTypeID::NODE || boundExpression->dataType.typeID == DataTypeID::REL) {
            throw BinderException("Cannot order by " + boundExpression->getRawName() +
                                  ". Order by node or rel is not supported.");
        }
        boundOrderByExpressions.push_back(std::move(boundExpression));
    }
    resolveAnyDataTypeWithDefaultType(boundOrderByExpressions);
    return boundOrderByExpressions;
}

uint64_t Binder::bindSkipLimitExpression(const ParsedExpression& expression) {
    auto boundExpression = expressionBinder.bindExpression(expression);
    // We currently do not support the number of rows to skip/limit written as an expression (eg.
    // SKIP 3 + 2 is not supported).
    if (expression.getExpressionType() == LITERAL &&
        (((LiteralExpression&)(*boundExpression)).getDataType().typeID == DataTypeID::UINTEGER
        || ((LiteralExpression&)(*boundExpression)).getDataType().typeID == DataTypeID::INTEGER)) {
        return ((LiteralExpression&)(*boundExpression)).literal->val.uint32Val;
    }

    if (expression.getExpressionType() == LITERAL &&
        (((LiteralExpression&)(*boundExpression)).getDataType().typeID == DataTypeID::UBIGINT
        || ((LiteralExpression&)(*boundExpression)).getDataType().typeID == DataTypeID::INT64)) {
        return ((LiteralExpression&)(*boundExpression)).literal->val.uint64Val;
    }

    throw BinderException("The number of rows to skip/limit must be a non-negative integer.");
}

void Binder::addExpressionsToScope(const expression_vector& projectionExpressions) {
    for (auto& expression : projectionExpressions) {
        // In RETURN clause, if expression is not aliased, its input name will serve its alias.
        auto alias = expression->hasAlias() ? expression->getAlias() : expression->getRawName();
        variablesInScope.insert({alias, expression});
    }
}

void Binder::resolveAnyDataTypeWithDefaultType(const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == DataTypeID::ANY) {
            ExpressionBinder::implicitCastIfNecessary(expression, DataTypeID::STRING);
        }
    }
}

} // namespace binder
} // namespace kuzu
