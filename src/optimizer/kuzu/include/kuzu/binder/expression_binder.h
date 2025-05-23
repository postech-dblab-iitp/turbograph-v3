#pragma once

#include "kuzu/binder/expression/expression.h"
#include "kuzu/catalog/catalog_structs.h"
#include "kuzu/common/types/literal.h"
#include "kuzu/parser/expression/parsed_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

class Binder;
class CaseAlternative;

class ExpressionBinder {
    friend class Binder;

public:
    explicit ExpressionBinder(Binder* queryBinder) : binder{queryBinder} {}

    shared_ptr<Expression> bindExpression(const ParsedExpression& parsedExpression);

private:
    shared_ptr<Expression> bindBooleanExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindBooleanExpression(
        ExpressionType expressionType, const expression_vector& children);

    shared_ptr<Expression> bindComparisonExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindComparisonExpression(
        ExpressionType expressionType, const expression_vector& children);

    shared_ptr<Expression> bindNullOperatorExpression(const ParsedExpression& parsedExpression);

    // bind to an existing property expression.
    shared_ptr<Expression> bindPropertyExpression(const ParsedExpression& parsedExpression);
    // bind to an existing property expression of given node table.
    shared_ptr<Expression> bindNodePropertyExpression(
        const Expression& expression, const string& propertyName);
    shared_ptr<Expression> bindRelPropertyExpression(
        const Expression& expression, const string& propertyName);
    unique_ptr<Expression> createPropertyExpression(
        Expression &nodeOrRel, Property &anchorProperty,
        unordered_map<table_id_t, property_id_t> &propertyIDPerTable,
        uint64_t prop_key_id = 0);

    shared_ptr<Expression> bindFunctionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindScalarFunctionExpression(
        const ParsedExpression& parsedExpression, const string& functionName);
    shared_ptr<Expression> bindAggregateFunctionExpression(
        const ParsedExpression& parsedExpression, const string& functionName, bool isDistinct);

    shared_ptr<Expression> staticEvaluate(const string& functionName,
        const ParsedExpression& parsedExpression, const expression_vector& children);

    shared_ptr<Expression> bindInternalIDExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindInternalIDExpression(const Expression& expression);
    unique_ptr<Expression> createInternalNodeIDExpression(const Expression& node);

    shared_ptr<Expression> bindParameterExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindLiteralExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindNullLiteralExpression();

    shared_ptr<Expression> bindVariableExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindExistentialSubqueryExpression(
        const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindCaseExpression(const ParsedExpression& parsedExpression);

    shared_ptr<Expression> bindListComprehensionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindPatternComprehensionExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindFilterExpression(const ParsedExpression& parsedExpression);
    shared_ptr<Expression> bindIdInCollExpression(const ParsedExpression& parsedExpression);

    /****** cast *****/
    // Note: we expose two implicitCastIfNecessary interfaces.
    // For function binding we cast with data type ID because function definition cannot be
    // recursively generated, e.g. list_extract(param) we only declare param with type DataTypeID::LIST but do
    // not specify its child type.
    // For the rest, i.e. set clause binding, we cast with data type. For example, a.list = $1.
    static shared_ptr<Expression> implicitCastIfNecessary(
        const shared_ptr<Expression>& expression, DataType &targetType);
    static shared_ptr<Expression> implicitCastIfNecessary(
        const shared_ptr<Expression>& expression, DataTypeID targetTypeID);
    static void resolveAnyDataType(Expression& expression, DataType targetType);
    static shared_ptr<Expression> implicitCast(
        const shared_ptr<Expression>& expression, DataType targetType);

    /****** validation *****/
    static void validateExpectedDataType(const Expression& expression, DataTypeID target) {
        validateExpectedDataType(expression, unordered_set<DataTypeID>{target});
    }
    static void validateExpectedDataType(
        const Expression& expression, const unordered_set<DataTypeID>& targets);
    // E.g. SUM(SUM(a.age)) is not allowed
    static void validateAggregationExpressionIsNotNested(const Expression& expression);

private:
    Binder* binder;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
    uint64_t currentORGroupID = 0;
};

} // namespace binder
} // namespace kuzu
