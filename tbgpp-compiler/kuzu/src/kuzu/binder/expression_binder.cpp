#include "binder/expression_binder.h"

#include "binder/binder.h"
#include "binder/expression/case_expression.h"
#include "binder/expression/existential_subquery_expression.h"
#include "binder/expression/function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
#include "binder/expression/rel_expression.h"
#include "common/type_utils.h"
#include "common/utils.h"

#include "function/vector_operations.h"


// #include "function/boolean/vector_boolean_operations.h"
// #include "function/null/vector_null_operations.h"
#include "parser/expression/parsed_case_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/expression/parsed_parameter_expression.h"
#include "parser/expression/parsed_property_expression.h"
#include "parser/expression/parsed_subquery_expression.h"
#include "parser/expression/parsed_variable_expression.h"

// using namespace kuzu::function;

namespace kuzu {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    shared_ptr<Expression> expression;
    auto expressionType = parsedExpression.getExpressionType();
    if (isExpressionBoolConnection(expressionType)) {
        expression = bindBooleanExpression(parsedExpression);
    } else if (isExpressionComparison(expressionType)) {
        expression = bindComparisonExpression(parsedExpression);
    } else if (isExpressionNullOperator(expressionType)) {
        expression = bindNullOperatorExpression(parsedExpression);
    } else if (FUNCTION == expressionType) {
        expression = bindFunctionExpression(parsedExpression);
    } else if (PROPERTY == expressionType) {
        expression = bindPropertyExpression(parsedExpression);
    } else if (PARAMETER == expressionType) {
        expression = bindParameterExpression(parsedExpression);
    } else if (isExpressionLiteral(expressionType)) {
        expression = bindLiteralExpression(parsedExpression);
    } else if (VARIABLE == expressionType) {
        expression = bindVariableExpression(parsedExpression);
    } else if (EXISTENTIAL_SUBQUERY == expressionType) {
        expression = bindExistentialSubqueryExpression(parsedExpression);
    } else if (CASE_ELSE == expressionType) {
        expression = bindCaseExpression(parsedExpression);
    } else {
        throw NotImplementedException(
            "bindExpression(" + expressionTypeToString(expressionType) + ").");
    }
    if (parsedExpression.hasAlias()) {
        expression->setAlias(parsedExpression.getAlias());
    }
    expression->setRawName(parsedExpression.getRawName());
    if (isExpressionAggregate(expression->expressionType)) {
        validateAggregationExpressionIsNotNested(*expression);
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    return bindBooleanExpression(parsedExpression.getExpressionType(), children);
}

shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
   ExpressionType expressionType, const expression_vector& children) {
    
    expression_vector childrenAfterCast;
    for (auto& child : children) {
        childrenAfterCast.push_back(implicitCastIfNecessary(child, BOOL));
    }
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = empty_scalar_exec_func();
    auto selectFunc = empty_scalar_select_func();
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(functionName, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(BOOL),
        move(childrenAfterCast), move(execFunc), move(selectFunc), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        children.push_back(move(child));
    }
    return bindComparisonExpression(parsedExpression.getExpressionType(), std::move(children));
}

shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    ExpressionType expressionType, const expression_vector& children) {

    auto builtInFunctions = binder->builtInVectorOperations.get();
    auto functionName = expressionTypeToString(expressionType);
    vector<DataType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }

    bool isComparsionOnTwoNodeOrEdges = false;
    if( (expressionType == ExpressionType::EQUALS || expressionType == ExpressionType::NOT_EQUALS) 
        && (children.size() == 2)
        && (
            (children[0].get()->dataType.typeID == NODE && children[1].get()->dataType.typeID == NODE)
            || (children[0].get()->dataType.typeID == REL && children[1].get()->dataType.typeID == REL)
        )
    ) {
        isComparsionOnTwoNodeOrEdges = true;
        // change childrenTypes
        auto child = bindInternalIDExpression(*children[0]);
        childrenTypes.clear();
        childrenTypes.push_back(child->dataType);
        childrenTypes.push_back(child->dataType);
    }

    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        if(isComparsionOnTwoNodeOrEdges) {
            // rewrite x = y on node or rel as comp on their internal IDs.
            auto child = bindInternalIDExpression(*children[i]);
            childrenAfterCast.push_back(
                implicitCastIfNecessary(child, function->parameterTypeIDs[i]));
        } else {
            childrenAfterCast.push_back(
                implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));
        }
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(function->returnTypeID),
        move(childrenAfterCast), function->execFunc, function->selectFunc, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindNullOperatorExpression(
    const ParsedExpression& parsedExpression) {

// TODO jhko do nothing
        return make_shared<Expression>(kuzu::common::ExpressionType::VARIABLE, kuzu::common::DataType(), "EMPTY_EXPRESSION");

    // expression_vector children;
    // for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
    //     children.push_back(bindExpression(*parsedExpression.getChild(i)));
    // }
    // auto expressionType = parsedExpression.getExpressionType();
    // auto functionName = expressionTypeToString(expressionType);
    // auto execFunc = VectorNullOperations::bindExecFunction(expressionType, children);
    // auto selectFunc = VectorNullOperations::bindSelectFunction(expressionType, children);
    // auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    // return make_shared<ScalarFunctionExpression>(expressionType, DataType(BOOL), move(children),
    //     move(execFunc), move(selectFunc), uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
    auto& propertyExpression = (ParsedPropertyExpression&)parsedExpression;
    auto propertyName = propertyExpression.getPropertyName();
    
// TODO s62 disabled
    // if (TableSchema::isReservedPropertyName(propertyName)) {
    //     // Note we don't expose direct access to internal properties in case user tries to modify
    //     // them. However, we can expose indirect read-only access through function e.g. ID().
    //     throw BinderException(
    //         propertyName + " is reserved for system usage. External access is not allowed.");
    // }
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE, REL});
    if (NODE == child->dataType.typeID) {
        return bindNodePropertyExpression(*child, propertyName);
    } else {
        assert(REL == child->dataType.typeID);
        return bindRelPropertyExpression(*child, propertyName);
    }
}

shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    const Expression& expression, const string& propertyName) {
    auto& nodeOrRel = (NodeOrRelExpression&)expression;
    if (!nodeOrRel.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + expression.getRawName() + ".");
    }
    return nodeOrRel.getPropertyExpression(propertyName);
}

static void validatePropertiesWithSameDataType(const vector<Property>& properties,
    const DataType& dataType, const string& propertyName, const string& variableName) {
    for (auto& property : properties) {
        if (property.dataType != dataType) {
            throw BinderException(
                "Cannot resolve data type of " + propertyName + " for " + variableName + ".");
        }
    }
}

static unordered_map<table_id_t, property_id_t> populatePropertyIDPerTable(
    const vector<Property>& properties) {
    unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto& property : properties) {
        propertyIDPerTable.insert({property.tableID, property.propertyID});
    }
    return propertyIDPerTable;
}

shared_ptr<Expression> ExpressionBinder::bindRelPropertyExpression(
    const Expression& expression, const string& propertyName) {
    auto& rel = (RelExpression&)expression;
    if (rel.isVariableLength()) {
        throw BinderException(
            "Cannot read property of variable length rel " + rel.getRawName() + ".");
    }
    if (!rel.hasPropertyExpression(propertyName)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + expression.getRawName() + ".");
    }
    return rel.getPropertyExpression(propertyName);
}

unique_ptr<Expression> ExpressionBinder::createPropertyExpression(
    const Expression& nodeOrRel, const vector<Property>& properties) {
    assert(!properties.empty());
    auto anchorProperty = properties[0];

// conform data type between multi table access
    validatePropertiesWithSameDataType(
        properties, anchorProperty.dataType, anchorProperty.name, nodeOrRel.getRawName());
    return make_unique<PropertyExpression>(anchorProperty.dataType, anchorProperty.name, nodeOrRel,
        populatePropertyIDPerTable(properties));
}

shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& parsedExpression) {
        
    auto& parsedFunctionExpression = (ParsedFunctionExpression&)parsedExpression;
    auto functionName = parsedFunctionExpression.getFunctionName();
    StringUtils::toUpper(functionName);
    auto functionType = binder->getFunctionType(functionName);
    if (functionType == FUNCTION) {
        return bindScalarFunctionExpression(parsedExpression, functionName);
    } else {
        assert(functionType == AGGREGATE_FUNCTION);
        return bindAggregateFunctionExpression(
            parsedExpression, functionName, parsedFunctionExpression.getIsDistinct());
    }
}

shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const string& functionName) {

    auto builtInFunctions = binder->builtInVectorOperations.get();
    vector<DataType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        childrenTypes.push_back(child->dataType);
        children.push_back(move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes);
    if (builtInFunctions->canApplyStaticEvaluation(functionName, children)) {
        return staticEvaluate(functionName, parsedExpression, children);
    }
    expression_vector childrenAfterCast;
    for (auto i = 0u; i < children.size(); ++i) {
        auto targetType =
            function->isVarLength ? function->parameterTypeIDs[0] : function->parameterTypeIDs[i];
        childrenAfterCast.push_back(implicitCastIfNecessary(children[i], targetType));
    }
    DataType returnType;
    if (function->bindFunc) {
        function->bindFunc(childrenTypes, function, returnType);
    } else {
        returnType = DataType(function->returnTypeID);
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(FUNCTION, returnType, move(childrenAfterCast),
        function->execFunc, function->selectFunc, uniqueExpressionName, function->name);
}

shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const string& functionName, bool isDistinct) {

    auto builtInFunctions = binder->builtInAggregateFunctions.get();
    vector<DataType> childrenTypes;
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        auto child = bindExpression(*parsedExpression.getChild(i));
        // rewrite aggregate on node or rel as aggregate on their internal IDs.
        // e.g. COUNT(a) -> COUNT(a._id)
        if (child->dataType.typeID == NODE || child->dataType.typeID == REL) {
            child = bindInternalIDExpression(*child);
        }
        childrenTypes.push_back(child->dataType);
        children.push_back(std::move(child));
    }
    auto function = builtInFunctions->matchFunction(functionName, childrenTypes, isDistinct);
    auto uniqueExpressionName =
        AggregateFunctionExpression::getUniqueName(function->name, children, function->isDistinct);
    if (children.empty()) {
        uniqueExpressionName = binder->getUniqueExpressionName(uniqueExpressionName);
    }
    return make_shared<AggregateFunctionExpression>(DataType(function->returnTypeID),
        move(children), function->aggregateFunction->clone(), uniqueExpressionName, functionName);
}

shared_ptr<Expression> ExpressionBinder::staticEvaluate(const string& functionName,
    const ParsedExpression& parsedExpression, const expression_vector& children) {
    if (functionName == CAST_TO_DATE_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(DATE),
            make_unique<Literal>(Date::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(TIMESTAMP),
            make_unique<Literal>(Timestamp::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_INTERVAL_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(INTERVAL),
            make_unique<Literal>(Interval::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_YEAR_FUNC_NAME) {
        return make_shared<LiteralExpression>(DataType(INT64),
            make_unique<Literal>(Date::getDatePart(DatePartSpecifier::YEAR, ((LiteralExpression*)children[0].get())->literal->val.dateVal)));
    }
    else {
        assert(functionName == ID_FUNC_NAME);
        return bindInternalIDExpression(parsedExpression);
    }
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(
    const ParsedExpression& parsedExpression) {
    auto child = bindExpression(*parsedExpression.getChild(0));
    validateExpectedDataType(*child, unordered_set<DataTypeID>{NODE, REL});
    return bindInternalIDExpression(*child);
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(const Expression& expression) {
    if (expression.dataType.typeID == NODE) {
        auto& node = (NodeExpression&)expression;
        return node.getInternalIDProperty();
    } else {
        assert(expression.dataType.typeID == REL);
        return bindRelPropertyExpression(expression, INTERNAL_ID_SUFFIX);
    }
}

unique_ptr<Expression> ExpressionBinder::createInternalNodeIDExpression(
    const Expression& expression) {
    auto& node = (NodeExpression&)expression;
    unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    for (auto tableID : node.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    auto result = make_unique<PropertyExpression>(
        DataType(NODE_ID), INTERNAL_ID_SUFFIX, node, std::move(propertyIDPerTable));
    return result;
}

shared_ptr<Expression> ExpressionBinder::bindParameterExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedParameterExpression = (ParsedParameterExpression&)parsedExpression;
    auto parameterName = parsedParameterExpression.getParameterName();
    if (parameterMap.find(parameterName) != parameterMap.end()) {
        return make_shared<ParameterExpression>(parameterName, parameterMap.at(parameterName));
    } else {
        auto literal = make_shared<Literal>();
        parameterMap.insert({parameterName, literal});
        return make_shared<ParameterExpression>(parameterName, literal);
    }
}

shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto& literalExpression = (ParsedLiteralExpression&)parsedExpression;
    auto literal = literalExpression.getLiteral();
    if (literal->isNull()) {
        return bindNullLiteralExpression();
    }
    return make_shared<LiteralExpression>(literal->dataType, make_unique<Literal>(*literal));
}

shared_ptr<Expression> ExpressionBinder::bindNullLiteralExpression() {
    return make_shared<LiteralExpression>(
        DataType(ANY), make_unique<Literal>(), binder->getUniqueExpressionName("NULL"));
}

shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto& variableExpression = (ParsedVariableExpression&)parsedExpression;
    auto variableName = variableExpression.getVariableName();
    if (binder->variablesInScope.find(variableName) != binder->variablesInScope.end()) {
        return binder->variablesInScope.at(variableName);
    }
    throw BinderException("Variable " + parsedExpression.getRawName() + " is not in scope.");
}

shared_ptr<Expression> ExpressionBinder::bindExistentialSubqueryExpression(
    const ParsedExpression& parsedExpression) {
    auto& subqueryExpression = (ParsedSubqueryExpression&)parsedExpression;
    auto prevVariablesInScope = binder->enterSubquery();
    auto qgpair = binder->bindGraphPattern(subqueryExpression.getPatternElements());
    auto& queryGraph = qgpair.first;
    auto name = binder->getUniqueExpressionName(parsedExpression.getRawName());
    auto boundSubqueryExpression =
        make_shared<ExistentialSubqueryExpression>(std::move(queryGraph), std::move(name));
    if (subqueryExpression.hasWhereClause()) {
        boundSubqueryExpression->setWhereExpression(
            binder->bindWhereExpression(*subqueryExpression.getWhereClause()));
    }
    binder->exitSubquery(move(prevVariablesInScope));
    return boundSubqueryExpression;
}

shared_ptr<Expression> ExpressionBinder::bindCaseExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedCaseExpression = (ParsedCaseExpression&)parsedExpression;
    auto anchorCaseAlternative = parsedCaseExpression.getCaseAlternative(0);
    auto outDataType = bindExpression(*anchorCaseAlternative->thenExpression)->dataType;
    auto name = binder->getUniqueExpressionName(parsedExpression.getRawName());
    // bind ELSE ...
    shared_ptr<Expression> elseExpression;
    if (parsedCaseExpression.hasElseExpression()) {
        elseExpression = bindExpression(*parsedCaseExpression.getElseExpression());
    } else {
        elseExpression = bindNullLiteralExpression();
    }
    elseExpression = implicitCastIfNecessary(elseExpression, outDataType);
    auto boundCaseExpression =
        make_shared<CaseExpression>(outDataType, std::move(elseExpression), name);
    // bind WHEN ... THEN ...
    if (parsedCaseExpression.hasCaseExpression()) {
        auto boundCase = bindExpression(*parsedCaseExpression.getCaseExpression());
        for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
            auto caseAlternative = parsedCaseExpression.getCaseAlternative(i);
            auto boundWhen = bindExpression(*caseAlternative->whenExpression);
            boundWhen = implicitCastIfNecessary(boundWhen, boundCase->dataType);
            // rewrite "CASE a.age WHEN 1" as "CASE WHEN a.age = 1"
            boundWhen = bindComparisonExpression(
                EQUALS, vector<shared_ptr<Expression>>{boundCase, boundWhen});
            auto boundThen = bindExpression(*caseAlternative->thenExpression);
            boundThen = implicitCastIfNecessary(boundThen, outDataType);
            boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
        }
    } else {
        for (auto i = 0u; i < parsedCaseExpression.getNumCaseAlternative(); ++i) {
            auto caseAlternative = parsedCaseExpression.getCaseAlternative(i);
            auto boundWhen = bindExpression(*caseAlternative->whenExpression);
            boundWhen = implicitCastIfNecessary(boundWhen, BOOL);
            auto boundThen = bindExpression(*caseAlternative->thenExpression);
            boundThen = implicitCastIfNecessary(boundThen, outDataType);
            boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
        }
    }
    return boundCaseExpression;
}

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataType targetType) {
    if (targetType.typeID == ANY || expression->dataType == targetType) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) {
        resolveAnyDataType(*expression, targetType);
        return expression;
    }
    if (targetType.typeID == INT64 && expression->dataType.typeID == INTEGER) {
        expression->dataType.typeID = INT64; // TODO temporary..
        return expression;
    }
    if (targetType.typeID == DECIMAL && expression->dataType.typeID == INTEGER) {
        expression->dataType.typeID = DECIMAL; // TODO temporary..
        return expression;
    }
    return implicitCast(expression, targetType);
}

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataTypeID targetTypeID) {
    if (targetTypeID == ANY || expression->dataType.typeID == targetTypeID) {
        return expression;
    }
    if (expression->dataType.typeID == ANY) {
        if (targetTypeID == LIST) {
            // e.g. len($1) we cannot infer the child type for $1.
            throw BinderException("Cannot resolve recursive data type for expression " +
                                  expression->getRawName() + ".");
        }
        resolveAnyDataType(*expression, DataType(targetTypeID));
        return expression;
    }
    assert(targetTypeID != LIST);
    return implicitCast(expression, DataType(targetTypeID));
}

void ExpressionBinder::resolveAnyDataType(Expression& expression, DataType targetType) {
    if (expression.expressionType == PARAMETER) { // expression is parameter
        ((ParameterExpression&)expression).setDataType(targetType);
    } else { // expression is null literal
        assert(expression.expressionType == LITERAL);
        ((LiteralExpression&)expression).setDataType(targetType);
    }
}

shared_ptr<Expression> ExpressionBinder::implicitCast(
    const shared_ptr<Expression>& expression, DataType targetType) {

    // INT64 to INT32
    throw BinderException("Expression " + expression->getRawName() + " has data type " +
                          Types::dataTypeToString(expression->dataType) + " but expect " +
                          Types::dataTypeToString(targetType) +
                          ". Implicit cast is not supported.");
}

void ExpressionBinder::validateExpectedDataType(
    const Expression& expression, const unordered_set<DataTypeID>& targets) {
    auto dataType = expression.dataType;
    if (!(targets.find(dataType.typeID) != targets.end())) {
        vector<DataTypeID> targetsVec{targets.begin(), targets.end()};
        throw BinderException(expression.getRawName() + " has data type " +
                              Types::dataTypeToString(dataType.typeID) + ". " +
                              Types::dataTypesToString(targetsVec) + " was expected.");
    }
}

void ExpressionBinder::validateAggregationExpressionIsNotNested(const Expression& expression) {
    if (expression.getNumChildren() == 0) {
        return;
    }
    // TODO why is this need?
    // if (expression.getChild(0)->hasAggregationExpression()) {
    //     throw BinderException(
    //         "Expression " + expression.getRawName() + " contains nested aggregation.");
    // }
}

} // namespace binder
} // namespace kuzu
