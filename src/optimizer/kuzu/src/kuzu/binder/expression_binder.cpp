#include "kuzu/binder/expression_binder.h"
#include "kuzu/binder/binder.h"
#include "kuzu/binder/expression/case_expression.h"
#include "kuzu/binder/expression/existential_subquery_expression.h"
#include "kuzu/binder/expression/function_expression.h"
#include "kuzu/binder/expression/literal_expression.h"
#include "kuzu/binder/expression/parameter_expression.h"
#include "kuzu/binder/expression/rel_expression.h"
#include "kuzu/binder/expression/list_comprehension_expression.h"
#include "kuzu/binder/expression/pattern_comprehension_expression.h"
#include "kuzu/binder/expression/filter_expression.h"
#include "kuzu/binder/expression/idincoll_expression.h"
#include "kuzu/common/type_utils.h"
#include "kuzu/common/utils.h"
#include "kuzu/function/vector_operations.h"
#include "kuzu/parser/expression/parsed_case_expression.h"
#include "kuzu/parser/expression/parsed_function_expression.h"
#include "kuzu/parser/expression/parsed_literal_expression.h"
#include "kuzu/parser/expression/parsed_parameter_expression.h"
#include "kuzu/parser/expression/parsed_property_expression.h"
#include "kuzu/parser/expression/parsed_subquery_expression.h"
#include "kuzu/parser/expression/parsed_variable_expression.h"
#include "kuzu/parser/expression/parsed_list_comprehension_expression.h"
#include "kuzu/parser/expression/parsed_pattern_comprehension_expression.h"
#include "kuzu/parser/expression/parsed_filter_expression.h"
#include "kuzu/parser/expression/parsed_idincoll_expression.h"

// using namespace kuzu::function;

namespace kuzu {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindExpression(const ParsedExpression& parsedExpression) {
    bool is_root = currentORGroupID == 0;
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
    } else if (LIST_COMPREHENSION == expressionType) {
        expression = bindListComprehensionExpression(parsedExpression);
    } else if (PATTERN_COMPREHENSION == expressionType) {
        expression = bindPatternComprehensionExpression(parsedExpression);
    } else if (ID_IN_COLL == expressionType) {
        expression = bindIdInCollExpression(parsedExpression);
    } else if (FILTER == expressionType) {
        expression = bindFilterExpression(parsedExpression);
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
    if (is_root) { //@jhha: temporal implementation for OR filter
        currentORGroupID = 0;
    }
    return expression;
}

shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
    const ParsedExpression& parsedExpression) {
    auto expressionType = parsedExpression.getExpressionType();
    expression_vector children;
    if (expressionType == ExpressionType::OR) {
        for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
            children.push_back(bindExpression(*parsedExpression.getChild(i)));
            currentORGroupID++;
        }
    } else {
        for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
            children.push_back(bindExpression(*parsedExpression.getChild(i)));
        }
    }
    return bindBooleanExpression(parsedExpression.getExpressionType(), children);
}

shared_ptr<Expression> ExpressionBinder::bindBooleanExpression(
   ExpressionType expressionType, const expression_vector& children) {
    
    expression_vector childrenAfterCast;
    for (auto& child : children) {
        childrenAfterCast.push_back(implicitCastIfNecessary(child, DataTypeID::BOOLEAN));
    }
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = empty_scalar_exec_func();
    auto selectFunc = empty_scalar_select_func();
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(functionName, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(DataTypeID::BOOLEAN),
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

    // Issue 111 (DOUBLE to DECIMAL Casting)
    for (auto i = 0u; i < children.size(); ++i) {
        if (isExpressionLiteral(children[i]->expressionType)) {
            // Explicit casting for double to decimal cases
            auto &other_child = children[(i + 1) % 2];
            auto &current_child = children[i];
            if (other_child->dataType.typeID == DataTypeID::DECIMAL &&
                current_child->dataType.typeID == DataTypeID::DOUBLE) {
                // Assumption: DECIMAL type is always (15,2)
                std::cout << "CAUTION: Casting DOUBLE to DECIMAL with scale 2" << std::endl;
                auto &literal = ((LiteralExpression *)current_child.get())->literal;
                auto double_val = literal->val.doubleVal;
                literal->val.int64Val = static_cast<int64_t>(std::round(double_val * 100));
                ((LiteralExpression *)current_child.get())->setDataTypeForced(DataType(DataTypeID::DECIMAL));
            }
        }
    }

    vector<DataType> childrenTypes;
    for (auto& child : children) {
        childrenTypes.push_back(child->dataType);
    }

    bool isComparsionOnTwoNodeOrEdges = false;
    if ((expressionType == ExpressionType::EQUALS ||
         expressionType == ExpressionType::NOT_EQUALS) &&
        (children.size() == 2) &&
        ((children[0].get()->dataType.typeID == DataTypeID::NODE &&
          children[1].get()->dataType.typeID == DataTypeID::NODE) ||
         (children[0].get()->dataType.typeID == DataTypeID::REL &&
          children[1].get()->dataType.typeID == DataTypeID::REL))) {
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
        if (isComparsionOnTwoNodeOrEdges) {
            // rewrite x = y on node or rel as comp on their internal IDs.
            auto child = bindInternalIDExpression(*children[i]);
            childrenAfterCast.push_back(
                implicitCastIfNecessary(child, function->parameterTypeIDs[i]));
            
            D_ASSERT(childrenAfterCast[i]->expressionType == ExpressionType::PROPERTY);
            auto &node_expr = (NodeOrRelExpression &)*children[i];
            node_expr.setUsedForFilterColumn(INTERNAL_ID_PROPERTY_KEY_ID, currentORGroupID);
        } else {
            childrenAfterCast.push_back(
                implicitCastIfNecessary(children[i], function->parameterTypeIDs[i]));

            // TODO we need to consider IS (NOT) NULL comparison
            if (childrenAfterCast[i]->expressionType == ExpressionType::PROPERTY) {
                auto &property = (PropertyExpression&)*children[i];
                auto *node_expr = (NodeOrRelExpression *)property.getNodeOrRelExpr();
                node_expr->setUsedForFilterColumn(property.getPropertyID(), currentORGroupID);
            } else if (childrenAfterCast[i]->expressionType == ExpressionType::VARIABLE) {
                D_ASSERT(false); // not implemeneted yet
            }
        }
    }
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(function->name, childrenAfterCast);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(function->returnTypeID),
        move(childrenAfterCast), function->execFunc, function->selectFunc, uniqueExpressionName);
}

shared_ptr<Expression> ExpressionBinder::bindNullOperatorExpression(
    const ParsedExpression& parsedExpression) {
    expression_vector children;
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        children.push_back(bindExpression(*parsedExpression.getChild(i)));
    }
    // Assumption: IS NOT NULL filter (no IS NULL filter)
    // If IS NULL, don't set filter column
    for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
        if (children[i]->expressionType == ExpressionType::PROPERTY) {
            auto &property = (PropertyExpression&)*children[i];
            auto *node_expr = (NodeOrRelExpression *)property.getNodeOrRelExpr();
            if (parsedExpression.getExpressionType() == ExpressionType::IS_NOT_NULL) {
                node_expr->setUsedForFilterColumn(property.getPropertyID(), currentORGroupID);
            }
        }
        else {
            D_ASSERT(false);
        }
    }
    auto expressionType = parsedExpression.getExpressionType();
    auto functionName = expressionTypeToString(expressionType);
    auto execFunc = empty_scalar_exec_func();
    auto selectFunc = empty_scalar_select_func();
    auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(functionName, children);
    return make_shared<ScalarFunctionExpression>(expressionType, DataType(DataTypeID::BOOLEAN), move(children),
        move(execFunc), move(selectFunc), uniqueExpressionName);
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
    validateExpectedDataType(*child, unordered_set<DataTypeID>{DataTypeID::NODE, DataTypeID::REL});
    if (DataTypeID::NODE == child->dataType.typeID) {
        return bindNodePropertyExpression(*child, propertyName);
    } else {
        assert(DataTypeID::REL == child->dataType.typeID);
        return bindRelPropertyExpression(*child, propertyName);
    }
}

shared_ptr<Expression> ExpressionBinder::bindNodePropertyExpression(
    const Expression& expression, const string& propertyName) {
    auto& nodeOrRel = (NodeOrRelExpression&)expression;
    uint64_t propertyKeyID = binder->getPropertyKeyID(propertyName);
    if (!nodeOrRel.hasPropertyExpression(propertyKeyID)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + expression.getRawName() + ".");
    }
    return nodeOrRel.getPropertyExpression(propertyKeyID);
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
    uint64_t propertyKeyID = binder->getPropertyKeyID(propertyName);
    if (!rel.hasPropertyExpression(propertyKeyID)) {
        throw BinderException(
            "Cannot find property " + propertyName + " for " + expression.getRawName() + ".");
    }
    return rel.getPropertyExpression(propertyKeyID);
}

unique_ptr<Expression> ExpressionBinder::createPropertyExpression(
    Expression &nodeOrRel, Property &anchorProperty,
    unordered_map<table_id_t, property_id_t>* propertyIDPerTable,
    uint64_t prop_key_id)
{
    // assert(!properties.empty());
    // auto anchorProperty = properties[0];

    // tslee: 250114 disabled for performance issue
    // // conform data type between multi table access
    // validatePropertiesWithSameDataType(properties, anchorProperty.dataType,
    //                                    anchorProperty.name,
    //                                    nodeOrRel.getRawName());
    return make_unique<PropertyExpression>(
        anchorProperty.dataType, anchorProperty.name, prop_key_id, nodeOrRel,
        propertyIDPerTable);
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
        if (child->dataType.typeID == DataTypeID::NODE || child->dataType.typeID == DataTypeID::REL) {
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
        return make_shared<LiteralExpression>(DataType(DataTypeID::DATE),
            make_unique<Literal>(Date::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_TIMESTAMP_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(DataTypeID::TIMESTAMP),
            make_unique<Literal>(Timestamp::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_INTERVAL_FUNC_NAME) {
        auto strVal = ((LiteralExpression*)children[0].get())->literal->strVal;
        return make_shared<LiteralExpression>(DataType(DataTypeID::INTERVAL),
            make_unique<Literal>(Interval::FromCString(strVal.c_str(), strVal.length())));
    } else if (functionName == CAST_TO_YEAR_FUNC_NAME) {
        return make_shared<LiteralExpression>(DataType(DataTypeID::INT64),
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
    validateExpectedDataType(*child, unordered_set<DataTypeID>{DataTypeID::NODE, DataTypeID::REL});
    return bindInternalIDExpression(*child);
}

shared_ptr<Expression> ExpressionBinder::bindInternalIDExpression(const Expression& expression) {
    if (expression.dataType.typeID == DataTypeID::NODE) {
        auto& node = (NodeExpression&)expression;
        return node.getInternalIDProperty();
    } else {
        assert(expression.dataType.typeID == DataTypeID::REL);
        return bindRelPropertyExpression(expression, INTERNAL_ID_SUFFIX);
    }
}

unique_ptr<Expression> ExpressionBinder::createInternalNodeIDExpression(
    const Expression& expression, unordered_map<table_id_t, property_id_t>* propertyIDPerTable) {
    auto& node = (NodeExpression&)expression;
    auto result = make_unique<PropertyExpression>(
        DataType(DataTypeID::NODE_ID), INTERNAL_ID_SUFFIX, 0, node, propertyIDPerTable);
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
        DataType(DataTypeID::ANY), make_unique<Literal>(), binder->getUniqueExpressionName("NULL"));
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
            boundWhen = implicitCastIfNecessary(boundWhen, DataTypeID::BOOLEAN);
            auto boundThen = bindExpression(*caseAlternative->thenExpression);
            boundThen = implicitCastIfNecessary(boundThen, outDataType);
            boundCaseExpression->addCaseAlternative(boundWhen, boundThen);
        }
    }
    return boundCaseExpression;
}

shared_ptr<Expression> ExpressionBinder::bindListComprehensionExpression(
    const ParsedExpression &parsedExpression)
{
    auto &parsedListCompExpression =
        (ParsedListComprehensionExpression &)parsedExpression;
    D_ASSERT(parsedListCompExpression.getNumChildren() == 1);
    auto filterExpression =
        bindExpression(*parsedListCompExpression.getChild(0));
    if (parsedListCompExpression.hasExpression()) {
        auto expression =
            bindExpression(*parsedListCompExpression.getExpression());
        return make_shared<ListComprehensionExpression>(
            std::move(filterExpression), std::move(expression),
            binder->getUniqueExpressionName(parsedExpression.getRawName()));
    }
    return make_shared<ListComprehensionExpression>(
        std::move(filterExpression), nullptr,
        binder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::bindPatternComprehensionExpression(
    const ParsedExpression& parsedExpression) {
    throw NotImplementedException("bindPatternComprehensionExpression");
}

shared_ptr<Expression> ExpressionBinder::bindFilterExpression(const ParsedExpression& parsedExpression)
{
    auto &parsedFilterExpression =
        (ParsedFilterExpression &)parsedExpression;
    auto idincollExpression =
        bindExpression(*parsedFilterExpression.getChild(0));
    if (parsedFilterExpression.hasWhereClause()) {
        auto whereExpression =
            bindExpression(*parsedFilterExpression.getWhereClause());
        return make_shared<FilterExpression>(
            std::move(idincollExpression), std::move(whereExpression),
            binder->getUniqueExpressionName(parsedExpression.getRawName()));
    }
    return make_shared<FilterExpression>(
        std::move(idincollExpression), nullptr,
        binder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::bindIdInCollExpression(const ParsedExpression& parsedExpression)
{
    auto &parsedIdInCollExpression =
        (ParsedIdInCollExpression &)parsedExpression;
    auto coll_expression = bindExpression(*parsedIdInCollExpression.getChild(0));
    if (coll_expression->dataType.typeID != DataTypeID::LIST) {
        throw BinderException("Expression " + coll_expression->getRawName() +
                              " has data type " +
                              Types::dataTypeToString(coll_expression->dataType) +
                              ". List was expected.");
    }
    if (coll_expression->getNumChildren() == 1) {
        binder->createVariable(parsedIdInCollExpression.getVarName(), coll_expression->getChild(0)->dataType);
    } else if (coll_expression->dataType.childType) {
        binder->createVariable(parsedIdInCollExpression.getVarName(), *(coll_expression->dataType.childType));
    } else {
        throw BinderException("Cannot resolve child data type of " +
                              coll_expression->getRawName() + ".");
    }
    return make_shared<IdInCollExpression>(
        parsedIdInCollExpression.getVarName(),
        std::move(bindExpression(*parsedIdInCollExpression.getChild(0))),
        binder->getUniqueExpressionName(parsedExpression.getRawName()));
}

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataType &targetType) {
    if (targetType.typeID == DataTypeID::ANY || expression->dataType == targetType) {
        targetType = expression->dataType;
        return expression;
    }
    if (expression->dataType.typeID == DataTypeID::ANY) {
        resolveAnyDataType(*expression, targetType);
        return expression;
    }
    if (targetType.typeID == DataTypeID::INT64 && expression->dataType.typeID == DataTypeID::INTEGER) {
        expression->dataType.typeID = DataTypeID::INT64; // TODO temporary..
        return expression;
    }
    if (targetType.typeID == DataTypeID::UBIGINT && expression->dataType.typeID == DataTypeID::INTEGER) {
        expression->dataType.typeID = DataTypeID::UBIGINT; // TODO temporary..
        return expression;
    }
    if (targetType.typeID == DataTypeID::UBIGINT && expression->dataType.typeID == DataTypeID::INT64) {
        expression->dataType.typeID = DataTypeID::UBIGINT; // TODO temporary..
        return expression;
    }
    if (targetType.typeID == DataTypeID::DECIMAL && expression->dataType.typeID == DataTypeID::INTEGER) {
        expression->dataType.typeID = DataTypeID::DECIMAL; // TODO temporary..
        return expression;
    }
    if (targetType.typeID == DataTypeID::DECIMAL && expression->dataType.typeID == DataTypeID::DOUBLE) {
        expression->dataType.typeID = DataTypeID::DECIMAL; // TODO temporary..
        return expression;
    }
    return implicitCast(expression, targetType);
}

shared_ptr<Expression> ExpressionBinder::implicitCastIfNecessary(
    const shared_ptr<Expression>& expression, DataTypeID targetTypeID) {
    if (targetTypeID == DataTypeID::ANY || expression->dataType.typeID == targetTypeID) {
        return expression;
    }
    if (expression->dataType.typeID == DataTypeID::ANY) {
        if (targetTypeID == DataTypeID::LIST) {
            // e.g. len($1) we cannot infer the child type for $1.
            throw BinderException("Cannot resolve recursive data type for expression " +
                                  expression->getRawName() + ".");
        }
        resolveAnyDataType(*expression, DataType(targetTypeID));
        return expression;
    }
    assert(targetTypeID != DataTypeID::LIST);
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
