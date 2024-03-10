#include "common/expression_type.h"

#include "common/exception.h"

namespace kuzu {
namespace common {

bool isExpressionUnary(ExpressionType type) {
    return NOT == type || IS_NULL == type || IS_NOT_NULL == type;
}

bool isExpressionBinary(ExpressionType type) {
    return isExpressionComparison(type) || OR == type || XOR == type || AND == type;
}

bool isExpressionBoolConnection(ExpressionType type) {
    return OR == type || XOR == type || AND == type || NOT == type;
}

bool isExpressionComparison(ExpressionType type) {
    return EQUALS == type || NOT_EQUALS == type || GREATER_THAN == type ||
           GREATER_THAN_EQUALS == type || LESS_THAN == type || LESS_THAN_EQUALS == type;
}

bool isExpressionNullOperator(ExpressionType type) {
    return IS_NULL == type || IS_NOT_NULL == type;
}

bool isExpressionLiteral(ExpressionType type) {
    return LITERAL == type;
}

bool isExpressionAggregate(ExpressionType type) {
    return AGGREGATE_FUNCTION == type;
}

bool isExpressionScalarFunc(ExpressionType type) {
    return FUNCTION == type;
}

bool isExpressionSubquery(ExpressionType type) {
    return EXISTENTIAL_SUBQUERY == type;
}

bool isExpressionCaseElse(ExpressionType type) {
    return CASE_ELSE == type;
}

bool isExpressionFunction(ExpressionType type) {
    return FUNCTION == type;
}

bool isExpressionParameter(ExpressionType type) {
    return PARAMETER == type;
}

bool isExpressionProperty(ExpressionType type) {
    return PROPERTY == type;
}

bool isExpressionListComprehension(ExpressionType type) {
    return LIST_COMPREHENSION == type;
}

bool isExpressionPatternComprehension(ExpressionType type) {
    return PATTERN_COMPREHENSION == type;
}

bool isExpressionFilter(ExpressionType type) {
    return FILTER == type;
}

bool isExpressionIdInColl(ExpressionType type) {
    return ID_IN_COLL == type;
}

string expressionTypeToString(ExpressionType type) {
    switch (type) {
    case OR:
        return "OR";
    case XOR:
        return "XOR";
    case AND:
        return "AND";
    case NOT:
        return "NOT";
    case EQUALS:
        return EQUALS_FUNC_NAME;
    case NOT_EQUALS:
        return NOT_EQUALS_FUNC_NAME;
    case GREATER_THAN:
        return GREATER_THAN_FUNC_NAME;
    case GREATER_THAN_EQUALS:
        return GREATER_THAN_EQUALS_FUNC_NAME;
    case LESS_THAN:
        return LESS_THAN_FUNC_NAME;
    case LESS_THAN_EQUALS:
        return LESS_THAN_EQUALS_FUNC_NAME;
    case IS_NULL:
        return "IS_NULL";
    case IS_NOT_NULL:
        return "IS_NOT_NULL";
    case ID_IN_COLL:
        return "ID_IN_COLL";
    case FILTER:
        return "FILTER";
    case LIST_COMPREHENSION:
        return "LIST_COMPREHENSION";
    case RELATIONSHIPS:
        return "RELATIONSHIPS";
    case PATTERN_COMPREHENSION:
        return "PATTERN_COMPREHENSION";
    case PROPERTY:
        return "PROPERTY";
    case LITERAL:
        return "LITERAL";
    case VARIABLE:
        return "VARIABLE";
    case PARAMETER:
        return "PARAMETER";
    case FUNCTION:
        return "FUNCTION";
    case AGGREGATE_FUNCTION:
        return "AGGREGATE_FUNCTION";
    case EXISTENTIAL_SUBQUERY:
        return "EXISTENTIAL_SUBQUERY";
    default:
        throw NotImplementedException("Cannot convert expression type to string");
    }
}

} // namespace common
} // namespace kuzu
