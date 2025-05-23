#pragma once

#include "kuzu/binder/expression/expression.h"
#include "kuzu/function//aggregate/aggregate_function.h"
#include "kuzu/function//vector_operations.h"

using namespace kuzu::function;

namespace kuzu {
namespace binder {

class FunctionExpression : public Expression {

protected:
    FunctionExpression(ExpressionType expressionType, DataType dataType, const string& uniqueName)
        : Expression{expressionType, move(dataType), uniqueName}, rawFuncName("") {}

    FunctionExpression(ExpressionType expressionType, DataType dataType,
        const shared_ptr<Expression>& child, const string& uniqueName)
        : Expression{expressionType, move(dataType), child, uniqueName}, rawFuncName("") {}

    FunctionExpression(ExpressionType expressionType, DataType dataType, expression_vector children,
        const string& uniqueName)
        : Expression{expressionType, move(dataType), move(children), uniqueName}, rawFuncName("") {}
    
    FunctionExpression(ExpressionType expressionType, DataType dataType, const string& uniqueName, const string& rawFuncName)
        : Expression{expressionType, move(dataType), uniqueName}, rawFuncName(rawFuncName) {}

    FunctionExpression(ExpressionType expressionType, DataType dataType,
        const shared_ptr<Expression>& child, const string& uniqueName, const string& rawFuncName)
        : Expression{expressionType, move(dataType), child, uniqueName}, rawFuncName(rawFuncName) {}

    FunctionExpression(ExpressionType expressionType, DataType dataType, expression_vector children,
        const string& uniqueName, const string& rawFuncName)
        : Expression{expressionType, move(dataType), move(children), uniqueName}, rawFuncName(rawFuncName) {}

    string rawFuncName;
    
public:
    inline string getRawFuncName() const {
        return rawFuncName;
    }

    
};

class ScalarFunctionExpression : public FunctionExpression {

public:
    ScalarFunctionExpression(ExpressionType expressionType, const DataType& dataType,
        expression_vector children, scalar_exec_func execFunc, scalar_select_func selectFunc,
        const string& uniqueName)
        : FunctionExpression{expressionType, dataType, move(children), uniqueName},
          execFunc{move(execFunc)}, selectFunc{move(selectFunc)} {}
    
    ScalarFunctionExpression(ExpressionType expressionType, const DataType& dataType,
        expression_vector children, scalar_exec_func execFunc, scalar_select_func selectFunc,
        const string& uniqueName, const string& rawFuncName)
        : FunctionExpression{expressionType, dataType, move(children), uniqueName, rawFuncName},
          execFunc{move(execFunc)}, selectFunc{move(selectFunc)} {}

    static inline string getUniqueName(const string& functionName, expression_vector& children) {
        auto result = functionName + "(";
        for (auto& child : children) {
            result += child->getUniqueName() + ", ";
        }
        result += ")";
        return result;
    }

public:
    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
};

class AggregateFunctionExpression : public FunctionExpression {

public:
    AggregateFunctionExpression(const DataType& dataType,
        unique_ptr<AggregateFunction> aggregateFunction, const string& uniqueName)
        : AggregateFunctionExpression{
              dataType, expression_vector{}, move(aggregateFunction), uniqueName} {}
    
    AggregateFunctionExpression(const DataType& dataType,
        unique_ptr<AggregateFunction> aggregateFunction, const string& uniqueNamem, const string& rawFuncName)
        : AggregateFunctionExpression{
              dataType, expression_vector{}, move(aggregateFunction), uniqueName, rawFuncName} {}

    AggregateFunctionExpression(const DataType& dataType, expression_vector children,
        unique_ptr<AggregateFunction> aggregateFunction, const string& uniqueName)
        : FunctionExpression{AGGREGATE_FUNCTION, dataType, move(children), uniqueName},
          aggregateFunction{move(aggregateFunction)} {
        }

    AggregateFunctionExpression(const DataType& dataType, expression_vector children,
        unique_ptr<AggregateFunction> aggregateFunction, const string& uniqueName, const string& rawFuncName)
        : FunctionExpression{AGGREGATE_FUNCTION, dataType, move(children), uniqueName, rawFuncName},
          aggregateFunction{move(aggregateFunction)} {
        }

    static inline string getUniqueName(
        const string& functionName, expression_vector& children, bool isDistinct) {
        auto result = functionName + "(";
        if (isDistinct) {
            result += "DISTINCT ";
        }
        for (auto& child : children) {
            result += child->getUniqueName() + ", ";
        }
        result += ")";
        return result;
    }

    inline bool isDistinct() { return aggregateFunction->isFunctionDistinct(); }

public:
    unique_ptr<AggregateFunction> aggregateFunction;
};

} // namespace binder
} // namespace kuzu
