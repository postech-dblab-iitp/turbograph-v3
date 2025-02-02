#pragma once

#include "kuzu/common/types/literal.h"
#include "kuzu/binder/expression/expression.h"

namespace kuzu {
namespace binder {

class ParameterExpression : public Expression {

public:
    explicit ParameterExpression(const string& parameterName, shared_ptr<Literal> literal)
        : Expression{PARAMETER, DataTypeID::ANY, "$" + parameterName /* add $ to avoid conflict between parameter name and variable name */}, literal{move(literal)} {}

    inline void setDataType(const DataType& targetType) {
        assert(dataType.typeID == DataTypeID::ANY);
        dataType = targetType;
        literal->dataType = targetType;
    }

    inline shared_ptr<Literal> getLiteral() const { return literal; }

private:
    shared_ptr<Literal> literal;
};

} // namespace binder
} // namespace kuzu
