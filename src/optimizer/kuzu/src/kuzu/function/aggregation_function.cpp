#include "kuzu/function//aggregate/aggregate_function.h"

#include "kuzu/binder/expression/expression.h"

#include "kuzu/common/types/interval_t.h"
#include "kuzu/function//comparison/comparison_operations.h"

// #include "kuzu/function//aggregate/avg.h"
// #include "kuzu/function//aggregate/count.h"
// #include "kuzu/function//aggregate/count_star.h"
// #include "kuzu/function//aggregate/min_max.h"
// #include "kuzu/function//aggregate/sum.h"

namespace kuzu {
namespace function {

using namespace kuzu::function::operation;

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountStarFunction() {
    return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), DataType(DataTypeID::ANY) /* dummy input data type */);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction(
    const DataType& inputType, bool isDistinct) {
    return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
        isDistinct);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getEmptyFunction(
    const DataType& inputType, bool isDistinct) {
    return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
        isDistinct);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
	case DataTypeID::TINYINT:
	case DataTypeID::SMALLINT:
	case DataTypeID::INTEGER:
	case DataTypeID::FLOAT:
	case DataTypeID::UTINYINT:
	case DataTypeID::USMALLINT:
	case DataTypeID::UINTEGER:
	case DataTypeID::UBIGINT:
    case DataTypeID::INT64:
    case DataTypeID::DOUBLE:
    case DataTypeID::DECIMAL:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getAvgFunction.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
	case DataTypeID::TINYINT:
	case DataTypeID::SMALLINT:
	case DataTypeID::INTEGER:
	case DataTypeID::FLOAT:
	case DataTypeID::UTINYINT:
	case DataTypeID::USMALLINT:
	case DataTypeID::UINTEGER:
	case DataTypeID::UBIGINT:
	case DataTypeID::INT64:
    case DataTypeID::DOUBLE:
    case DataTypeID::DECIMAL:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getSumFunction.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinFunction(
    const DataType& inputType, bool isDistinct) {
    return getMinMaxFunction<LessThan>(inputType, isDistinct);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getMaxFunction(
    const DataType& inputType, bool isDistinct) {
    return getMinMaxFunction<GreaterThan>(inputType, isDistinct);
}

template<typename FUNC>
unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case DataTypeID::BOOLEAN:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
	case DataTypeID::TINYINT:
	case DataTypeID::SMALLINT:
	case DataTypeID::INTEGER:
	case DataTypeID::FLOAT:
	case DataTypeID::UTINYINT:
	case DataTypeID::USMALLINT:
	case DataTypeID::UINTEGER:
	case DataTypeID::UBIGINT:
    case DataTypeID::DECIMAL:
    case DataTypeID::INT64:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case DataTypeID::DOUBLE:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case DataTypeID::DATE:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case DataTypeID::STRING:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case DataTypeID::NODE_ID:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getMinMaxFunction.");
    }
}

} // namespace function
} // namespace kuzu
