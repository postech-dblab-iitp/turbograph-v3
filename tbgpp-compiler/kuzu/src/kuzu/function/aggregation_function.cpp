#include "function/aggregate/aggregate_function.h"

#include "binder/expression/expression.h"

#include "common/types/interval_t.h"
#include "function/comparison/comparison_operations.h"

// #include "function/aggregate/avg.h"
// #include "function/aggregate/count.h"
// #include "function/aggregate/count_star.h"
// #include "function/aggregate/min_max.h"
// #include "function/aggregate/sum.h"

namespace kuzu {
namespace function {

using namespace kuzu::function::operation;

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountStarFunction() {
    return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), DataType(ANY) /* dummy input data type */);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction(
    const DataType& inputType, bool isDistinct) {
    return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
        isDistinct);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
	case TINYINT:
	case SMALLINT:
	case INTEGER:
	case FLOAT:
	case UTINYINT:
	case USMALLINT:
	case UINTEGER:
	case UBIGINT:
    case INT64:
    case DOUBLE:
    case DECIMAL:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getAvgFunction.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
	case TINYINT:
	case SMALLINT:
	case INTEGER:
	case FLOAT:
	case UTINYINT:
	case USMALLINT:
	case UINTEGER:
	case UBIGINT:
	case INT64:
    case DOUBLE:
    case DECIMAL:
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
    case BOOL:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
	case TINYINT:
	case SMALLINT:
	case INTEGER:
	case FLOAT:
	case UTINYINT:
	case USMALLINT:
	case UINTEGER:
	case UBIGINT:
    case INT64:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case DOUBLE:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case DATE:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    case STRING:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(),
            inputType, isDistinct);
    case NODE_ID:
        return make_unique<AggregateFunction>(empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), empty_agg_func(), inputType,
            isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getMinMaxFunction.");
    }
}

} // namespace function
} // namespace kuzu
