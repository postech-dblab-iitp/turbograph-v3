#include "kuzu/function//cast/vector_cast_operations.h"

// #include "kuzu/common/vector/value_vector_utils.h"
#include "kuzu/function//cast/cast_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToDateVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DATE_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING}, DataTypeID::DATE,
        empty_scalar_exec_func()));
        // UnaryExecFunction<ku_string_t, date_t, operation::CastStringToDate>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToTimestampVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_TIMESTAMP_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING}, DataTypeID::TIMESTAMP,
        empty_scalar_exec_func()));
        // UnaryExecFunction<ku_string_t, timestamp_t, operation::CastStringToTimestamp>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToIntervalVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INTERVAL_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING}, DataTypeID::INTERVAL,
        empty_scalar_exec_func()));
        // UnaryExecFunction<ku_string_t, interval_t, operation::CastStringToInterval>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToStringVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::BOOLEAN}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<bool, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INTEGER}, DataTypeID::STRING,
        empty_scalar_exec_func()));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INT64}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<int64_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DOUBLE}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<double_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DATE}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<date_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<timestamp_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INTERVAL}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<interval_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<ku_string_t, ku_string_t, operation::CastToString>));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_STRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::LIST}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryCastExecFunction<ku_list_t, ku_string_t, operation::CastToString>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> 
CastToDoubleVectorFunction::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DOUBLE_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INT64}, DataTypeID::DOUBLE,
        empty_scalar_exec_func()));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_DOUBLE_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::FLOAT}, DataTypeID::DOUBLE,
        empty_scalar_exec_func()));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
CastToFloatVectorFunction::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_FLOAT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INT64}, DataTypeID::FLOAT,
        empty_scalar_exec_func()));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_FLOAT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DOUBLE}, DataTypeID::FLOAT,
        empty_scalar_exec_func()));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_FLOAT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING}, DataTypeID::FLOAT,
        empty_scalar_exec_func()));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> 
CastToInt64VectorFunction::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INT64_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DOUBLE}, DataTypeID::INT64,
        empty_scalar_exec_func()));
    result.push_back(make_unique<VectorOperationDefinition>(CAST_TO_INT64_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::FLOAT}, DataTypeID::INT64,
        empty_scalar_exec_func()));
    return result;
}

} // namespace function
} // namespace kuzu
