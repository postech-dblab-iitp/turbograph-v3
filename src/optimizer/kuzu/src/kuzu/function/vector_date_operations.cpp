#include "kuzu/function//date/vector_date_operations.h"

#include "kuzu/function//date/date_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>> DatePartVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::DATE}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, date_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::TIMESTAMP}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, timestamp_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INTERVAL}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, interval_t, int64_t, operation::DatePart>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DatePartYearVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_YEAR_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DATE}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, date_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_YEAR_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, timestamp_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_YEAR_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INTERVAL}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, interval_t, int64_t, operation::DatePart>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DatePartMonthVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_MONTH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DATE}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, date_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_MONTH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, timestamp_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_MONTH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INTERVAL}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, interval_t, int64_t, operation::DatePart>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DatePartDayVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_DAY_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DATE}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, date_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_DAY_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, timestamp_t, int64_t, operation::DatePart>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_PART_DAY_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INTERVAL}, DataTypeID::INT64,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, interval_t, int64_t, operation::DatePart>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DateTruncVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::DATE}, DataTypeID::DATE,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, date_t, date_t, operation::DateTrunc>));
    result.push_back(make_unique<VectorOperationDefinition>(DATE_TRUNC_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::TIMESTAMP}, DataTypeID::TIMESTAMP,
        empty_scalar_exec_func()));
        // BinaryExecFunction<ku_string_t, timestamp_t, timestamp_t, operation::DateTrunc>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DayNameVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::DATE},
            DataTypeID::STRING, empty_scalar_exec_func()));
            // UnaryExecFunction<date_t, ku_string_t, operation::DayName>));
    result.push_back(make_unique<VectorOperationDefinition>(DAYNAME_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryExecFunction<timestamp_t, ku_string_t, operation::DayName>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> GreatestVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::DATE}, DataTypeID::DATE,
        empty_scalar_exec_func()));
        // BinaryExecFunction<date_t, date_t, date_t, operation::Greatest>));
    result.push_back(make_unique<VectorOperationDefinition>(GREATEST_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP, DataTypeID::TIMESTAMP}, DataTypeID::TIMESTAMP,
        empty_scalar_exec_func()));
        // BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, operation::Greatest>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LastDayVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::DATE},
            DataTypeID::DATE, empty_scalar_exec_func()));
            // UnaryExecFunction<date_t, date_t, operation::LastDay>));
    result.push_back(make_unique<VectorOperationDefinition>(LAST_DAY_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::DATE,
        empty_scalar_exec_func()));
        // UnaryExecFunction<timestamp_t, date_t, operation::LastDay>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LeastVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::DATE},
            DataTypeID::DATE, empty_scalar_exec_func()));
            // BinaryExecFunction<date_t, date_t, date_t, operation::Least>));
    result.push_back(make_unique<VectorOperationDefinition>(LEAST_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP, DataTypeID::TIMESTAMP}, DataTypeID::TIMESTAMP,
        empty_scalar_exec_func()));
        // BinaryExecFunction<timestamp_t, timestamp_t, timestamp_t, operation::Least>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> MakeDateVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(MAKE_DATE_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::DATE,
        empty_scalar_exec_func()));
        // TernaryExecFunction<int64_t, int64_t, int64_t, date_t, operation::MakeDate>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> MonthNameVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::DATE},
            DataTypeID::STRING, empty_scalar_exec_func()));
            // UnaryExecFunction<date_t, ku_string_t, operation::MonthName>));
    result.push_back(make_unique<VectorOperationDefinition>(MONTHNAME_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::TIMESTAMP}, DataTypeID::STRING,
        empty_scalar_exec_func()));
        // UnaryExecFunction<timestamp_t, ku_string_t, operation::MonthName>));
    return result;
}

} // namespace function
} // namespace kuzu
