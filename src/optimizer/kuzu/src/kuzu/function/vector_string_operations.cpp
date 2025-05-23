#include "kuzu/function//string/vector_string_operations.h"

// #include "kuzu/function//string/operations/array_extract_operation.h"
// #include "kuzu/function//string/operations/concat_operation.h"
// #include "kuzu/function//string/operations/contains_operation.h"
// #include "kuzu/function//string/operations/ends_with_operation.h"
// #include "kuzu/function//string/operations/left_operation.h"
// #include "kuzu/function//string/operations/length_operation.h"
// #include "kuzu/function//string/operations/lpad_operation.h"
// #include "kuzu/function//string/operations/reg_expr_operation.h"
// #include "kuzu/function//string/operations/repeat_operation.h"
// #include "kuzu/function//string/operations/right_operation.h"
// #include "kuzu/function//string/operations/rpad_operation.h"
// #include "kuzu/function//string/operations/starts_with_operation.h"
// #include "kuzu/function//string/operations/substr_operation.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>>
ArrayExtractVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ARRAY_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ConcatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONCAT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ContainsVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONTAINS_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> EndsWithVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ENDS_WITH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> REMatchVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RE_MATCH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LeftVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LEFT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LengthVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LENGTH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING}, DataTypeID::INT64,
        empty_scalar_exec_func(), false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LENGTH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::PATH}, DataTypeID::INT64,
        empty_scalar_exec_func(), false)); // shortest path
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LpadVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LPAD_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64, DataTypeID::STRING}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RepeatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REPEAT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RightVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RIGHT_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RpadVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RPAD_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64, DataTypeID::STRING}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
StartsWithVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(STARTS_WITH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
PrefixVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(PREFIX_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
RegexMatchesVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_MATCH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_MATCH_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::STRING, DataTypeID::STRING}, DataTypeID::BOOLEAN,
        empty_scalar_exec_func(),
        empty_scalar_select_func(),
        false /* isVarLength */));
    return definitions;
}


std::vector<std::unique_ptr<VectorOperationDefinition>> SubStrVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(SUBSTRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(SUBSTRING_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INTEGER, DataTypeID::INTEGER}, DataTypeID::STRING,
        empty_scalar_exec_func(),
        false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
