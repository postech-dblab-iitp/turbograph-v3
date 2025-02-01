#include "kuzu/common/types/ku_list.h"
// #include "kuzu/function//list/operations/list_append_operation.h"
// #include "kuzu/function//list/operations/list_concat_operation.h"
#include "kuzu/function//list/operations/list_contains.h"
// #include "kuzu/function//list/operations/list_extract_operation.h"
// #include "kuzu/function//list/operations/list_len_operation.h"
// #include "kuzu/function//list/operations/list_position_operation.h"
// #include "kuzu/function//list/operations/list_prepend_operation.h"
// #include "kuzu/function//list/operations/list_slice_operation.h"
#include "kuzu/function//list/vector_list_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::string getListFunctionIncompatibleChildrenTypeErrorMsg(
    const std::string& functionName, const DataType& left, const DataType& right) {
    return std::string("Cannot bind " + functionName + " with parameter type " +
                       Types::dataTypeToString(left) + " and " + Types::dataTypeToString(right) +
                       ".");
}

// void VectorListOperations::ListCreation(
//     const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
//     assert(!parameters.empty() && result.dataType.typeID == DataTypeID::LIST);
//     result.resetOverflowBuffer();
//     auto& childType = parameters[0]->dataType;
//     auto numBytesOfListElement = Types::getDataTypeSize(childType);
//     auto elements = std::make_unique<uint8_t[]>(parameters.size() * numBytesOfListElement);
//     for (auto selectedPos = 0u; selectedPos < result.state->selVector->selectedSize;
//          ++selectedPos) {
//         auto pos = result.state->selVector->selectedPositions[selectedPos];
//         auto& kuList = ((ku_list_t*)result.getData())[pos];
//         for (auto paramIdx = 0u; paramIdx < parameters.size(); paramIdx++) {
//             auto paramPos = parameters[paramIdx]->state->isFlat() ?
//                                 parameters[paramIdx]->state->selVector->selectedPositions[0] :
//                                 pos;
//             memcpy(elements.get() + paramIdx * numBytesOfListElement,
//                 parameters[paramIdx]->getData() + paramPos * numBytesOfListElement,
//                 numBytesOfListElement);
//         }
//         ku_list_t tmpList(parameters.size(), (uint64_t)elements.get());
//         InMemOverflowBufferUtils::copyListRecursiveIfNested(
//             tmpList, kuList, result.dataType, result.getOverflowBuffer());
//     }
// }

void ListCreationVectorOperation::listCreationBindFunc(const std::vector<DataType>& argumentTypes,
    VectorOperationDefinition* definition, DataType& actualReturnType) {
    if (argumentTypes.empty()) {
        throw BinderException(
            "Cannot resolve child data type for " + LIST_CREATION_FUNC_NAME + ".");
    }
    for (auto i = 1u; i < argumentTypes.size(); i++) {
        if (argumentTypes[i] != argumentTypes[0]) {
            throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
                LIST_CREATION_FUNC_NAME, argumentTypes[0], argumentTypes[i]));
        }
    }
    definition->returnTypeID = DataTypeID::LIST;
    actualReturnType = DataType(DataTypeID::LIST, std::make_unique<DataType>(argumentTypes[0]));
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListCreationVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CREATION_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::ANY}, DataTypeID::LIST, empty_scalar_exec_func(), nullptr, listCreationBindFunc,
        true /* isVarlength*/));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ListLenVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    // auto execFunc = UnaryExecFunction<ku_list_t, int64_t, operation::ListLen>;
    result.push_back(std::make_unique<VectorOperationDefinition>(
        LIST_LEN_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::LIST}, DataTypeID::INT64, empty_scalar_exec_func(), true /* isVarlength*/));
    return result;
}

// void ListExtractVectorOperation::listExtractBindFunc(const std::vector<DataType>& argumentTypes,
//     VectorOperationDefinition* definition, DataType& returnType) {
//     definition->returnTypeID = argumentTypes[0].childType->typeID;
//     returnType = *argumentTypes[0].childType;
//     switch (definition->returnTypeID) {
//     case BOOL: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, uint8_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::INT64: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, int64_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::DOUBLE: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, double_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::DATE: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, date_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::TIMESTAMP: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, timestamp_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::INTERVAL: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, interval_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::STRING: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, ku_string_t, operation::ListExtract>;
//     } break;
//     case DataTypeID::LIST: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, int64_t, ku_list_t, operation::ListExtract>;
//     } break;
//     default: {
//         assert(false);
//     }
//     }
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// ListExtractVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::LIST, DataTypeID::INT64}, DataTypeID::ANY, nullptr, nullptr, listExtractBindFunc,
//         false /* isVarlength*/));
//     result.push_back(std::make_unique<VectorOperationDefinition>(LIST_EXTRACT_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64}, DataTypeID::STRING,
//         BinaryExecFunction<ku_string_t, int64_t, ku_string_t, operation::ListExtract>,
//         false /* isVarlength */));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// ListConcatVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     auto execFunc = BinaryListExecFunction<ku_list_t, ku_list_t, ku_list_t, operation::ListConcat>;
//     auto bindFunc = [](const std::vector<DataType>& argumentTypes,
//                         VectorOperationDefinition* definition, DataType& actualReturnType) {
//         if (argumentTypes[0] != argumentTypes[1]) {
//             throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
//                 LIST_CONCAT_FUNC_NAME, argumentTypes[0], argumentTypes[1]));
//         }
//         definition->returnTypeID = argumentTypes[0].typeID;
//         actualReturnType = argumentTypes[0];
//     };
//     result.push_back(std::make_unique<VectorOperationDefinition>(LIST_CONCAT_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::LIST, DataTypeID::LIST}, DataTypeID::LIST, execFunc, nullptr, bindFunc,
//         false /* isVarlength*/));
//     return result;
// }

void ListAppendVectorOperation::listAppendBindFunc(const std::vector<DataType>& argumentTypes,
    VectorOperationDefinition* definition, DataType& returnType) {
    if (*argumentTypes[0].childType != argumentTypes[1]) {
        throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
            LIST_APPEND_FUNC_NAME, argumentTypes[0], argumentTypes[1]));
    }
    definition->returnTypeID = argumentTypes[0].typeID;
    returnType = argumentTypes[0];
    switch (argumentTypes[1].typeID) {
    case DataTypeID::INT64:
    case DataTypeID::DOUBLE:
    case DataTypeID::BOOLEAN:
    case DataTypeID::STRING:
    case DataTypeID::DATE:
    case DataTypeID::TIMESTAMP:
    case DataTypeID::INTERVAL:
    case DataTypeID::LIST: {
        definition->execFunc = empty_scalar_exec_func();
    } break;
    // case DataTypeID::INT64: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, int64_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case DataTypeID::DOUBLE: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, double_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case BOOL: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, uint8_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case DataTypeID::STRING: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, ku_string_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case DataTypeID::DATE: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, date_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case DataTypeID::TIMESTAMP: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, timestamp_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case DataTypeID::INTERVAL: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, interval_t, ku_list_t, operation::ListAppend>;
    // } break;
    // case DataTypeID::LIST: {
    //     definition->execFunc =
    //         BinaryListExecFunction<ku_list_t, ku_list_t, ku_list_t, operation::ListAppend>;
    // } break;
    default: {
        assert(false);
    }
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListAppendVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(LIST_APPEND_FUNC_NAME,
        std::vector<DataTypeID>{DataTypeID::LIST, DataTypeID::ANY}, DataTypeID::LIST, nullptr, nullptr, listAppendBindFunc,
        false /* isVarlength*/));
    return result;
}

// void ListPrependVectorOperation::listPrependBindFunc(const std::vector<DataType>& argumentTypes,
//     VectorOperationDefinition* definition, DataType& returnType) {
//     if (argumentTypes[0] != *argumentTypes[1].childType) {
//         throw BinderException(getListFunctionIncompatibleChildrenTypeErrorMsg(
//             LIST_APPEND_FUNC_NAME, argumentTypes[0], argumentTypes[1]));
//     }
//     definition->returnTypeID = argumentTypes[1].typeID;
//     returnType = argumentTypes[1];
//     switch (argumentTypes[0].typeID) {
//     case DataTypeID::INT64: {
//         definition->execFunc =
//             BinaryListExecFunction<int64_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case DataTypeID::DOUBLE: {
//         definition->execFunc =
//             BinaryListExecFunction<double_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case BOOL: {
//         definition->execFunc =
//             BinaryListExecFunction<uint8_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case DataTypeID::STRING: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_string_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case DataTypeID::DATE: {
//         definition->execFunc =
//             BinaryListExecFunction<date_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case DataTypeID::TIMESTAMP: {
//         definition->execFunc =
//             BinaryListExecFunction<timestamp_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case DataTypeID::INTERVAL: {
//         definition->execFunc =
//             BinaryListExecFunction<interval_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     case DataTypeID::LIST: {
//         definition->execFunc =
//             BinaryListExecFunction<ku_list_t, ku_list_t, ku_list_t, operation::ListPrepend>;
//     } break;
//     default: {
//         assert(false);
//     }
//     }
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// ListPrependVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(std::make_unique<VectorOperationDefinition>(LIST_PREPEND_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::ANY, DataTypeID::LIST}, DataTypeID::LIST, nullptr, nullptr, listPrependBindFunc,
//         false /* isVarlength*/));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// ListPositionVectorOperation::getDefinitions() {
//     return getBinaryListOperationDefinitions<operation::ListPosition, int64_t>(
//         LIST_POSITION_FUNC_NAME, DataTypeID::INT64);
// }

std::vector<std::unique_ptr<VectorOperationDefinition>>
ListContainsVectorOperation::getDefinitions() {
    return getBinaryListOperationDefinitions<operation::ListContains, uint8_t>(
        LIST_CONTAINS_FUNC_NAME, DataTypeID::BOOLEAN);
}

std::vector<std::unique_ptr<VectorOperationDefinition>> PathListLenVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(std::make_unique<VectorOperationDefinition>(
        PATH_LIST_LEN_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::PATH}, DataTypeID::INTEGER, empty_scalar_exec_func(), true /* isVarlength*/));
    return result;
}

// std::vector<std::unique_ptr<VectorOperationDefinition>> ListSliceVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     auto bindFunc = [](const std::vector<DataType>& argumentTypes,
//                         VectorOperationDefinition* definition, DataType& actualReturnType) {
//         definition->returnTypeID = argumentTypes[0].typeID;
//         actualReturnType = argumentTypes[0];
//     };
//     result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::LIST, DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::LIST,
//         TernaryListExecFunction<ku_list_t, int64_t, int64_t, ku_list_t, operation::ListSlice>,
//         nullptr, bindFunc, false /* isVarlength*/));
//     result.push_back(std::make_unique<VectorOperationDefinition>(LIST_SLICE_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::STRING, DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::STRING,
//         TernaryListExecFunction<ku_string_t, int64_t, int64_t, ku_string_t, operation::ListSlice>,
//         false /* isVarlength */));
//     return result;
// }

} // namespace function
} // namespace kuzu
