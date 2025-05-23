#pragma once

#include "kuzu/function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorListOperations : public VectorOperations {

    // template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    // static void TernaryListExecFunction(
    //     const std::vector<std::shared_ptr<common::ValueVector>>& params,
    //     common::ValueVector& result) {
    //     assert(params.size() == 3);
    //     TernaryOperationExecutor::executeStringAndList<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
    //         *params[0], *params[1], *params[2], result);
    // }

    // template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    // static void BinaryListPosAndContainsExecFunction(
    //     const std::vector<std::shared_ptr<common::ValueVector>>& params,
    //     common::ValueVector& result) {
    //     assert(params.size() == 2);
    //     BinaryOperationExecutor::executeListPosAndContains<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE,
    //         FUNC>(*params[0], *params[1], result);
    // }

    // template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    // static void BinaryListExecFunction(
    //     const std::vector<std::shared_ptr<common::ValueVector>>& params,
    //     common::ValueVector& result) {
    //     assert(params.size() == 2);
    //     BinaryOperationExecutor::executeStringAndList<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
    //         *params[0], *params[1], result);
    // }

    // static void ListCreation(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    //     common::ValueVector& result);

    template<typename OPERATION, typename RESULT_TYPE>
    static std::vector<std::unique_ptr<VectorOperationDefinition>>
    getBinaryListOperationDefinitions(std::string funcName, common::DataTypeID resultTypeID) {
        std::vector<std::unique_ptr<VectorOperationDefinition>> result;
        scalar_exec_func execFunc;
        for (auto& rightTypeID :
            std::vector<common::DataTypeID>{common::DataTypeID::BOOLEAN, common::DataTypeID::INT64, common::DataTypeID::UBIGINT, common::DataTypeID::DOUBLE,
                common::DataTypeID::STRING, common::DataTypeID::DATE, common::DataTypeID::TIMESTAMP, common::DataTypeID::INTERVAL, common::DataTypeID::LIST, common::DataTypeID::NODE_ID}) {
            // switch (rightTypeID) {
            // case common::BOOL: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t, uint8_t,
            //         RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::INT64: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t, int64_t,
            //         RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::DOUBLE: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t, double_t,
            //         RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::STRING: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t,
            //         common::ku_string_t, RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::DATE: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t, common::date_t,
            //         RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::TIMESTAMP: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t,
            //         common::timestamp_t, RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::INTERVAL: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t,
            //         common::interval_t, RESULT_TYPE, OPERATION>;
            // } break;
            // case common::DataTypeID::LIST: {
            //     execFunc = BinaryListPosAndContainsExecFunction<common::ku_list_t,
            //         common::ku_list_t, RESULT_TYPE, OPERATION>;
            // } break;
            // default: {
            //     assert(false);
            // }
            // }
            result.push_back(make_unique<VectorOperationDefinition>(funcName,
                std::vector<common::DataTypeID>{common::DataTypeID::LIST, rightTypeID}, resultTypeID, empty_scalar_exec_func(),
                nullptr, false /* isVarlength*/));
        }
        // result.push_back(make_unique<VectorOperationDefinition>(funcName,
        //     std::vector<common::DataTypeID>{common::DataTypeID::LIST, common::DataTypeID::ANY}, resultTypeID, empty_scalar_exec_func(),
        //     nullptr, false /* isVarlength*/));
        return result;
    }
};

struct ListCreationVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static void listCreationBindFunc(const std::vector<common::DataType>& argumentTypes,
        VectorOperationDefinition* definition, common::DataType& actualReturnType);
};

struct ListLenVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

// struct ListExtractVectorOperation : public VectorListOperations {
//     static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
//     static void listExtractBindFunc(const std::vector<common::DataType>& argumentTypes,
//         VectorOperationDefinition* definition, common::DataType& returnType);
// };

// struct ListConcatVectorOperation : public VectorListOperations {
//     static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
// };

struct ListAppendVectorOperation : public VectorListOperations {
    static void listAppendBindFunc(const std::vector<common::DataType>& argumentTypes,
        VectorOperationDefinition* definition, common::DataType& returnType);
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

// struct ListPrependVectorOperation : public VectorListOperations {
//     static void listPrependBindFunc(const std::vector<common::DataType>& argumentTypes,
//         VectorOperationDefinition* definition, common::DataType& returnType);
//     static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
// };

// struct ListPositionVectorOperation : public VectorListOperations {
//     static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
// };

struct ListContainsVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

// struct ListSliceVectorOperation : public VectorListOperations {
//     static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
// };

struct PathListLenVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu
