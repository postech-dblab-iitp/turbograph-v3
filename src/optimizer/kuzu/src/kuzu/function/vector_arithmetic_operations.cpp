#include "kuzu/function//arithmetic/vector_arithmetic_operations.h"

#include "kuzu/function//arithmetic/arithmetic_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

// TODO very strange..
static DataTypeID resolveResultType(DataTypeID leftTypeID, DataTypeID rightTypeID) {
    if (leftTypeID == DataTypeID::DOUBLE || rightTypeID == DataTypeID::DOUBLE) {
        return DataTypeID::DOUBLE;
    }
    if (leftTypeID == DataTypeID::DECIMAL || rightTypeID == DataTypeID::DECIMAL) {
        return DataTypeID::DECIMAL;
    }
    return DataTypeID::INT64;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> AddVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Add>(ADD_FUNC_NAME, leftTypeID,
                rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }
    // // date + int → date
    // result.push_back(
    //     make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::INT64},
    //         DataTypeID::DATE, BinaryExecFunction<date_t, int64_t, date_t, operation::Add>));
    // // int + date → date
    // result.push_back(
    //     make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::DATE},
    //         DataTypeID::DATE, BinaryExecFunction<int64_t, date_t, date_t, operation::Add>));
    // // date + interval → date
    // result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::INTERVAL}, DataTypeID::DATE,
    //     BinaryExecFunction<date_t, interval_t, date_t, operation::Add>));
    // // interval + date → date
    // result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::INTERVAL, DataTypeID::DATE}, DataTypeID::DATE,
    //     BinaryExecFunction<interval_t, date_t, date_t, operation::Add>));
    // // timestamp + interval → timestamp
    // result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::TIMESTAMP, DataTypeID::INTERVAL}, DataTypeID::TIMESTAMP,
    //     BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Add>));
    // // interval + timestamp → timestamp
    // result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::INTERVAL, DataTypeID::TIMESTAMP}, DataTypeID::TIMESTAMP,
    //     BinaryExecFunction<interval_t, timestamp_t, timestamp_t, operation::Add>));
    // // interval + interval → interval
    // result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::INTERVAL, DataTypeID::INTERVAL}, DataTypeID::INTERVAL,
    //     BinaryExecFunction<interval_t, interval_t, interval_t, operation::Add>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> SubtractVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Subtract>(SUBTRACT_FUNC_NAME,
                leftTypeID, rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }

    // // date - date → integer
    // result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::DATE}, DataTypeID::INT64,
    //     BinaryExecFunction<date_t, date_t, int64_t, operation::Subtract>));
    // // date - integer → date
    // result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::INT64}, DataTypeID::DATE,
    //     BinaryExecFunction<date_t, int64_t, date_t, operation::Subtract>));
    // // date - interval → date
    // result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::DATE, DataTypeID::INTERVAL}, DataTypeID::DATE,
    //     BinaryExecFunction<date_t, interval_t, date_t, operation::Subtract>));
    // // timestamp - timestamp → interval
    // result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::TIMESTAMP, DataTypeID::TIMESTAMP}, DataTypeID::INTERVAL,
    //     BinaryExecFunction<timestamp_t, timestamp_t, interval_t, operation::Subtract>));
    // // timestamp - interval → timestamp
    // result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::TIMESTAMP, DataTypeID::INTERVAL}, DataTypeID::TIMESTAMP,
    //     BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Subtract>));
    // // interval - interval → interval
    // result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::INTERVAL, DataTypeID::INTERVAL}, DataTypeID::INTERVAL,
    //     BinaryExecFunction<interval_t, interval_t, interval_t, operation::Subtract>));

    // Issue #111 (Handle NEDAGE function)
    for (auto& typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Negate>(NEGATE_FUNC_NAME, typeID, typeID));
    }
    
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> MultiplyVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Multiply>(MULTIPLY_FUNC_NAME,
                leftTypeID, rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DivideVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftType : DataType::getNumericalTypeIDs()) {
        for (auto& rightType : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Divide>(
                DIVIDE_FUNC_NAME, leftType, rightType, resolveResultType(leftType, rightType)));
        }
    }
    // // interval / int → interval
    // result.push_back(make_unique<VectorOperationDefinition>(DIVIDE_FUNC_NAME,
    //     std::vector<DataTypeID>{DataTypeID::INTERVAL, DataTypeID::INT64}, DataTypeID::INTERVAL,
    //     BinaryExecFunction<interval_t, int64_t, interval_t, operation::Divide>));
    return result;
}

// std::vector<std::unique_ptr<VectorOperationDefinition>> ModuloVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
//         for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
//             result.push_back(getBinaryDefinition<operation::Modulo>(MODULO_FUNC_NAME, leftTypeID,
//                 rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
//         }
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> PowerVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
//         for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
//             result.push_back(getBinaryDefinition<operation::Power, true>(
//                 POWER_FUNC_NAME, leftTypeID, rightTypeID, DataTypeID::DOUBLE));
//         }
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> NegateVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(getUnaryDefinition<operation::Negate>(NEGATE_FUNC_NAME, typeID, typeID));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> AbsVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(getUnaryDefinition<operation::Abs>(ABS_FUNC_NAME, typeID, typeID));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> FloorVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(getUnaryDefinition<operation::Floor>(FLOOR_FUNC_NAME, typeID, typeID));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> CeilVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(getUnaryDefinition<operation::Ceil>(CEIL_FUNC_NAME, typeID, typeID));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> SinVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Sin, false, true>(SIN_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> CosVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Cos, false, true>(COS_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> TanVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Tan, false, true>(TAN_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> CotVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Cot, false, true>(COT_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> AsinVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Asin, false, true>(ASIN_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> AcosVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Acos, false, true>(ACOS_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> AtanVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Atan, false, true>(ATAN_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> FactorialVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(
//         make_unique<VectorOperationDefinition>(FACTORIAL_FUNC_NAME, std::vector<DataTypeID>{DataTypeID::INT64},
//             DataTypeID::INT64, UnaryExecFunction<int64_t, int64_t, operation::Factorial>));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> SqrtVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Sqrt, false, true>(SQRT_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> CbrtVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Cbrt, false, true>(CBRT_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> GammaVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(getUnaryDefinition<operation::Gamma>(GAMMA_FUNC_NAME, typeID, typeID));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> LgammaVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Lgamma, false, true>(LGAMMA_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> LnVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Ln, false, true>(LN_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> LogVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Log, false, true>(LOG_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> Log2VectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Log2, false, true>(LOG2_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> DegreesVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Degrees, false, true>(DEGREES_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> RadiansVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Radians, false, true>(RADIANS_FUNC_NAME, typeID, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> EvenVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Even, true, false>(EVEN_FUNC_NAME, typeID, DataTypeID::INT64));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> SignVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& typeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(
//             getUnaryDefinition<operation::Sign, true, false>(SIGN_FUNC_NAME, typeID, DataTypeID::INT64));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> Atan2VectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
//         for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
//             result.push_back(getBinaryDefinition<operation::Atan2, true /* DOUBLE_RESULT */
//                 >(ATAN2_FUNC_NAME, leftTypeID, rightTypeID, DataTypeID::DOUBLE));
//         }
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> RoundVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
//         result.push_back(getBinaryDefinition<operation::Round, true /* DOUBLE_RESULT */>(
//             ROUND_FUNC_NAME, leftTypeID, DataTypeID::INT64, DataTypeID::DOUBLE));
//     }
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// BitwiseXorVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(make_unique<VectorOperationDefinition>(BITWISE_XOR_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::INT64,
//         BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitwiseXor>));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// BitwiseAndVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(make_unique<VectorOperationDefinition>(BITWISE_AND_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::INT64,
//         BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitwiseAnd>));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> BitwiseOrVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(make_unique<VectorOperationDefinition>(BITWISE_OR_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::INT64,
//         BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitwiseOr>));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// BitShiftLeftVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(make_unique<VectorOperationDefinition>(BITSHIFT_LEFT_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::INT64,
//         BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitShiftLeft>));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>>
// BitShiftRightVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(make_unique<VectorOperationDefinition>(BITSHIFT_RIGHT_FUNC_NAME,
//         std::vector<DataTypeID>{DataTypeID::INT64, DataTypeID::INT64}, DataTypeID::INT64,
//         BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitShiftRight>));
//     return result;
// }

// std::vector<std::unique_ptr<VectorOperationDefinition>> PiVectorOperation::getDefinitions() {
//     std::vector<std::unique_ptr<VectorOperationDefinition>> result;
//     result.push_back(make_unique<VectorOperationDefinition>(PI_FUNC_NAME, std::vector<DataTypeID>{},
//         DataTypeID::DOUBLE, ConstExecFunction<double_t, operation::Pi>));
//     return result;
// }

} // namespace function
} // namespace kuzu