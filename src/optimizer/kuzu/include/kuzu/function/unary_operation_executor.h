// #pragma once

// #include <functional>

// #include "kuzu/common/vector/value_vector.h"

// namespace kuzu {
// namespace function {

// /**
//  * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
//  * IS_NOT_NULL operation.
//  */

// struct UnaryOperationWrapper {
//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
//     static inline void operation(
//         OPERAND_TYPE& input, RESULT_TYPE& result, void* dataptr, const DataType& inputType) {
//         FUNC::operation(input, result);
//     }
// };

// struct UnaryStringOperationWrapper {
//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void operation(
//         OPERAND_TYPE& input, RESULT_TYPE& result, void* dataptr, const DataType& inputType) {
//         FUNC::operation(input, result, *(ValueVector*)dataptr);
//     }
// };

// struct UnaryCastOperationWrapper {
//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void operation(
//         OPERAND_TYPE& input, RESULT_TYPE& result, void* dataptr, const DataType& inputType) {
//         FUNC::operation(input, result, *(ValueVector*)dataptr, inputType);
//     }
// };

// struct UnaryOperationExecutor {
//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
//     static void executeOnValue(ValueVector& operand, uint64_t operandPos, RESULT_TYPE& resultValue,
//         ValueVector& resultValueVector) {
//         OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
//             ((OPERAND_TYPE*)operand.getData())[operandPos], resultValue, (void*)&resultValueVector,
//             operand.dataType);
//     }

//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
//     static void executeSwitch(ValueVector& operand, ValueVector& result) {
//         result.resetOverflowBuffer();
//         auto resultValues = (RESULT_TYPE*)result.getData();
//         if (operand.state->isFlat()) {
//             auto pos = operand.state->selVector->selectedPositions[0];
//             result.setNull(pos, operand.isNull(pos));
//             if (!result.isNull(pos)) {
//                 executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                     operand, pos, resultValues[pos], result);
//             }
//         } else {
//             if (operand.hasNoNullsGuarantee()) {
//                 if (operand.state->selVector->isUnfiltered()) {
//                     for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
//                         executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             operand, i, resultValues[i], result);
//                     }
//                 } else {
//                     for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
//                         auto pos = operand.state->selVector->selectedPositions[i];
//                         executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             operand, pos, resultValues[pos], result);
//                     }
//                 }
//             } else {
//                 if (operand.state->selVector->isUnfiltered()) {
//                     for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
//                         result.setNull(i, operand.isNull(i));
//                         if (!result.isNull(i)) {
//                             executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                                 operand, i, resultValues[i], result);
//                         }
//                     }
//                 } else {
//                     for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
//                         auto pos = operand.state->selVector->selectedPositions[i];
//                         result.setNull(pos, operand.isNull(pos));
//                         if (!result.isNull(pos)) {
//                             executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                                 operand, pos, resultValues[pos], result);
//                         }
//                     }
//                 }
//             }
//         }
//     }

//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void execute(ValueVector& operand, ValueVector& result) {
//         executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryOperationWrapper>(operand, result);
//     }

//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void executeString(ValueVector& operand, ValueVector& result) {
//         executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryStringOperationWrapper>(
//             operand, result);
//     }

//     template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void executeCast(ValueVector& operand, ValueVector& result) {
//         executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryCastOperationWrapper>(operand, result);
//     }
// };

// } // namespace function
// } // namespace kuzu
