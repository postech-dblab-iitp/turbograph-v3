// #pragma once

// #include <functional>

// #include "kuzu/common/type_utils.h"
// #include "kuzu/common/vector/value_vector.h"

// namespace kuzu {
// namespace function {

// /**
//  * Binary operator assumes operation with null returns null. This does NOT applies to binary boolean
//  * operations (e.g. AND, OR, XOR).
//  */

// struct BinaryOperationWrapper {
//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
//     static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
//         void* leftValueVector, void* rightValueVector, void* resultValueVector) {
//         OP::operation(left, right, result);
//     }
// };

// struct BinaryStringAndListOperationWrapper {
//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
//     static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
//         void* leftValueVector, void* rightValueVector, void* resultValueVector) {
//         OP::operation(left, right, result, *(ValueVector*)resultValueVector);
//     }
// };

// struct BinaryListPosAndContainsOperationWrapper {
//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename OP>
//     static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
//         void* leftValueVector, void* rightValueVector, void* resultValueVector) {
//         OP::operation(left, right, result, ((ValueVector*)leftValueVector)->dataType,
//             ((ValueVector*)rightValueVector)->dataType);
//     }
// };

// struct BinaryOperationExecutor {
//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
//         typename OP_WRAPPER>
//     static inline void executeOnValue(ValueVector& left, ValueVector& right,
//         ValueVector& resultValueVector, uint64_t lPos, uint64_t rPos, uint64_t resPos) {
//         OP_WRAPPER::template operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
//             ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos],
//             ((RESULT_TYPE*)resultValueVector.getData())[resPos], (void*)&left, (void*)&right,
//             (void*)&resultValueVector);
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
//         typename OP_WRAPPER>
//     static void executeBothFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
//         auto lPos = left.state->selVector->selectedPositions[0];
//         auto rPos = right.state->selVector->selectedPositions[0];
//         auto resPos = result.state->selVector->selectedPositions[0];
//         result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
//         if (!result.isNull(resPos)) {
//             executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                 left, right, result, lPos, rPos, resPos);
//         }
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
//         typename OP_WRAPPER>
//     static void executeFlatUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
//         auto lPos = left.state->selVector->selectedPositions[0];
//         if (left.isNull(lPos)) {
//             result.setAllNull();
//         } else if (right.hasNoNullsGuarantee()) {
//             if (right.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                         left, right, result, lPos, i, i);
//                 }
//             } else {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     auto rPos = right.state->selVector->selectedPositions[i];
//                     executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                         left, right, result, lPos, rPos, rPos);
//                 }
//             }
//         } else {
//             if (right.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     result.setNull(i, right.isNull(i)); // left is always not null
//                     if (!result.isNull(i)) {
//                         executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             left, right, result, lPos, i, i);
//                     }
//                 }
//             } else {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     auto rPos = right.state->selVector->selectedPositions[i];
//                     result.setNull(rPos, right.isNull(rPos)); // left is always not null
//                     if (!result.isNull(rPos)) {
//                         executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             left, right, result, lPos, rPos, rPos);
//                     }
//                 }
//             }
//         }
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
//         typename OP_WRAPPER>
//     static void executeUnFlatFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
//         auto rPos = right.state->selVector->selectedPositions[0];
//         if (right.isNull(rPos)) {
//             result.setAllNull();
//         } else if (left.hasNoNullsGuarantee()) {
//             if (left.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                         left, right, result, i, rPos, i);
//                 }
//             } else {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     auto lPos = left.state->selVector->selectedPositions[i];
//                     executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                         left, right, result, lPos, rPos, lPos);
//                 }
//             }
//         } else {
//             if (left.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     result.setNull(i, left.isNull(i)); // right is always not null
//                     if (!result.isNull(i)) {
//                         executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             left, right, result, i, rPos, i);
//                     }
//                 }
//             } else {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     auto lPos = left.state->selVector->selectedPositions[i];
//                     result.setNull(lPos, left.isNull(lPos)); // right is always not null
//                     if (!result.isNull(lPos)) {
//                         executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             left, right, result, lPos, rPos, lPos);
//                     }
//                 }
//             }
//         }
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
//         typename OP_WRAPPER>
//     static void executeBothUnFlat(ValueVector& left, ValueVector& right, ValueVector& result) {
//         assert(left.state == right.state);
//         if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
//             if (result.state->selVector->isUnfiltered()) {
//                 for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
//                     executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                         left, right, result, i, i, i);
//                 }
//             } else {
//                 for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
//                     auto pos = result.state->selVector->selectedPositions[i];
//                     executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                         left, right, result, pos, pos, pos);
//                 }
//             }
//         } else {
//             if (result.state->selVector->isUnfiltered()) {
//                 for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
//                     result.setNull(i, left.isNull(i) || right.isNull(i));
//                     if (!result.isNull(i)) {
//                         executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             left, right, result, i, i, i);
//                     }
//                 }
//             } else {
//                 for (uint64_t i = 0; i < result.state->selVector->selectedSize; i++) {
//                     auto pos = result.state->selVector->selectedPositions[i];
//                     result.setNull(pos, left.isNull(pos) || right.isNull(pos));
//                     if (!result.isNull(pos)) {
//                         executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                             left, right, result, pos, pos, pos);
//                     }
//                 }
//             }
//         }
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC,
//         typename OP_WRAPPER>
//     static void executeSwitch(ValueVector& left, ValueVector& right, ValueVector& result) {
//         result.resetOverflowBuffer();
//         if (left.state->isFlat() && right.state->isFlat()) {
//             executeBothFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                 left, right, result);
//         } else if (left.state->isFlat() && !right.state->isFlat()) {
//             executeFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                 left, right, result);
//         } else if (!left.state->isFlat() && right.state->isFlat()) {
//             executeUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                 left, right, result);
//         } else if (!left.state->isFlat() && !right.state->isFlat()) {
//             executeBothUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
//                 left, right, result);
//         } else {
//             assert(false);
//         }
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
//         executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC, BinaryOperationWrapper>(
//             left, right, result);
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void executeStringAndList(ValueVector& left, ValueVector& right, ValueVector& result) {
//         executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
//             BinaryStringAndListOperationWrapper>(left, right, result);
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
//     static void executeListPosAndContains(
//         ValueVector& left, ValueVector& right, ValueVector& result) {
//         executeSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC,
//             BinaryListPosAndContainsOperationWrapper>(left, right, result);
//     }

//     template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
//     static void selectOnValue(ValueVector& left, ValueVector& right, uint64_t lPos, uint64_t rPos,
//         uint64_t resPos, uint64_t& numSelectedValues, sel_t* selectedPositionsBuffer) {
//         uint8_t resultValue = 0;
//         FUNC::operation(
//             ((LEFT_TYPE*)left.getData())[lPos], ((RIGHT_TYPE*)right.getData())[rPos], resultValue);
//         selectedPositionsBuffer[numSelectedValues] = resPos;
//         numSelectedValues += (resultValue == true);
//     }

//     template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
//     static uint64_t selectBothFlat(ValueVector& left, ValueVector& right) {
//         auto lPos = left.state->selVector->selectedPositions[0];
//         auto rPos = right.state->selVector->selectedPositions[0];
//         uint8_t resultValue = 0;
//         if (!left.isNull(lPos) && !right.isNull(rPos)) {
//             FUNC::operation(((LEFT_TYPE*)left.getData())[lPos],
//                 ((RIGHT_TYPE*)right.getData())[rPos], resultValue);
//         }
//         return resultValue == true;
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
//     static bool selectFlatUnFlat(
//         ValueVector& left, ValueVector& right, SelectionVector& selVector) {
//         auto lPos = left.state->selVector->selectedPositions[0];
//         uint64_t numSelectedValues = 0;
//         auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
//         if (left.isNull(lPos)) {
//             return numSelectedValues;
//         } else if (right.hasNoNullsGuarantee()) {
//             if (right.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                         left, right, lPos, i, i, numSelectedValues, selectedPositionsBuffer);
//                 }
//             } else {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     auto rPos = right.state->selVector->selectedPositions[i];
//                     selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                         left, right, lPos, rPos, rPos, numSelectedValues, selectedPositionsBuffer);
//                 }
//             }
//         } else {
//             if (right.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     if (!right.isNull(i)) {
//                         selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                             left, right, lPos, i, i, numSelectedValues, selectedPositionsBuffer);
//                     }
//                 }
//             } else {
//                 for (auto i = 0u; i < right.state->selVector->selectedSize; ++i) {
//                     auto rPos = right.state->selVector->selectedPositions[i];
//                     if (!right.isNull(rPos)) {
//                         selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, lPos, rPos, rPos,
//                             numSelectedValues, selectedPositionsBuffer);
//                     }
//                 }
//             }
//         }
//         selVector.selectedSize = numSelectedValues;
//         return numSelectedValues > 0;
//     }

//     template<typename LEFT_TYPE, typename RIGHT_TYPE, typename FUNC>
//     static bool selectUnFlatFlat(
//         ValueVector& left, ValueVector& right, SelectionVector& selVector) {
//         auto rPos = right.state->selVector->selectedPositions[0];
//         uint64_t numSelectedValues = 0;
//         auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
//         if (right.isNull(rPos)) {
//             return numSelectedValues;
//         } else if (left.hasNoNullsGuarantee()) {
//             if (left.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                         left, right, i, rPos, i, numSelectedValues, selectedPositionsBuffer);
//                 }
//             } else {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     auto lPos = left.state->selVector->selectedPositions[i];
//                     selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                         left, right, lPos, rPos, lPos, numSelectedValues, selectedPositionsBuffer);
//                 }
//             }
//         } else {
//             if (left.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     if (!left.isNull(i)) {
//                         selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                             left, right, i, rPos, i, numSelectedValues, selectedPositionsBuffer);
//                     }
//                 }
//             } else {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; ++i) {
//                     auto lPos = left.state->selVector->selectedPositions[i];
//                     if (!left.isNull(lPos)) {
//                         selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, lPos, rPos, lPos,
//                             numSelectedValues, selectedPositionsBuffer);
//                     }
//                 }
//             }
//         }
//         selVector.selectedSize = numSelectedValues;
//         return numSelectedValues > 0;
//     }

//     // Right, left, and result vectors share the same selectedPositions.
//     template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
//     static bool selectBothUnFlat(
//         ValueVector& left, ValueVector& right, SelectionVector& selVector) {
//         uint64_t numSelectedValues = 0;
//         auto selectedPositionsBuffer = selVector.getSelectedPositionsBuffer();
//         if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
//             if (left.state->selVector->isUnfiltered()) {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; i++) {
//                     selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                         left, right, i, i, i, numSelectedValues, selectedPositionsBuffer);
//                 }
//             } else {
//                 for (auto i = 0u; i < left.state->selVector->selectedSize; i++) {
//                     auto pos = left.state->selVector->selectedPositions[i];
//                     selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                         left, right, pos, pos, pos, numSelectedValues, selectedPositionsBuffer);
//                 }
//             }
//         } else {
//             if (left.state->selVector->isUnfiltered()) {
//                 for (uint64_t i = 0; i < left.state->selVector->selectedSize; i++) {
//                     auto isNull = left.isNull(i) || right.isNull(i);
//                     if (!isNull) {
//                         selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                             left, right, i, i, i, numSelectedValues, selectedPositionsBuffer);
//                     }
//                 }
//             } else {
//                 for (uint64_t i = 0; i < left.state->selVector->selectedSize; i++) {
//                     auto pos = left.state->selVector->selectedPositions[i];
//                     auto isNull = left.isNull(pos) || right.isNull(pos);
//                     if (!isNull) {
//                         selectOnValue<LEFT_TYPE, RIGHT_TYPE, FUNC>(
//                             left, right, pos, pos, pos, numSelectedValues, selectedPositionsBuffer);
//                     }
//                 }
//             }
//         }
//         selVector.selectedSize = numSelectedValues;
//         return numSelectedValues > 0;
//     }

//     // COMPARISON (GT, GTE, LT, LTE, EQ, NEQ), DataTypeID::BOOLEAN (AND, OR, XOR)
//     template<class LEFT_TYPE, class RIGHT_TYPE, class FUNC>
//     static bool select(ValueVector& left, ValueVector& right, SelectionVector& selVector) {
//         if (left.state->isFlat() && right.state->isFlat()) {
//             return selectBothFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right);
//         } else if (left.state->isFlat() && !right.state->isFlat()) {
//             return selectFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, selVector);
//         } else if (!left.state->isFlat() && right.state->isFlat()) {
//             return selectUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, selVector);
//         } else {
//             return selectBothUnFlat<LEFT_TYPE, RIGHT_TYPE, FUNC>(left, right, selVector);
//         }
//     }
// };

// } // namespace function
// } // namespace kuzu
