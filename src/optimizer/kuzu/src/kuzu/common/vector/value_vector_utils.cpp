// #include "kuzu/common/vector/value_vector_utils.h"

// #include "kuzu/common/in_mem_overflow_buffer_utils.h"

// using namespace kuzu;
// using namespace common;

// void ValueVectorUtils::addLiteralToValueVector(
//     ValueVector& resultVector, uint64_t pos, const Literal& literal) {
//     if (literal.isNull()) {
//         resultVector.setNull(pos, true);
//         return;
//     }
//     switch (literal.dataType.typeID) {
//     case DataTypeID::INT64: {
//         resultVector.setValue(pos, literal.val.int64Val);
//     } break;
//     case DataTypeID::DOUBLE: {
//         resultVector.setValue(pos, literal.val.doubleVal);
//     } break;
//     case BOOL: {
//         resultVector.setValue(pos, literal.val.booleanVal);
//     } break;
//     case DataTypeID::DATE: {
//         resultVector.setValue(pos, literal.val.dateVal);
//     } break;
//     case DataTypeID::TIMESTAMP: {
//         resultVector.setValue(pos, literal.val.timestampVal);
//     } break;
//     case DataTypeID::INTERVAL: {
//         resultVector.setValue(pos, literal.val.intervalVal);
//     } break;
//     case DataTypeID::STRING: {
//         resultVector.setValue(pos, literal.strVal);
//     } break;
//     default:
//         assert(false);
//     }
// }

// void ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(
//     ValueVector& resultVector, uint64_t pos, const uint8_t* srcData) {
//     copyNonNullDataWithSameType(resultVector.dataType, srcData,
//         resultVector.getData() + pos * resultVector.getNumBytesPerValue(),
//         resultVector.getOverflowBuffer());
// }

// void ValueVectorUtils::copyNonNullDataWithSameTypeOutFromPos(const ValueVector& srcVector,
//     uint64_t pos, uint8_t* dstData, InMemOverflowBuffer& dstOverflowBuffer) {
//     copyNonNullDataWithSameType(srcVector.dataType,
//         srcVector.getData() + pos * srcVector.getNumBytesPerValue(), dstData, dstOverflowBuffer);
// }

// void ValueVectorUtils::copyNonNullDataWithSameType(const DataType& dataType, const uint8_t* srcData,
//     uint8_t* dstData, InMemOverflowBuffer& inMemOverflowBuffer) {
//     if (dataType.typeID == DataTypeID::STRING) {
//         InMemOverflowBufferUtils::copyString(
//             *(ku_string_t*)srcData, *(ku_string_t*)dstData, inMemOverflowBuffer);
//     } else if (dataType.typeID == DataTypeID::LIST) {
//         InMemOverflowBufferUtils::copyListRecursiveIfNested(
//             *(ku_list_t*)srcData, *(ku_list_t*)dstData, dataType, inMemOverflowBuffer);
//     } else {
//         memcpy(dstData, srcData, Types::getDataTypeSize(dataType));
//     }
// }
