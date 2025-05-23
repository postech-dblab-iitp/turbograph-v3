#pragma once

#include "kuzu/common/types/literal.h"
#include "kuzu/common/types/types_include.h"

namespace kuzu {
namespace common {

class TypeUtils {

public:
    static int32_t convertToInt32(const char* data);
    static int64_t convertToInt64(const char* data);
    static uint32_t convertToUint32(const char* data);
    static uint64_t convertToUint64(const char* data);
    static double_t convertToDouble(const char* data);
    static bool convertToBoolean(const char* data);

    static inline string toString(bool boolVal) { return boolVal ? "True" : "False"; }
    static inline string toString(int32_t val) { return to_string(val); }
    static inline string toString(int64_t val) { return to_string(val); }
    static inline string toString(uint32_t val) { return to_string(val); }
    static inline string toString(uint64_t val) { return to_string(val); }
    static inline string toString(double val) { return to_string(val); }
    static inline string toString(const nodeID_t& val) {
        return to_string(val.tableID) + ":" + to_string(val.offset);
    }
    static inline string toString(const date_t& val) { return Date::toString(val); }
    static inline string toString(const timestamp_t& val) { return Timestamp::toString(val); }
    static inline string toString(const interval_t& val) { return Interval::toString(val); }
    static inline string toString(const ku_string_t& val) { return val.getAsString(); }
    static inline string toString(const string& val) { return val; }
    static string toString(const ku_list_t& val, const DataType& dataType);
    static string toString(const Literal& literal);

    static inline void encodeOverflowPtr(
        uint64_t& overflowPtr, page_idx_t pageIdx, uint16_t pageOffset) {
        memcpy(&overflowPtr, &pageIdx, 4);
        memcpy(((uint8_t*)&overflowPtr) + 4, &pageOffset, 2);
    }
    static inline void decodeOverflowPtr(
        uint64_t overflowPtr, page_idx_t& pageIdx, uint16_t& pageOffset) {
        pageIdx = 0;
        memcpy(&pageIdx, &overflowPtr, 4);
        memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 4, 2);
    }

    template<typename T>
    static inline bool isValueEqual(
        T& left, T& right, const DataType& leftDataType, const DataType& rightDataType) {
        return left == right;
    }

private:
    static string elementToString(const DataType& dataType, uint8_t* overflowPtr, uint64_t pos);

    static void throwConversionExceptionOutOfRange(const char* data, DataTypeID dataTypeID);
    static void throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
        const char* data, const char* eptr, DataTypeID dataTypeID);
    static string prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID);
};

template<>
inline bool TypeUtils::isValueEqual(ku_list_t& left, ku_list_t& right, const DataType& leftDataType,
    const DataType& rightDataType) {
    if (leftDataType != rightDataType || left.size != right.size) {
        return false;
    }

    for (auto i = 0u; i < left.size; i++) {
        switch (leftDataType.childType->typeID) {
        case DataTypeID::BOOLEAN: {
            if (!isValueEqual(reinterpret_cast<uint8_t*>(left.overflowPtr)[i],
                    reinterpret_cast<uint8_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::INT64: {
            if (!isValueEqual(reinterpret_cast<int64_t*>(left.overflowPtr)[i],
                    reinterpret_cast<int64_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::UBIGINT: {
            if (!isValueEqual(reinterpret_cast<uint64_t*>(left.overflowPtr)[i],
                    reinterpret_cast<uint64_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::DOUBLE: {
            if (!isValueEqual(reinterpret_cast<double_t*>(left.overflowPtr)[i],
                    reinterpret_cast<double_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::STRING: {
            if (!isValueEqual(reinterpret_cast<ku_string_t*>(left.overflowPtr)[i],
                    reinterpret_cast<ku_string_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::DATE: {
            if (!isValueEqual(reinterpret_cast<date_t*>(left.overflowPtr)[i],
                    reinterpret_cast<date_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::TIMESTAMP: {
            if (!isValueEqual(reinterpret_cast<timestamp_t*>(left.overflowPtr)[i],
                    reinterpret_cast<timestamp_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::INTERVAL: {
            if (!isValueEqual(reinterpret_cast<interval_t*>(left.overflowPtr)[i],
                    reinterpret_cast<interval_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DataTypeID::LIST: {
            if (!isValueEqual(reinterpret_cast<ku_list_t*>(left.overflowPtr)[i],
                    reinterpret_cast<ku_list_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        default: {
            throw RuntimeException("Unsupported data type " +
                                   Types::dataTypeToString(leftDataType) +
                                   " for TypeUtils::isValueEqual.");
        }
        }
    }
    return true;
}

} // namespace common
} // namespace kuzu
