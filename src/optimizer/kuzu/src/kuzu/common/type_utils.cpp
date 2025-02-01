#include "kuzu/common/type_utils.h"

#include <cerrno>
#include <climits>

#include "kuzu/common/exception.h"
#include "kuzu/common/utils.h"

namespace kuzu {
namespace common {

int32_t TypeUtils::convertToInt32(const char* data) {
    istringstream iss(data);
    int32_t val;
    if (!(iss >> val)) {
        throw ConversionException(
            StringUtils::string_format("Failed to convert %s to int32_t", data));
    }
    return val;
}

int64_t TypeUtils::convertToInt64(const char* data) {
    char* eptr;
    errno = 0;
    auto retVal = strtoll(data, &eptr, 10);
    throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(data, eptr, DataTypeID::INT64);
    if ((LLONG_MAX == retVal || LLONG_MIN == retVal) && errno == ERANGE) {
        throw ConversionException(
            prefixConversionExceptionMessage(data, DataTypeID::INT64) + " Input out of range.");
    }
    return retVal;
}

uint32_t TypeUtils::convertToUint32(const char* data) {
    istringstream iss(data);
    uint32_t val;
    if (!(iss >> val)) {
        throw ConversionException(
            StringUtils::string_format("Failed to convert %s to uint32_t", data));
    }
    return val;
}

uint64_t TypeUtils::convertToUint64(const char* data) {
    char* eptr;
    errno = 0;
    auto retVal = strtoull(data, &eptr, 10);
    throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(data, eptr, DataTypeID::INT64);
    if ((ULLONG_MAX == retVal) && errno == ERANGE) {
        throw ConversionException(
            prefixConversionExceptionMessage(data, DataTypeID::UBIGINT) + " Input out of range.");
    }
    return retVal;
}

double_t TypeUtils::convertToDouble(const char* data) {
    char* eptr;
    errno = 0;
    auto retVal = strtod(data, &eptr);
    throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(data, eptr, DataTypeID::DOUBLE);
    if ((HUGE_VAL == retVal || -HUGE_VAL == retVal) && errno == ERANGE) {
        throwConversionExceptionOutOfRange(data, DataTypeID::DOUBLE);
    }
    return retVal;
};

bool TypeUtils::convertToBoolean(const char* data) {
    auto len = strlen(data);
    if (len == 4 && 't' == tolower(data[0]) && 'r' == tolower(data[1]) && 'u' == tolower(data[2]) &&
        'e' == tolower(data[3])) {
        return true;
    } else if (len == 5 && 'f' == tolower(data[0]) && 'a' == tolower(data[1]) &&
               'l' == tolower(data[2]) && 's' == tolower(data[3]) && 'e' == tolower(data[4])) {
        return false;
    }
    throw ConversionException(
        prefixConversionExceptionMessage(data, DataTypeID::BOOLEAN) +
        ". Input is not equal to True or False (in a case-insensitive manner)");
}

string TypeUtils::elementToString(const DataType& dataType, uint8_t* overflowPtr, uint64_t pos) {
    switch (dataType.typeID) {
    case DataTypeID::BOOLEAN:
        return TypeUtils::toString(((bool*)overflowPtr)[pos]);
    case DataTypeID::INTEGER:
        return TypeUtils::toString(((int32_t*)overflowPtr)[pos]);
    case DataTypeID::INT64:
        return TypeUtils::toString(((int64_t*)overflowPtr)[pos]);
    case DataTypeID::UINTEGER:
        return TypeUtils::toString(((uint32_t*)overflowPtr)[pos]);
    case DataTypeID::UBIGINT:
        return TypeUtils::toString(((uint64_t*)overflowPtr)[pos]);
    case DataTypeID::DOUBLE:
        return TypeUtils::toString(((double_t*)overflowPtr)[pos]);
    case DataTypeID::DATE:
        return TypeUtils::toString(((date_t*)overflowPtr)[pos]);
    case DataTypeID::TIMESTAMP:
        return TypeUtils::toString(((timestamp_t*)overflowPtr)[pos]);
    case DataTypeID::INTERVAL:
        return TypeUtils::toString(((interval_t*)overflowPtr)[pos]);
    case DataTypeID::STRING:
        return TypeUtils::toString(((ku_string_t*)overflowPtr)[pos]);
    case DataTypeID::LIST:
        return TypeUtils::toString(((ku_list_t*)overflowPtr)[pos], dataType);
    default:
        throw RuntimeException("Invalid data type " + Types::dataTypeToString(dataType) +
                               " for TypeUtils::elementToString.");
    }
}

string TypeUtils::toString(const ku_list_t& val, const DataType& dataType) {
    string result = "[";
    for (auto i = 0u; i < val.size - 1; ++i) {
        result +=
            elementToString(*dataType.childType, reinterpret_cast<uint8_t*>(val.overflowPtr), i) +
            ",";
    }
    result += elementToString(
                  *dataType.childType, reinterpret_cast<uint8_t*>(val.overflowPtr), val.size - 1) +
              "]";
    return result;
}

string TypeUtils::toString(const Literal& literal) {
    if (literal.isNull()) {
        return "NULL";
    }
    switch (literal.dataType.typeID) {
    case DataTypeID::BOOLEAN:
        return TypeUtils::toString(literal.val.booleanVal);
    case DataTypeID::INTEGER:
        return TypeUtils::toString(literal.val.int32Val);
    case DataTypeID::INT64:
        return TypeUtils::toString(literal.val.int64Val);
    case DataTypeID::UINTEGER:
        return TypeUtils::toString(literal.val.uint32Val);
    case DataTypeID::UBIGINT:
        return TypeUtils::toString(literal.val.uint64Val);
    case DataTypeID::DOUBLE:
        return TypeUtils::toString(literal.val.doubleVal);
    case DataTypeID::NODE_ID:
        return TypeUtils::toString(literal.val.nodeID);
    case DataTypeID::DATE:
        return TypeUtils::toString(literal.val.dateVal);
    case DataTypeID::TIMESTAMP:
        return TypeUtils::toString(literal.val.timestampVal);
    case DataTypeID::INTERVAL:
        return TypeUtils::toString(literal.val.intervalVal);
    case DataTypeID::STRING:
        return literal.strVal;
    case DataTypeID::LIST: {
        if (literal.listVal.empty()) {
            return "[]";
        }
        string result = "[";
        for (auto i = 0u; i < literal.listVal.size() - 1; i++) {
            result += toString(literal.listVal[i]) + ",";
        }
        result += toString(literal.listVal[literal.listVal.size() - 1]);
        result += "]";
        return result;
    }
    default:
        throw RuntimeException("Invalid data type " + Types::dataTypeToString(literal.dataType) +
                               " for TypeUtils::toString.");
    }
}

string TypeUtils::prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID) {
    return "Cannot convert string " + string(data) + " to " + Types::dataTypeToString(dataTypeID) +
           ".";
}

void TypeUtils::throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
    const char* data, const char* eptr, DataTypeID dataTypeID) {
    if (data == eptr) {
        throw ConversionException(prefixConversionExceptionMessage(data, dataTypeID) +
                                  ". Invalid input. No characters consumed.");
    }
    if (*eptr != '\0') {
        throw ConversionException(prefixConversionExceptionMessage(data, dataTypeID) +
                                  " Not all characters were read. read from character " + *data +
                                  " up to character: " + *eptr + ".");
    }
}

void TypeUtils::throwConversionExceptionOutOfRange(const char* data, DataTypeID dataTypeID) {
    throw ConversionException(
        prefixConversionExceptionMessage(data, dataTypeID) + " Input out of range.");
}

} // namespace common
} // namespace kuzu
