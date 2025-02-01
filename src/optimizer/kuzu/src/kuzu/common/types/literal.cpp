#include "kuzu/common/types/literal.h"

#include <cassert>

namespace kuzu {
namespace common {

Literal::Literal(uint8_t* value, const DataType& dataType) : _isNull{false}, dataType{dataType} {
    switch (dataType.typeID) {
    case DataTypeID::BOOLEAN:
        val.booleanVal = *(bool*)value;
        break;
    case DataTypeID::INT64:
        val.int64Val = *(int64_t*)value;
        break;
    case DataTypeID::UBIGINT:
        val.uint64Val = *(uint64_t*)value;
        break;
    case DataTypeID::DOUBLE:
        val.doubleVal = *(double_t*)value;
        break;
    case DataTypeID::NODE_ID:
        val.nodeID = *(nodeID_t*)value;
        break;
    case DataTypeID::DATE:
        val.dateVal = *(date_t*)value;
        break;
    case DataTypeID::TIMESTAMP:
        val.timestampVal = *(timestamp_t*)value;
        break;
    case DataTypeID::INTERVAL:
        val.intervalVal = *(interval_t*)value;
        break;
    default:
        assert(false);
    }
}

Literal::Literal(const Literal& other) : dataType{other.dataType} {
    bind(other);
}

void Literal::bind(const Literal& other) {
    if (other._isNull) {
        _isNull = true;
        return;
    }
    _isNull = false;
    assert(dataType.typeID == other.dataType.typeID);
    switch (dataType.typeID) {
    case DataTypeID::BOOLEAN: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case DataTypeID::INTEGER: {
        val.int32Val = other.val.int32Val;
    } break;
    case DataTypeID::INT64: {
        val.int64Val = other.val.int64Val;
    } break;
    case DataTypeID::UINTEGER: {
        val.uint32Val = other.val.uint32Val;
    } break;
    case DataTypeID::UBIGINT: {
        val.uint64Val = other.val.uint64Val;
    } break;
    case DataTypeID::DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case DataTypeID::DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    case DataTypeID::TIMESTAMP: {
        val.timestampVal = other.val.timestampVal;
    } break;
    case DataTypeID::INTERVAL: {
        val.intervalVal = other.val.intervalVal;
    } break;
    case DataTypeID::STRING: {
        strVal = other.strVal;
    } break;
    case DataTypeID::LIST: {
        listVal = other.listVal;
    } break;
    default:
        assert(false);
    }
}

} // namespace common
} // namespace kuzu
