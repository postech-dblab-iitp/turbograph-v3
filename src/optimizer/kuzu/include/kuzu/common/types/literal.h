#pragma once

#include <utility>

#include "kuzu/common/exception.h"
#include "kuzu/common/types/types_include.h"

using namespace std;

namespace kuzu {
namespace common {

class Literal {

public:
    Literal() : _isNull{true}, dataType{DataTypeID::ANY} {}

    // TODO(Guodong): initializing literal with only datatype doesn't make sense to me. Consider
    // remove this interface.
    explicit Literal(DataType dataType) : _isNull{false}, dataType{move(dataType)} {
        assert(dataType.typeID == DataTypeID::LIST);
    }

    explicit Literal(bool value) : _isNull{false}, dataType(DataTypeID::BOOLEAN) { this->val.booleanVal = value; }

    explicit Literal(uint64_t value) : _isNull{false}, dataType(DataTypeID::UBIGINT) {
        this->val.uint64Val = value;
    }
    
    explicit Literal(int64_t value) : _isNull{false}, dataType(DataTypeID::INT64) {
        this->val.int64Val = value;
    }

    explicit Literal(int32_t value) : _isNull{false}, dataType(DataTypeID::INTEGER) {
        this->val.int32Val = value;
    }

    explicit Literal(uint32_t value) : _isNull{false}, dataType(DataTypeID::UINTEGER) {
        this->val.uint32Val = value;
    }

    explicit Literal(double value) : _isNull{false}, dataType(DataTypeID::DOUBLE) {
        this->val.doubleVal = value;
    }

    explicit Literal(date_t value) : _isNull{false}, dataType(DataTypeID::DATE) { this->val.dateVal = value; }

    explicit Literal(timestamp_t value) : _isNull{false}, dataType(DataTypeID::TIMESTAMP) {
        this->val.timestampVal = value;
    }

    explicit Literal(interval_t value) : _isNull{false}, dataType(DataTypeID::INTERVAL) {
        this->val.intervalVal = value;
    }

    explicit Literal(const string& value) : _isNull{false}, dataType(DataTypeID::STRING) {
        this->strVal = value;
    }

    explicit Literal(vector<Literal> value, const DataType& dataType)
        : _isNull{false}, dataType{dataType} {
        this->listVal = move(value);
    }

    explicit Literal(uint8_t* value, const DataType& dataType);

    Literal(const Literal& other);

    void bind(const Literal& other);

    inline bool isNull() const { return _isNull; }

    template<typename T>
    static Literal createLiteral(T value) {
        throw NotImplementedException("Unimplemented template for createLiteral.");
    }

public:
    bool _isNull;

    union Val {
        bool booleanVal;
        int32_t int32Val;
        uint32_t uint32Val;
        int64_t int64Val;
        uint64_t uint64Val;
        double doubleVal;
        nodeID_t nodeID;
        date_t dateVal;
        timestamp_t timestampVal;
        interval_t intervalVal;
    } val{};

    string strVal;
    vector<Literal> listVal;

    DataType dataType;
};

template<>
inline Literal Literal::createLiteral(bool value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(int64_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(double_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(date_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(timestamp_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(interval_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(const char* value) {
    return Literal(string(value));
}

template<>
inline Literal Literal::createLiteral(string value) {
    return Literal(value);
}

} // namespace common
} // namespace kuzu
