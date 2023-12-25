#pragma once

#include <math.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace kuzu {
namespace common {

typedef uint16_t sel_t;
typedef uint64_t hash_t;
typedef uint32_t page_idx_t;
constexpr page_idx_t PAGE_IDX_MAX = UINT32_MAX;
typedef uint32_t list_header_t;

typedef uint32_t property_id_t;
constexpr property_id_t INVALID_PROPERTY_ID = UINT32_MAX;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements = 0;
    uint8_t* value = nullptr;
};

enum DataTypeID : uint8_t {
    // NOTE: Not all data types can be used in processor. For example, ANY should be resolved during
    // query compilation. Similarly logical data types should also only be used in compilation.
    // Some use cases are as follows.
    //    - differentiate whether is a variable refers to node table or rel table
    //    - bind (static evaluate) functions work on node/rel table.
    //      E.g. ID(a "datatype:NODE") -> node ID property "datatype:NODE_ID"

    // S62 ID changed to match that of Duckdb::LogicalTypeId
    // refer to /turbograph-v3/tbgpp-common/include/common/types.hpp

    // logical  types
    ANY = 3,
    NODE = 201, // NON-existent in DuckDB
    REL = 202,  // NON-existent in DuckDB
    PATH = 203, // NON-existent in DuckDB

    // physical fixed size types
    NODE_ID = 108,
    BOOL = 10,
    INTEGER = 13,
    INT64 = 14,     // BIGINT
    UBIGINT = 31,
    DOUBLE = 23,
    DATE = 15,
    TIMESTAMP = 19,
    INTERVAL = 27,
    STRING = 25,
    UINTEGER = 30,
    LIST = 101,

    // not existent in kuzu, but existent in duckdb
    INVALID = 0,
	SQLNULL = 1, /* NULL type, used for constant NULL */
	UNKNOWN = 2, /* unknown type, used for parameter expressions */
//	ANY = 3,     /* ANY type, used for functions that accept any type as parameter */
	USER = 4, /* A User Defined Type (e.g., ENUMs before the binder) */
//	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
//	BIGINT = 14,
//	DATE = 15,
	TIME = 16,
	TIMESTAMP_SEC = 17,
	TIMESTAMP_MS = 18,
//	TIMESTAMP = 19, //! us
	TIMESTAMP_NS = 20,
	DECIMAL = 21,
	FLOAT = 22,
//	DOUBLE = 23,
	CHAR = 24,
//	VARCHAR = 25,
	BLOB = 26,
//	INTERVAL = 27,
	UTINYINT = 28,
	USMALLINT = 29,
	TIMESTAMP_TZ = 32,
	TIME_TZ = 34,
	JSON = 35,
	HUGEINT = 50,
	POINTER = 51,
	HASH = 52,
	VALIDITY = 53,
	UUID = 54,
	STRUCT = 100,
//	LIST = 101,
	MAP = 102,
	TABLE = 103,
	ENUM = 104,
	AGGREGATE_STATE = 105,
	// TBGPP-specific
	FORWARD_ADJLIST = 106,
	BACKWARD_ADJLIST = 107,
//  ID = 108,
	ADJLISTCOLUMN = 109
};

class DataType {
public:
    DataType() : typeID{ANY}, childType{nullptr} {}
    explicit DataType(DataTypeID typeID) : typeID{typeID}, childType{nullptr} {
        assert(typeID != LIST);
    }
    DataType(DataTypeID typeID, std::unique_ptr<DataType> childType)
        : typeID{typeID}, childType{std::move(childType)} {
        assert(typeID == LIST);
    }

    DataType(const DataType& other);
    DataType(DataType&& other) noexcept
        : typeID{other.typeID}, childType{std::move(other.childType)} {}

    static inline std::vector<DataTypeID> getNumericalTypeIDs() {
        // S62 extended
        // return std::vector<DataTypeID>{
        //     INT64, DOUBLE, TINYINT, SMALLINT, INTEGER, FLOAT, UTINYINT, USMALLINT, UINTEGER, UBIGINT// added in duckdb
        //     };
        // currently supported integer types
        return std::vector<DataTypeID>{
            INT64, DOUBLE, INTEGER, FLOAT, UINTEGER, UBIGINT, DECIMAL// added in duckdb
            };
    }
    static inline std::vector<DataTypeID> getAllValidTypeIDs() {
        // S62 extended
        return std::vector<DataTypeID>{
            NODE_ID, BOOL, INT64, DOUBLE, STRING, DATE, TIMESTAMP, INTERVAL, LIST,
            SQLNULL, USER, TINYINT, SMALLINT, INTEGER, TIME, TIMESTAMP_SEC, TIMESTAMP_MS, TIMESTAMP_NS,
            DECIMAL, FLOAT, CHAR, BLOB, UTINYINT, USMALLINT, UINTEGER, UBIGINT, TIMESTAMP_TZ, TIME_TZ, JSON, HUGEINT, POINTER, HASH, UUID, STRUCT, MAP, TABLE, ENUM, FORWARD_ADJLIST, BACKWARD_ADJLIST, ADJLISTCOLUMN  
            };
    }

    DataType& operator=(const DataType& other);

    bool operator==(const DataType& other) const;

    bool operator!=(const DataType& other) const { return !((*this) == other); }

    inline DataType& operator=(DataType&& other) noexcept {
        typeID = other.typeID;
        childType = std::move(other.childType);
        return *this;
    }

public:
    DataTypeID typeID;
    std::unique_ptr<DataType> childType;

private:
    std::unique_ptr<DataType> copy();
};

class Types {
public:
    static std::string dataTypeToString(const DataType& dataType);
    static std::string dataTypeToString(DataTypeID dataTypeID);
    static std::string dataTypesToString(const std::vector<DataType>& dataTypes);
    static std::string dataTypesToString(const std::vector<DataTypeID>& dataTypeIDs);
    static DataType dataTypeFromString(const std::string& dataTypeString);
    static uint32_t getDataTypeSize(DataTypeID dataTypeID);
    static inline uint32_t getDataTypeSize(const DataType& dataType) {
        return getDataTypeSize(dataType.typeID);
    }

private:
    static DataTypeID dataTypeIDFromString(const std::string& dataTypeIDString);
};

// RelDirection
enum RelDirection : uint8_t { FWD = 0, BWD = 1 };
const std::vector<RelDirection> REL_DIRECTIONS = {FWD, BWD};
RelDirection operator!(RelDirection& direction);
std::string getRelDirectionAsString(RelDirection relDirection);

enum class DBFileType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace kuzu
