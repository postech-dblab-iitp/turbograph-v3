#include "kuzu/common/types/types.h"

#include <stdexcept>

#include "kuzu/common/exception.h"
#include "kuzu/common/types/types_include.h"

namespace kuzu {
namespace common {

DataType::DataType(const DataType& other) : typeID{other.typeID} {
    if (other.childType) {
        childType = other.childType->copy();
    }
}

DataType& DataType::operator=(const DataType& other) {
    typeID = other.typeID;
    if (other.childType) {
        childType = other.childType->copy();
    }
    return *this;
}

bool DataType::operator==(const DataType& other) const {
    if (typeID != other.typeID) {
        return false;
    }
    if (typeID == LIST && *childType != *other.childType) {
        return false;
    }
    return true;
}

unique_ptr<DataType> DataType::copy() {
    if (childType) {
        return make_unique<DataType>(typeID, childType->copy());
    } else {
        return make_unique<DataType>(typeID);
    }
}

DataType Types::dataTypeFromString(const string& dataTypeString) {
    DataType dataType;
    /* JHKO CPP20 ends_with replaced with different expression*/
    if (dataTypeString.substr( dataTypeString.length() - 2 ) == "[]") {
        dataType.typeID = LIST;
        dataType.childType = make_unique<DataType>(
            dataTypeFromString(dataTypeString.substr(0, dataTypeString.size() - 2)));
        return dataType;
    } else {
        dataType.typeID = dataTypeIDFromString(dataTypeString);
    }
    return dataType;
}

DataTypeID Types::dataTypeIDFromString(const std::string& dataTypeIDString) {
    if ("NODE_ID" == dataTypeIDString) {
        return DataTypeID::NODE_ID;
    } else if ("INT64" == dataTypeIDString) {
        return DataTypeID::INT64;
    } else if ("UBIGINT" == dataTypeIDString) {
        return DataTypeID::UBIGINT;
    } else if ("DOUBLE" == dataTypeIDString) {
        return DataTypeID::DOUBLE;
    } else if ("BOOL" == dataTypeIDString) {
        return DataTypeID::BOOLEAN;
    } else if ("BOOLEAN" == dataTypeIDString) {
        return DataTypeID::BOOLEAN;
    } else if ("STRING" == dataTypeIDString) {
        return DataTypeID::STRING;
    } else if ("DATE" == dataTypeIDString) {
        return DataTypeID::DATE;
    } else if ("TIMESTAMP" == dataTypeIDString) {
        return DataTypeID::TIMESTAMP;
    } else if ("INTERVAL" == dataTypeIDString) {
        return DataTypeID::INTERVAL;
    } else {
        throw Exception("Cannot parse dataTypeID: " + dataTypeIDString);
    }
}

string Types::dataTypeToString(const DataType& dataType) {
    if (dataType.typeID == LIST) {
        assert(dataType.childType);
        auto result = dataTypeToString(*dataType.childType) + "[]";
        return result;
    } else {
        return dataTypeToString(dataType.typeID);
    }
}

string Types::dataTypeToString(DataTypeID dataTypeID) {
    switch (dataTypeID) {
    case DataTypeID::ANY:
        return "ANY";
    case DataTypeID::NODE:
        return "NODE";
    case DataTypeID::REL:
        return "REL";
    case DataTypeID::NODE_ID:
        return "NODE_ID";
    case DataTypeID::BOOLEAN:
        return "BOOL";
    case DataTypeID::INT64:
        return "INT64";
    case DataTypeID::UBIGINT:
        return "UBIGINT";
    case DataTypeID::DOUBLE:
        return "DOUBLE";
    case DataTypeID::DATE:
        return "DATE";
    case DataTypeID::TIMESTAMP:
        return "TIMESTAMP";
    case DataTypeID::INTERVAL:
        return "INTERVAL";
    case DataTypeID::STRING:
        return "STRING";
    case DataTypeID::LIST:
        return "LIST";
    default:
        return "DataTypeID:" + std::to_string(dataTypeID);
    }
}

string Types::dataTypesToString(const vector<DataType>& dataTypes) {
    vector<DataTypeID> dataTypeIDs;
    for (auto& dataType : dataTypes) {
        dataTypeIDs.push_back(dataType.typeID);
    }
    return dataTypesToString(dataTypeIDs);
}

string Types::dataTypesToString(const vector<DataTypeID>& dataTypeIDs) {
    if (dataTypeIDs.empty()) {
        return string("");
    }
    string result = "(" + Types::dataTypeToString(dataTypeIDs[0]);
    for (auto i = 1u; i < dataTypeIDs.size(); ++i) {
        result += "," + Types::dataTypeToString(dataTypeIDs[i]);
    }
    result += ")";
    return result;
}

uint32_t Types::getDataTypeSize(DataTypeID dataTypeID) {
    switch (dataTypeID) {
    case DataTypeID::NODE_ID:
        return sizeof(nodeID_t);
    case DataTypeID::BOOLEAN:
        return sizeof(uint8_t);
    case DataTypeID::INT64:
        return sizeof(int64_t);
    case DataTypeID::UBIGINT:
        return sizeof(uint64_t);
    case DataTypeID::DOUBLE:
        return sizeof(double_t);
    case DataTypeID::DATE:
        return sizeof(date_t);
    case DataTypeID::TIMESTAMP:
        return sizeof(timestamp_t);
    case DataTypeID::INTERVAL:
        return sizeof(interval_t);
    case DataTypeID::STRING:
        return sizeof(ku_string_t);
    case DataTypeID::LIST:
        return sizeof(ku_list_t);
    default:
        throw Exception(
            "Cannot infer the size of dataTypeID: " + dataTypeToString(dataTypeID) + ".");
    }
}

RelDirection operator!(RelDirection& direction) {
    return (FWD == direction) ? BWD : FWD;
}

string getRelDirectionAsString(RelDirection direction) {
    return (FWD == direction) ? "forward" : "backward";
}

} // namespace common
} // namespace kuzu
