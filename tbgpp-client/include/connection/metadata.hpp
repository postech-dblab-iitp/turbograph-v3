#pragma once

#include <string>
#include <vector>
#include "common/types.hpp"
#include "common/boost_typedefs.hpp"

using namespace std;

typedef string LabelName;
typedef string TypeName;
typedef string PropertyName;
typedef string PropertySQLType;
typedef vector<PropertyName> PropertyNames;
typedef vector<PropertySQLType> PropertySQLTypes;
typedef vector<duckdb::LogicalType> LogicalTypes;

namespace duckdb {
    namespace MetadataUtils {
        inline void LogicalTypeIdsToSQLTypes(LogicalTypes& logical_types, PropertySQLTypes& sql_types) {
            sql_types.reserve(logical_types.size());
            for (auto& type: logical_types) {
                sql_types.push_back(type.ToString());
            }
        }

        inline void LogicalTypeIdsToSQLTypes(LogicalTypeId_vector& logical_type_ids, PropertySQLTypes& sql_types) {
            sql_types.reserve(logical_type_ids.size());
            for (auto& type_id: logical_type_ids) {
                LogicalType prop_type(type_id);
                sql_types.push_back(prop_type.ToString());
            }
        }
    }

struct NodeMetadata {
    LabelName label_name;
    PropertyNames property_names;
    PropertySQLTypes property_types;

    void SetLabelName(LabelName& label_name_) {
        label_name = label_name_;
    }

    void SetPropertyNames(string_vector& property_key_names) {
        property_names.reserve(property_key_names.size());
        for (auto& property_key_name: property_key_names) {
            property_names.push_back(property_key_name);
        }
    }

    void SetPropertySQLTypes(LogicalTypeId_vector& prop_typesids) {
        property_types.reserve(prop_typesids.size());
        for (auto& prop_typesid: prop_typesids) {
            LogicalType prop_type(prop_typesid);
            property_types.push_back(prop_type.ToString());
        }
    }
};

typedef vector<NodeMetadata> NodeMetadataList;

struct EdgeMetadata {
    TypeName type_name;
    PropertyNames property_names;
    PropertySQLTypes property_types;

    void SetTypeName(TypeName& type_name_) {
        type_name = type_name_;
    }

    void SetPropertyNames(string_vector& property_key_names) {
        property_names.reserve(property_key_names.size());
        for (auto& property_key_name: property_key_names) {
            property_names.push_back(property_key_name);
        }
    }

    void SetPropertySQLTypes(LogicalTypeId_vector& prop_typesid) {
        MetadataUtils::LogicalTypeIdsToSQLTypes(prop_typesid, property_types);
    }
};

typedef vector<EdgeMetadata> EdgeMetadataList;

struct QueryResultSetMetadata {
    PropertyNames property_names;
    PropertySQLTypes property_types;

    void SetPropertyNames(PropertyNames& stored_names) {
        property_names.reserve(stored_names.size());
        for (auto& stored_name: stored_names) {
            property_names.push_back(move(stored_name));
        }
    }

    void SetPropertySQLTypes(LogicalTypes& stored_types) {
        MetadataUtils::LogicalTypeIdsToSQLTypes(stored_types, property_types);
    }
};

}