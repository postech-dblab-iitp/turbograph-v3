#include <algorithm>
#include <iostream>
#include <map>

#include "common/types/schema.hpp"

namespace duckdb {

void Schema::setStoredTypes(std::vector<duckdb::LogicalType> types)
{
    stored_types_size = 0;
    for (auto &t : types) {
        stored_types.push_back(t);
        stored_types_size += GetTypeIdSize(t.InternalType());
    }
}

void Schema::appendStoredTypes(std::vector<duckdb::LogicalType> types)
{
    for (auto &t : types) {
        stored_types.push_back(t);
    }
}

void Schema::setStoredColumnNames(std::vector<std::string> &names)
{
    for (auto &t : names) {
        stored_column_names.push_back(t);
    }
}

std::vector<duckdb::LogicalType> Schema::getStoredTypes()
{
    return stored_types;
}

std::vector<duckdb::LogicalType> &Schema::getStoredTypesRef()
{
    return stored_types;
}

std::vector<string> Schema::getStoredColumnNames()
{
    return stored_column_names;
}

std::string Schema::printStoredTypes()
{
    std::string result = "(";
    for (auto i = 0; i < stored_types.size(); i++) {
        if (i != 0)
            result += ", ";
        result += std::to_string(i) + ":";
        result += stored_types[i].ToString();
    }
    result += ")";
    return result;
}

std::string Schema::printStoredColumnAndTypes()
{
    if (stored_types.size() != stored_column_names.size()) {
        return printStoredTypes();
    }
    std::string result = "(";
    for (int idx = 0; idx < stored_types.size(); idx++) {
        if (idx != 0)
            result += ", ";
        result += stored_column_names[idx];
        result += ":";
        result += stored_types[idx].ToString();
    }
    result += ")";
    return result;
}


void Schema::removeColumn(uint64_t col_idx) 
{
    if (stored_types.size() > col_idx) {
        stored_types.erase(stored_types.begin() + col_idx);
    }
    if (stored_column_names.size() > col_idx) {
        stored_column_names.erase(stored_column_names.begin() + col_idx);
    }
    stored_types_size = 0;
    for (auto &t : stored_types) {
        stored_types_size += GetTypeIdSize(t.InternalType());
    }
}

}
