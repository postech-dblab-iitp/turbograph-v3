#include "common/typedef.hpp"
#include <algorithm>
#include <iostream>
#include <map>

#include "common/types.hpp"

#include "icecream.hpp"

namespace duckdb {
void LabelSet::insert(std::string input)
{
    this->data.insert(input);
}

//! when a is superset of b, a contains all elems of b, which leads to smaller intersection in label hierarchy
bool LabelSet::isSupersetOf(const LabelSet &elem)
{

    // if size bigger, always false
    if (elem.data.size() > this->data.size())
        return false;
    // if same or small, check if all members exist.
    for (const auto &item : elem.data) {
        if (this->data.find(item) == this->data.end()) {
            return false;
        }
    }
    return true;
}

bool LabelSet::contains(const std::string st)
{
    return this->data.find(st) != this->data.end();
}

std::ostream &operator<<(std::ostream &os, const LabelSet &obj)
{
    os << "LabelSet(";
    for (auto item : obj.data) {
        os << item << ",";
    }
    os << ")";
    return os;
}

bool operator==(const LabelSet lhs, const LabelSet rhs)
{
    // This is a comparison between 'unordered' sets, thus different ordering will return true.
    return lhs.data == rhs.data;
}

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

void FilteredChunkBuffer::Initialize(vector<LogicalType> types)
{
    slice_buffer = make_unique<DataChunk>();
    slice_buffer->Initialize(types, STANDARD_VECTOR_SIZE);
    for (auto i = 0; i < FILTERED_CHUNK_BUFFER_SIZE; i++) {
        auto buffer_chunk = std::make_unique<DataChunk>();
        buffer_chunk->Initialize(types, STANDARD_VECTOR_SIZE);
        buffer_chunks[i] = std::move(buffer_chunk);
    }
    buffer_idx = 0;
}

}  // namespace duckdb