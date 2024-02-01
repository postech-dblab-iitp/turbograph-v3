#pragma once

#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

namespace duckdb {

struct LogicalType;

enum class StoreAPIResult { OK, DONE, ERROR };

typedef long VertexID;
typedef long EdgeID;

typedef std::vector<std::string> PropertyKeys;

enum class LoadAdjListOption { NONE, OUTGOING, INCOMING, BOTH };

enum class ExpandDirection { OUTGOING, INCOMING, BOTH };

class LabelSet {

   public:
    LabelSet() {}
    LabelSet(std::string e1) { this->insert(e1); }
    LabelSet(std::string e1, std::string e2)
    {
        this->insert(e1);
        this->insert(e1);
    }
    LabelSet(std::string e1, std::string e2, std::string e3)
    {
        this->insert(e1);
        this->insert(e2);
        this->insert(e3);
    }
    void insert(std::string input);
    // TODO support multiple insert at once by supporting variadic template

    bool isSupersetOf(const LabelSet &elem);
    bool contains(const std::string st);
    size_t size() { return this->data.size(); }

    friend bool operator==(const LabelSet lhs, const LabelSet rhs);
    friend std::ostream &operator<<(std::ostream &os, const LabelSet &obj);

   public:
    std::unordered_set<std::string> data;
};

enum class CypherValueType {
    // non-nested
    DATA,  // TODO need to be specified more
    ID,
    RANGE,
    // nested
    NODE,
    EDGE,
    PATH
};

class Schema {
   public:
    void setStoredTypes(std::vector<duckdb::LogicalType> types);
    std::vector<duckdb::LogicalType> getStoredTypes();
    void setStoredColumnNames(std::vector<std::string> &names);
    std::vector<std::string> getStoredColumnNames();
    std::string printStoredTypes();
    std::string printStoredColumnAndTypes();

	//! get the size of stored types
	uint64_t getStoredTypesSize() { return stored_types_size; }

   public:
    std::vector<duckdb::LogicalType> stored_types;
    std::vector<std::string> stored_column_names;
	uint64_t stored_types_size;
};
}  // namespace duckdb