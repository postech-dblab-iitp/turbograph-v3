#pragma once

#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>
#include <memory>
#include "common/types/data_chunk.hpp"

namespace duckdb {

struct LogicalType;

enum class StoreAPIResult { OK, DONE, ERROR };

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
    void appendStoredTypes(std::vector<duckdb::LogicalType> types);
    std::vector<duckdb::LogicalType> getStoredTypes();
    std::vector<duckdb::LogicalType> &getStoredTypesRef();
    void setStoredColumnNames(std::vector<std::string> &names);
    std::vector<std::string> getStoredColumnNames();
    void removeColumn(uint64_t col_idx);

    std::string printStoredTypes();
    std::string printStoredColumnAndTypes();

    //! get the size of stored types
    uint64_t getStoredTypesSize() { return stored_types_size; }

   public:
    std::vector<duckdb::LogicalType> stored_types;
    std::vector<std::string> stored_column_names;
    uint64_t stored_types_size;
};

// TODO need to be improved & renaming
struct PartialSchema {
   public:
    PartialSchema() {}
    bool hasIthCol(uint64_t col_idx) { return offset_info[col_idx] >= 0; }
    uint64_t getIthColOffset(uint64_t col_idx) { return offset_info[col_idx]; }
    uint64_t getStoredTypesSize() { return stored_types_size; }

   public:
    std::vector<int32_t> offset_info;
    uint64_t stored_types_size;
};

/** Scan Related */
#define FILTERED_CHUNK_BUFFER_SIZE 2

enum class FilterPushdownType: uint8_t {
	FP_EQ,
	FP_RANGE,
	FP_COMPLEX
};

struct RangeFilterValue {
	Value l_value;
	Value r_value;
	bool l_inclusive;
	bool r_inclusive;
};

class FilteredChunkBuffer {
public:
    FilteredChunkBuffer(): buffer_idx(0) {
        slice_buffer = nullptr;
        for (auto i = 0; i < FILTERED_CHUNK_BUFFER_SIZE; i++) {
            buffer_chunks.push_back(nullptr);
        }
    }

    bool Append(DataChunk &chunk) {
    }

    void Initialize(vector<LogicalType> types);

    unique_ptr<DataChunk> &GetSliceBuffer() {
        return slice_buffer;
    }

    unique_ptr<DataChunk> &GetFilteredChunk() {
        return buffer_chunks[buffer_idx];
    }

    unique_ptr<DataChunk> &GetFilteredChunk(uint64_t idx) {
        idx = idx % FILTERED_CHUNK_BUFFER_SIZE;
        return buffer_chunks[idx];
    }

private:
    std::unique_ptr<DataChunk> slice_buffer;
	std::vector<std::unique_ptr<DataChunk>> buffer_chunks;
	idx_t buffer_idx;
};

typedef vector<int64_t> FilterKeyIdxs;
typedef vector<Value> EQFilterValues;
typedef vector<RangeFilterValue> RangeFilterValues;

}  // namespace duckdb
