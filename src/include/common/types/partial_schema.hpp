#pragma once

#include <vector>
#include "common/types.hpp"

namespace duckdb {

// TODO need to be improved & renaming
struct PartialSchema {
   public:
    PartialSchema() {}
    bool hasIthCol(uint64_t col_idx) { 
        if (col_idx >= offset_info.size()) {
            return false;
        }
        return offset_info[col_idx] >= 0; 
    }
    uint64_t getIthColOffset(uint64_t col_idx) { 
        if (col_idx >= offset_info.size()) {
            return 0;
        }
        return offset_info[col_idx]; 
    }
    uint64_t getStoredTypesSize() { return stored_types_size; }

   public:
    std::vector<int32_t> offset_info;
    uint64_t stored_types_size;
};

static const PartialSchema EMPTY_PARTIAL_SCHEMA;

}