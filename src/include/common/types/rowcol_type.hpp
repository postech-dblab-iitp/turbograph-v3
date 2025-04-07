#pragma once

#include "common/assert.hpp"
#include "common/constants.hpp"

namespace duckdb {

struct rowcol_t {
    rowcol_t(idx_t offset_val, const char *schema_info) {
        offset = offset_val;
        schema_ptr = (char *)schema_info;
    }

    bool HasCol(int col_idx);
    bool GetColOffset(int col_idx, idx_t &final_offset);

public:
    idx_t offset = 0; // base offset
    char *schema_ptr = nullptr;
};

} // namespace duckdb