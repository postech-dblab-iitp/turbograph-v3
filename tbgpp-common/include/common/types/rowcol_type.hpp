#pragma once

#include "common/assert.hpp"
#include "common/constants.hpp"

namespace duckdb {

struct rowcol_t {
    rowcol_t(idx_t offset_val, const char *schema_info) {
        offset = offset_val;
        schema_ptr = (char *)schema_info;
    }

public:
    idx_t offset;
    char *schema_ptr;
};

} // namespace duckdb