#include "common/types/rowcol_type.hpp"
#include "common/typedef.hpp"

namespace duckdb {

bool rowcol_t::HasCol(int col_idx) {
    return ((PartialSchema *)schema_ptr)->hasIthCol(col_idx);
}

bool rowcol_t::GetColOffset(int col_idx, idx_t &final_offset) {
    if (((PartialSchema *)schema_ptr)->hasIthCol(col_idx)) {
        final_offset = ((PartialSchema *)schema_ptr)->getIthColOffset(col_idx) + offset;
        return true;
    } else {
        final_offset = 0;
        return false;
    }
}

}