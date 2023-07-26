#pragma once

#include <cstddef>

namespace duckdb {

enum CompressionFunctionType : size_t {
    UNCOMPRESSED = 0,
    BITPACKING = 1,
    RLE = 2,
    DICTIONARY = 3
};

enum SwizzleState : size_t {
    NO_SWIZZLING = 0,
    SWIZZLED = 1,
    UNSWIZZLED = 2
};

struct CompressionHeader {
    CompressionHeader() {}
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_) : comp_type(comp_type_), data_len(data_len_), swizzle_state(NO_SWIZZLING) {
    }
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_, SwizzleState swizzle_state_) : comp_type(comp_type_), data_len(data_len_), swizzle_state(swizzle_state_) {
    }

    CompressionFunctionType comp_type;
    size_t data_len;
    SwizzleState swizzle_state;
};

}