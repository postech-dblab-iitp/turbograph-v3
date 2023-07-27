#pragma once

#include <cstddef>

namespace duckdb {

enum CompressionFunctionType : size_t {
    UNCOMPRESSED = 0,
    BITPACKING = 1,
    RLE = 2,
    DICTIONARY = 3
};

typedef bool SwizzlingFlag;

struct CompressionHeader {
    CompressionHeader() {}
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_) : comp_type(comp_type_), data_len(data_len_), swizzling_needed(false) {
    }
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_, bool swizzling_needed_) : comp_type(comp_type_), data_len(data_len_), swizzling_needed(swizzling_needed_) {
    }

    CompressionFunctionType comp_type;
    size_t data_len;
    SwizzlingFlag swizzling_needed;
};

}