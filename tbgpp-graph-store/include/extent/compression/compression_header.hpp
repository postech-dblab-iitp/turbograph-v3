#pragma once

#include <cstddef>
#include <bitset>
#include "common/vector_size.hpp"

namespace duckdb {

enum CompressionFunctionType : size_t {
    UNCOMPRESSED = 0,
    BITPACKING = 1,
    RLE = 2,
    DICTIONARY = 3
};

struct CompressionHeader {
    CompressionHeader() {}
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_) : comp_type(comp_type_), data_len(data_len_), has_null(false) {
    }

    size_t GetValidSize() {
        return has_null ? sizeof(CompressionHeader) : sizeof(CompressionHeader) - sizeof(null_mask);
    }
    void SetCompFuncType(CompressionFunctionType comp_type_) {
        comp_type = comp_type_;
    }
    void SetNullMask(uint64_t* null_mask_) {
        has_null = true;
        null_mask = *null_mask_;
    }

    CompressionFunctionType comp_type;
    size_t data_len;
    bool has_null;
    std::bitset<STORAGE_STANDARD_VECTOR_SIZE> null_mask;
};



}