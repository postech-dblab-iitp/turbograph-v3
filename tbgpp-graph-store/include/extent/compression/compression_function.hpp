#pragma once

#include "common/types.hpp"
#include "common/common.hpp"
#include "extent/compression/bitpacking.hpp"

namespace duckdb {

enum CompressionFunctionType {
    BITPACKING = 0
};

class CompressionFunction {
public:
    CompressionFunction() {}
    ~CompressionFunction() {}

    virtual void Compress(data_ptr_t dst, data_ptr_t data_to_compress, size_t compression_size) {}
};

template <typename T>
class BitpackingCompressionFunction : public CompressionFunction {
public:
	BitpackingCompressionFunction() {}
	~BitpackingCompressionFunction() {}

    BitpackingCompressionFunction(PhysicalType p_type) {}
    
    virtual void Compress(data_ptr_t dst, data_ptr_t data_to_compress, size_t compression_size) {
        T *src = (T*) data_to_compress;
        bitpacking_width_t width = BitpackingPrimitives::MinimumBitWidth<T>(src, compression_size);
        BitpackingPrimitives::PackBuffer<T, false>(dst, src, compression_size, width); 
        //<class T, bool ASSUME_INPUT_ALIGNED = false> (data_ptr_t dst, T *src, idx_t count, bitpacking_width_t width)
    }
};

static CompressionFunction GetCompressionFunction(CompressionFunctionType func_type, PhysicalType p_type) {
    if (func_type == BITPACKING) {
        switch (p_type) {
        case PhysicalType::BOOL:
        case PhysicalType::INT8:
            return BitpackingCompressionFunction<int8_t>(p_type);
        case PhysicalType::INT16:
            return BitpackingCompressionFunction<int16_t>(p_type);
        case PhysicalType::INT32:
            return BitpackingCompressionFunction<int32_t>(p_type);
        case PhysicalType::INT64:
            return BitpackingCompressionFunction<int64_t>(p_type);
        case PhysicalType::UINT8:
            return BitpackingCompressionFunction<uint8_t>(p_type);
        case PhysicalType::UINT16:
            return BitpackingCompressionFunction<uint16_t>(p_type);
        case PhysicalType::UINT32:
            return BitpackingCompressionFunction<uint32_t>(p_type);
        case PhysicalType::UINT64:
            return BitpackingCompressionFunction<uint64_t>(p_type);
        default:
            throw InternalException("Unsupported type for Bitpacking");
        }
    } else {
        D_ASSERT(false);
    }
}

}