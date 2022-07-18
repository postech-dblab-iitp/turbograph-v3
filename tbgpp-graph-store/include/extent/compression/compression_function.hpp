#pragma once

#include "common/types.hpp"
#include "common/common.hpp"
#include "extent/compression/bitpacking.hpp"

namespace duckdb {

enum CompressionFunctionType : size_t {
    UNCOMPRESSED = 0,
    BITPACKING = 1,
    RLE = 2,
    DICTIONARY = 3
};

struct CompressionHeader {
    CompressionHeader() {}
    CompressionHeader(CompressionFunctionType comp_type_, size_t data_len_) : comp_type(comp_type_), data_len(data_len_) {
    }

    CompressionFunctionType comp_type;
    size_t data_len;
};

template <typename T>
bitpacking_width_t _BitPackingCompress(data_ptr_t dst, data_ptr_t data_to_compress, size_t compression_count) {
    T *src = (T*) data_to_compress;
    bitpacking_width_t width = BitpackingPrimitives::MinimumBitWidth<T>(src, compression_count);
    BitpackingPrimitives::PackBuffer<T, false>(dst, src, compression_count, width);
    
    return width;
}

template <typename T>
void BitPackingCompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {
    fprintf(stdout, "Bitpacking Compress Start!\n");
    size_t original_size, total_size;
    original_size = data_size * sizeof(T); // For debugging
    total_size = 0;

    size_t compression_count;
    size_t remain_count = data_size;
    data_ptr_t width_ptr = buf_ptr + buf_size - sizeof(bitpacking_width_t);

    while (remain_count > 0) {
        // Compute size to compress & Compress
        compression_count = remain_count > BITPACKING_WIDTH_GROUP_SIZE ? BITPACKING_WIDTH_GROUP_SIZE : remain_count;
        bitpacking_width_t width = _BitPackingCompress<T>(buf_ptr, data_to_compress, compression_count);
        
        // Write width
        memcpy(width_ptr, &width, sizeof(bitpacking_width_t));
        
        // Adjust Size & Pointer
        remain_count -= compression_count;
        buf_ptr += (compression_count * width) / 8;
        data_to_compress += (compression_count * sizeof(T));
        width_ptr -= sizeof(bitpacking_width_t);

        total_size += (compression_count * width) / 8 + sizeof(bitpacking_width_t);
    }
    fprintf(stdout, "Bitpacking Compress Done! %ld -> %ld, compression_ratio = %.3f%%\n", original_size, total_size, ((double) total_size / original_size) * 100);
}

template <typename T>
void RLECompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {}

template <typename T>
void DictionaryCompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {}

class CompressionFunction {
typedef void (*compression_compress_data_t)(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size);

public:
    CompressionFunction() {}
    ~CompressionFunction() {}

    CompressionFunction(CompressionFunctionType func_type, PhysicalType &p_type) {
        SetCompressionFunction(func_type, p_type);
    }

    virtual void Compress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {
        compress(buf_ptr, buf_size, data_to_compress, data_size);
    }

    void SetCompressionFunction(CompressionFunctionType func_type, PhysicalType &p_type) {
        if (func_type == UNCOMPRESSED) {

        } else if (func_type == BITPACKING) {
            switch (p_type) {
            case PhysicalType::BOOL:
            case PhysicalType::INT8:
                compress = BitPackingCompress<int8_t>; break;
            case PhysicalType::INT16:
                compress = BitPackingCompress<int16_t>; break;
            case PhysicalType::INT32:
                compress = BitPackingCompress<int32_t>; break;
            case PhysicalType::INT64:
                compress = BitPackingCompress<int64_t>; break;
            case PhysicalType::UINT8:
                compress = BitPackingCompress<uint8_t>; break;
            case PhysicalType::UINT16:
                compress = BitPackingCompress<uint16_t>; break;
            case PhysicalType::UINT32:
                compress = BitPackingCompress<uint32_t>; break;
            case PhysicalType::UINT64:
                compress = BitPackingCompress<uint64_t>; break;
            default:
                throw InternalException("Unsupported type for Bitpacking");
            }
        } else if (func_type == RLE) {
        } else if (func_type == DICTIONARY) {
        } else {
            D_ASSERT(false);
        }
    }

    compression_compress_data_t compress;
};

template <typename T>
void BitPackingDecompress (data_ptr_t buf_ptr, size_t buf_size, data_ptr_t output, size_t data_size) {
    data_ptr_t current_width_group_ptr;
    data_ptr_t bitpacking_width_ptr;
    bitpacking_width_t current_width;
    idx_t position_in_group = 0;
    size_t remaining_data_to_scan = data_size;
    T *current_output_ptr = (T*) output;
    T decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE]; // Temporary buffer for the tail
    bool skip_sign_extend = std::is_signed<T>::value;// && nstats.min >= 0;

    current_width_group_ptr = buf_ptr;
    bitpacking_width_ptr = buf_ptr + buf_size - sizeof(bitpacking_width_t);
    memcpy(&current_width, bitpacking_width_ptr, sizeof(bitpacking_width_t));

    while (remaining_data_to_scan > 0) {
        if (position_in_group >= BITPACKING_WIDTH_GROUP_SIZE) {
            position_in_group = 0;
            bitpacking_width_ptr -= sizeof(bitpacking_width_t);
            current_width_group_ptr += (current_width * BITPACKING_WIDTH_GROUP_SIZE) / 8;
            memcpy(&current_width, bitpacking_width_ptr, sizeof(bitpacking_width_t));
        }

        idx_t offset_in_compression_group =
		    position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		idx_t to_scan = MinValue<idx_t>(remaining_data_to_scan, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);

		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    current_width_group_ptr + position_in_group * current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * current_width / 8;
        
        if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
            BitpackingPrimitives::UnPackBlock<T>((data_ptr_t)current_output_ptr, decompression_group_start_pointer,
                                                 current_width, skip_sign_extend);
        } else {
            BitpackingPrimitives::UnPackBlock<T>((data_ptr_t)decompression_buffer, decompression_group_start_pointer,
                                                 current_width, skip_sign_extend);
            memcpy(current_output_ptr, decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
        }

        remaining_data_to_scan -= to_scan;
        position_in_group += to_scan;
        current_output_ptr += to_scan;
    }
}

class DeCompressionFunction {
typedef void (*compression_decompress_data_t)(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t output, size_t data_size);

public:
    DeCompressionFunction() {}
    ~DeCompressionFunction() {}

    DeCompressionFunction(CompressionFunctionType func_type, PhysicalType &p_type) {
        SetDeCompressionFunction(func_type, p_type);
    }

    virtual void DeCompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t output, size_t data_size) {
        decompress(buf_ptr, buf_size, output, data_size);
    }

    void SetDeCompressionFunction(CompressionFunctionType func_type, PhysicalType &p_type) {
        if (func_type == UNCOMPRESSED) {

        } else if (func_type == BITPACKING) {
            switch (p_type) {
            case PhysicalType::BOOL:
            case PhysicalType::INT8:
                decompress = BitPackingDecompress<int8_t>; break;
            case PhysicalType::INT16:
                decompress = BitPackingDecompress<int16_t>; break;
            case PhysicalType::INT32:
                decompress = BitPackingDecompress<int32_t>; break;
            case PhysicalType::INT64:
                decompress = BitPackingDecompress<int64_t>; break;
            case PhysicalType::UINT8:
                decompress = BitPackingDecompress<uint8_t>; break;
            case PhysicalType::UINT16:
                decompress = BitPackingDecompress<uint16_t>; break;
            case PhysicalType::UINT32:
                decompress = BitPackingDecompress<uint32_t>; break;
            case PhysicalType::UINT64:
                decompress = BitPackingDecompress<uint64_t>; break;
            default:
                throw InternalException("Unsupported type for Bitpacking");
            }
        } else if (func_type == RLE) {
        } else if (func_type == DICTIONARY) {
        } else {
            D_ASSERT(false);
        }
    }

    compression_decompress_data_t decompress;
};

}