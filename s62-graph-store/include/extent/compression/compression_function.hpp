#ifndef COMPRESSION_FUNCTION_HPP
#define COMPRESSION_FUNCTION_HPP

#include "common/types.hpp"
#include "common/common.hpp"
#include "common/unordered_map.hpp"
#include "common/types/hash.hpp"
#include "common/types/null_value.hpp"
#include "common/operator/comparison_operators.hpp"
#include "extent/compression/bitpacking.hpp"
#include "extent/compression/compression_header.hpp"

namespace s62 {

// BitPacking
template <typename T>
bitpacking_width_t _BitPackingCompress(data_ptr_t dst, data_ptr_t data_to_compress, size_t compression_count) {
    T *src = (T*) data_to_compress;
    bitpacking_width_t width = BitpackingPrimitives::MinimumBitWidth<T>(src, compression_count);
    BitpackingPrimitives::PackBuffer<T, false>(dst, src, compression_count, width);
    
    return width;
}

template <typename T>
void BitPackingCompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {
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
        fprintf(stdout, "Bitpacking Compress current_width = %d at %p\n", width, width_ptr);
        
        // Adjust Size & Pointer
        remain_count -= compression_count;
        buf_ptr += (compression_count * width) / 8;
        data_to_compress += (compression_count * sizeof(T));
        width_ptr -= sizeof(bitpacking_width_t);

        total_size += (compression_count * width) / 8 + sizeof(bitpacking_width_t);
    }
    fprintf(stdout, "Bitpacking Compress Done! %ld -> %ld, compression_ratio = %.3f%%\n", original_size, total_size, ((double) total_size / original_size) * 100);
}

// RLE
using rle_count_t = uint16_t;
struct RLEConstants {
	static constexpr const idx_t RLE_HEADER_SIZE = sizeof(uint64_t);
};

template <typename T>
void RLECompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {
    idx_t seen_count = 0;
    idx_t entry_count = 0;
	T last_value = NullValue<T>();
	rle_count_t last_seen_count = 0;
	//void *dataptr = nullptr;
	bool all_null = true;

    auto data = (T *)data_to_compress;
    auto data_pointer = (T *)buf_ptr;
    auto index_pointer = (rle_count_t *)(buf_ptr + data_size * sizeof(T));
    D_ASSERT((buf_ptr + data_size * sizeof(T)) == (buf_ptr + buf_size - data_size * sizeof(rle_count_t)));

    // Iterate all data
    for (idx_t i = 0; i < data_size; i++) {
        all_null = false;
        if (seen_count == 0) {
            // no value seen yet
            // assign the current value, and set the seen_count to 1
            // note that we increment last_seen_count rather than setting it to 1
            // this is intentional: this is the first VALID value we see
            // but it might not be the first value in case of nulls!
            last_value = data[i];
            seen_count = 1;
            last_seen_count++;
        } else if (last_value == data[i]) {
            // the last value is identical to this value: increment the last_seen_count
            last_seen_count++;
        } else {
            // the values are different
            // issue the callback on the last value
            // Flush
            data_pointer[entry_count] = last_value;
            index_pointer[entry_count] = last_seen_count;
            entry_count++;
            D_ASSERT(entry_count <= data_size);

            // increment the seen_count and put the new value into the RLE slot
            last_value = data[i];
            seen_count++;
            last_seen_count = 1;
        }
        if (last_seen_count == NumericLimits<rle_count_t>::Maximum()) {
            // we have seen the same value so many times in a row we are at the limit of what fits in our count
            // write away the value and move to the next value
            // Flush
            data_pointer[entry_count] = last_value;
            index_pointer[entry_count] = last_seen_count;
            entry_count++;
            D_ASSERT(entry_count <= data_size);

            last_seen_count = 0;
            seen_count++;
        }
    }
}

// Dictionary
struct StringHash {
	std::size_t operator()(const string_t &k) const {
		return Hash(k.GetDataUnsafe(), k.GetSize());
	}
};

struct StringCompare {
	bool operator()(const string_t &lhs, const string_t &rhs) const {
		return StringComparisonOperators::EqualsOrNot<false>(lhs, rhs);
	}
};

bool LookupString(string_t str, std::unordered_map<string_t, uint32_t, StringHash, StringCompare> &current_string_map, uint32_t &latest_lookup_result);

bool HasEnoughSpace(bool new_string, size_t string_size, std::vector<uint32_t> &index_buffer, bitpacking_width_t &next_width);

void AddNewString(string_t &str, data_ptr_t &string_data_pointer, idx_t &string_data_pos, std::vector<uint32_t> &index_buffer,
                  std::unordered_map<string_t, uint32_t, StringHash, StringCompare> &current_string_map);
void Verify();

void DictionaryCompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size);

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
            switch (p_type) {
            case PhysicalType::BOOL:
            case PhysicalType::INT8:
                compress = RLECompress<int8_t>; break;
            case PhysicalType::INT16:
                compress = RLECompress<int16_t>; break;
            case PhysicalType::INT32:
                compress = RLECompress<int32_t>; break;
            case PhysicalType::INT64:
                compress = RLECompress<int64_t>; break;
            case PhysicalType::INT128:
                compress = RLECompress<hugeint_t>; break;
            case PhysicalType::UINT8:
                compress = RLECompress<uint8_t>; break;
            case PhysicalType::UINT16:
                compress = RLECompress<uint16_t>; break;
            case PhysicalType::UINT32:
                compress = RLECompress<uint32_t>; break;
            case PhysicalType::UINT64:
                compress = RLECompress<uint64_t>; break;
            case PhysicalType::FLOAT:
                compress = RLECompress<float>; break;
            case PhysicalType::DOUBLE:
                compress = RLECompress<double>; break;
            default:
                throw InternalException("Unsupported type for RLE");
            }
        } else if (func_type == DICTIONARY) {
            switch (p_type) {
            case PhysicalType::VARCHAR:
                compress = DictionaryCompress; break;
            default:
                throw InternalException("Unsupported type for Dictionary");
            }
        } else {
            D_ASSERT(false);
        }
    }

    compression_compress_data_t compress;
};

template <typename T>
void BitPackingDecompress (data_ptr_t buf_ptr, size_t buf_size, Vector &output, size_t data_size) {
    data_ptr_t current_width_group_ptr;
    data_ptr_t bitpacking_width_ptr;
    bitpacking_width_t current_width;
    idx_t position_in_group = 0;
    size_t remaining_data_to_scan = data_size;
    data_ptr_t output_pointer = output.GetData();
    T *current_output_ptr = (T*) output_pointer;
    T decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE]; // Temporary buffer for the tail
    bool skip_sign_extend = std::is_signed<T>::value;// && nstats.min >= 0;

    current_width_group_ptr = buf_ptr;
    bitpacking_width_ptr = buf_ptr + buf_size - sizeof(bitpacking_width_t);
    memcpy(&current_width, bitpacking_width_ptr, sizeof(bitpacking_width_t));
    fprintf(stdout, "Bitpacking Decompress current_width = %d at %p\n", current_width, bitpacking_width_ptr);

    while (remaining_data_to_scan > 0) {
        if (position_in_group >= BITPACKING_WIDTH_GROUP_SIZE) {
            position_in_group = 0;
            bitpacking_width_ptr -= sizeof(bitpacking_width_t);
            current_width_group_ptr += (current_width * BITPACKING_WIDTH_GROUP_SIZE) / 8;
            memcpy(&current_width, bitpacking_width_ptr, sizeof(bitpacking_width_t));
            fprintf(stdout, "Bitpacking Decompress current_width = %d at %p\n", current_width, bitpacking_width_ptr);
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
        // fprintf(stdout, "to_scan %ld, offset_in_compression_group %ld, remaining_data_to_scan %ld, current_position_ptr %p, decompression_group_start_pointer %p, current_width %d, position_in_group %ld\n",
        //                 to_scan, offset_in_compression_group, remaining_data_to_scan, current_position_ptr, decompression_group_start_pointer, current_width, position_in_group);
        
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

template <typename T>
void RLEDecompress (data_ptr_t buf_ptr, size_t buf_size, Vector &output, size_t data_size) {
    auto output_pointer = output.GetData();
    auto data_pointer = (T *)(buf_ptr);// + RLEConstants::RLE_HEADER_SIZE);
	auto index_pointer = (rle_count_t *)(buf_ptr + data_size * sizeof(T));

	auto result_data = (T *)output_pointer;

    idx_t entry_pos = 0;
	idx_t position_in_entry = 0;
    idx_t result_offset = 0;
	
	for (idx_t i = 0; i < data_size; i++) {
		// assign the current value
		result_data[result_offset + i] = data_pointer[entry_pos];
		position_in_entry++;
		if (position_in_entry >= index_pointer[entry_pos]) {
			// handled all entries in this RLE value
			// move to the next entry
			entry_pos++;
			position_in_entry = 0;
		}
	}
}

void DictionaryDecompress (data_ptr_t buf_ptr, size_t buf_size, Vector &output, size_t data_size);

class DeCompressionFunction {
typedef void (*compression_decompress_data_t)(data_ptr_t buf_ptr, size_t buf_size, Vector &output, size_t data_size);

public:
    DeCompressionFunction() {}
    ~DeCompressionFunction() {}

    DeCompressionFunction(CompressionFunctionType func_type, PhysicalType &p_type) {
        SetDeCompressionFunction(func_type, p_type);
    }

    virtual void DeCompress(data_ptr_t buf_ptr, size_t buf_size, Vector &output, size_t data_size) {
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
            switch (p_type) {
            case PhysicalType::BOOL:
            case PhysicalType::INT8:
                decompress = RLEDecompress<int8_t>; break;
            case PhysicalType::INT16:
                decompress = RLEDecompress<int16_t>; break;
            case PhysicalType::INT32:
                decompress = RLEDecompress<int32_t>; break;
            case PhysicalType::INT64:
                decompress = RLEDecompress<int64_t>; break;
            case PhysicalType::INT128:
                decompress = RLEDecompress<hugeint_t>; break;
            case PhysicalType::UINT8:
                decompress = RLEDecompress<uint8_t>; break;
            case PhysicalType::UINT16:
                decompress = RLEDecompress<uint16_t>; break;
            case PhysicalType::UINT32:
                decompress = RLEDecompress<uint32_t>; break;
            case PhysicalType::UINT64:
                decompress = RLEDecompress<uint64_t>; break;
            case PhysicalType::FLOAT:
                decompress = RLEDecompress<float>;
            case PhysicalType::DOUBLE:
                decompress = RLEDecompress<double>;
            default:
                throw InternalException("Unsupported type for RLE");
            }
        } else if (func_type == DICTIONARY) {
            switch (p_type) {
            case PhysicalType::VARCHAR:
                decompress = DictionaryDecompress; break;
            default:
                throw InternalException("Unsupported type for Dictionary");
            }
        } else {
            D_ASSERT(false);
        }
    }

    compression_decompress_data_t decompress;
};

}

#endif
