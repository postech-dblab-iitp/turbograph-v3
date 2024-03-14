#include "cache/cache_data_transformer.h"
#include "common/types/string_type.hpp"
#include "common/string_util.hpp"
#include <fstream>
#include <iostream>

namespace duckdb {

CacheDataTransformer::~CacheDataTransformer() {
}

SwizzlingType CacheDataTransformer::GetSwizzlingType(uint8_t* ptr) {
    CompressionHeader comp_header;
    memcpy(&comp_header, ptr, comp_header.GetSizeWoBitSet());
    return comp_header.swizzle_type;
}

void CacheDataTransformer::Swizzle(uint8_t* ptr) {
    SwizzlingType swizzle_type = GetSwizzlingType(ptr);
    switch(swizzle_type) {      
        case SwizzlingType::SWIZZLE_NONE:
            break;
        case SwizzlingType::SWIZZLE_VARCHAR:
            SwizzleVarchar(ptr);
            break;
        default:
            throw NotImplementedException("Swizzling type not implemented");
    }
}

void CacheDataTransformer::SwizzleVarchar(uint8_t* ptr) {
  // Read Compression Header
    CompressionHeader comp_header;
    memcpy(&comp_header, ptr, comp_header.GetSizeWoBitSet());
    size_t comp_header_valid_size = comp_header.GetSizeWoBitSet();

    // Calculate Offsets
    size_t size = comp_header.data_len;
    size_t string_t_offset = comp_header_valid_size;
    size_t string_data_offset = comp_header_valid_size + size * sizeof(string_t);
    size_t acc_string_length = 0;

    // Iterate over strings and swizzle
    for (int i = 0; i < size; i++) {
        // Get string
        string_t& str = *((string_t *)(ptr + string_t_offset));

        // Check not inlined
        if (!str.IsInlined()) {
            uint8_t* address = ptr + string_data_offset + acc_string_length;
            string_t swizzled_str(reinterpret_cast<char *>(address), str.GetSize());
            memcpy(ptr + string_t_offset, &swizzled_str, sizeof(string_t));
            acc_string_length += str.GetSize();
        }

        string_t_offset += sizeof(string_t);
    }
}

void CacheDataTransformer::Unswizzle(uint8_t* ptr) {
    SwizzlingType swizzle_type = GetSwizzlingType(ptr);
    switch(swizzle_type) {
        case SWIZZLE_NONE:
            break;
        case SWIZZLE_VARCHAR:
            UnswizzleVarchar(ptr);
            break;
        default:
            throw NotImplementedException("Swizzling type not implemented");
    }
}

void CacheDataTransformer::UnswizzleVarchar(uint8_t* ptr) {
    // Read Compression Header
    CompressionHeader comp_header;
    memcpy(&comp_header, ptr, comp_header.GetSizeWoBitSet());
    size_t comp_header_valid_size = comp_header.GetSizeWoBitSet();

    // Calculate Offsets
    size_t size = comp_header.data_len;
    size_t string_t_offset = comp_header_valid_size;
    size_t acc_string_length = 0;

    // Iterate over strings and unswizzle
    for (int i = 0; i < size; i++) {
        // Get string
        string_t& str = *((string_t *)(ptr + string_t_offset));

        // Check not inlined
        if (!str.IsInlined()) {
            // string_t unswizzled_str(str.GetDataUnsafe(), str.GetSize(), acc_string_length);
            // memcpy(ptr + string_t_offset, &unswizzled_str, sizeof(string_t));
            str.SetOffset(acc_string_length);
            acc_string_length += str.GetSize();
        }

        string_t_offset += sizeof(string_t);
    }
}

}