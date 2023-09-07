#include "cache/cache_data_transformer.h"
#include "common/types/string_type.hpp"
#include "common/string_util.hpp"

namespace duckdb {

SwizzlingType CacheDataTransformer::GetSwizzlingType(uint8_t* ptr) {
    CompressionHeader comp_header;
    memcpy(&comp_header, ptr, sizeof(CompressionHeader));
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
    memcpy(&comp_header, ptr, sizeof(CompressionHeader));
    size_t comp_header_valid_size = comp_header.GetValidSize();

    // Calculate Offsets
    size_t size = comp_header.data_len;
    size_t string_t_offset = comp_header_valid_size;
    size_t string_data_offset = comp_header_valid_size + size * sizeof(string_t);

    // Iterate over strings and swizzle
    for (int i = 0; i < size; i++) {
        // Get string
        string_t& str = *((string_t *)(ptr + string_t_offset));

        // Check not inlined
        if (!str.IsInlined()) {
            // Calculate address
            uint64_t offset = str.GetOffset();
            uint8_t* address = ptr + string_data_offset + offset;
            
            // Replace offset to address
            size_t size_without_offset = sizeof(string_t) - sizeof(offset);
            memcpy(ptr + string_t_offset + size_without_offset, &address, sizeof(address));
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
    memcpy(&comp_header, ptr, sizeof(CompressionHeader));
    size_t comp_header_valid_size = comp_header.GetValidSize();

    // Calculate Offsets
    size_t size = comp_header.data_len;
    size_t string_t_offset = comp_header_valid_size;
    size_t string_data_offset = comp_header_valid_size + size * sizeof(string_t);

    // Iterate over strings and unswizzle
    for (int i = 0; i < size; i++) {
        // Get string
        string_t& str = *((string_t *)(ptr + string_t_offset));

        // Check not inlined
        if (!str.IsInlined()) {
            // Calculate offset
            auto str_data_ptr = str.GetDataUnsafe();
            uint64_t offset = reinterpret_cast<uintptr_t>(str_data_ptr) - (reinterpret_cast<uintptr_t>(ptr + string_data_offset));

            // Replace address to offset
            size_t size_without_address = sizeof(string_t) - sizeof(str_data_ptr);
            memcpy(ptr + string_t_offset + size_without_address, &offset, sizeof(offset));
        }

        string_t_offset += sizeof(string_t);
    }
}

}