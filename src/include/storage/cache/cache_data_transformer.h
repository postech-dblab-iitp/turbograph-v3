#ifndef CACHE_DATA_TRANSFORMER_H
#define CACHE_DATA_TRANSFORMER_H

#include "common/constants.hpp"
#include "storage/cache/common.h"
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"
#include "storage/extent/compression/compression_header.hpp"

namespace duckdb {

class CacheDataTransformer {
public: 
    CacheDataTransformer() {}
    ~CacheDataTransformer();
    static void Swizzle(uint8_t* ptr);
    static void SwizzleVarchar(uint8_t* ptr);
    static void Unswizzle(uint8_t* ptr);
    static void UnswizzleVarchar(uint8_t* ptr);
    static SwizzlingType GetSwizzlingType(uint8_t* ptr);
};

}

#endif