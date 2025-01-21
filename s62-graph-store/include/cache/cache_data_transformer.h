#include "common/constants.hpp"
#include "cache/common.h"
#include "cache/disk_aio/Bin_aio_handler.hpp"
#include "extent/compression/compression_header.hpp"

namespace s62 {

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