#pragma once

#include "common/types.hpp"
#include "common/common.hpp"

namespace duckdb {

enum CompressionFunctionType {
    BITPACKING = 0
};

template <PhysicalType T, CompressionFunctionType func_type>
class CompressionFunction {
public:
    CompressionFunction() {}
    ~CompressionFunction() {}
};

}