//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/checksum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace s62 {

//! Compute a checksum over a buffer of size size
uint64_t Checksum(uint8_t *buffer, size_t size);

} // namespace s62
