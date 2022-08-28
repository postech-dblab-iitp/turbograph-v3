//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_size.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.hpp"

namespace duckdb {

//! The vector size used in the storage
#ifndef STANDARD_VECTOR_SIZE
#define STANDARD_VECTOR_SIZE 1048576 // 1M, a unit of Disk IO
//#define STANDARD_VECTOR_SIZE 16384 // 16K
#endif

//! The vector size used in the execution engine
#ifndef EXEC_ENGINE_VECTOR_SIZE
#define EXEC_ENGINE_VECTOR_SIZE 1024	// the value is determined to fit chunk while processing in L1 cache
										//  of the recent processors thus to exploit advantages of vector-at-a-time processing model
#endif

#if ((STANDARD_VECTOR_SIZE & (STANDARD_VECTOR_SIZE - 1)) != 0)
#error Vector size should be a power of two
#endif

#if ((EXEC_ENGINE_VECTOR_SIZE & (EXEC_ENGINE_VECTOR_SIZE - 1)) != 0)
#error Vector size should be a power of two
#endif

#if ((STANDARD_VECTOR_SIZE / EXEC_ENGINE_VECTOR_SIZE != 0)
#error STANDARD_VECTOR_SIZE should be divided by EXEC_ENGINE_VECTOR_SIZE
#endif

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];

} // namespace duckdb
