//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/sel_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/vector_buffer.hpp"
#include "common/unordered_map.hpp"

namespace s62 {

//! Selection vector cache used for caching vector slices
struct SelCache {
	unordered_map<sel_t *, buffer_ptr<VectorBuffer>> cache;
};

} // namespace s62
