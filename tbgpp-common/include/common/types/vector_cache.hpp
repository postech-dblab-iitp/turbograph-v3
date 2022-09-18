//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.hpp"
#include "common/vector.hpp"
#include "common/types/vector_buffer.hpp"

namespace duckdb {
class Vector;

//! The VectorCache holds cached data that allows for re-use of the same memory by vectors
class VectorCache {
public:
	//! Instantiate a vector cache with the given type
	VectorCache() {}
	explicit VectorCache(const LogicalType &type);
	explicit VectorCache(const LogicalType &type, size_t size = STANDARD_VECTOR_SIZE);

	buffer_ptr<VectorBuffer> buffer;

public:
	void ResetFromCache(Vector &result) const;
	void AllocateBuffer(const LogicalType &type, size_t size = STANDARD_VECTOR_SIZE);
	const LogicalType &GetType() const;
};

} // namespace duckdb
