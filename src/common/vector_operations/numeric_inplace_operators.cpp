//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "common/vector_operations/vector_operations.hpp"

#include <algorithm>

namespace duckdb {

//===--------------------------------------------------------------------===//
// In-Place Addition
//===--------------------------------------------------------------------===//

void VectorOperations::AddInPlace(Vector &input, int64_t right, idx_t count) {
	D_ASSERT(input.GetType().id() == LogicalTypeId::POINTER);
	if (right == 0) {
		return;
	}
	switch (input.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		D_ASSERT(!ConstantVector::IsNull(input));
		auto data = ConstantVector::GetData<uintptr_t>(input);
		*data += right;
		break;
	}
	default: {
		D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);
		auto data = FlatVector::GetData<uintptr_t>(input);
		for (idx_t i = 0; i < count; i++) {
			data[i] += right;
		}
		break;
	}
	}
}

} // namespace duckdb
