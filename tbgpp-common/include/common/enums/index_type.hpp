//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/index_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace s62 {

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//
enum class IndexType : uint8_t {
	INVALID = 0, 		// invalid index type
	ART = 1,      		// Adaptive Radix Tree
	FORWARD_CSR = 2,	// CSR (adjacency list index)
	BACKWARD_CSR = 3,	// CSR (adjacency list index)
	PHYSICAL_ID = 4,	// Physical Tuple ID (we can directly access to tuple using this ID)
};

// //===--------------------------------------------------------------------===//
// // Index Constraint Types
// //===--------------------------------------------------------------------===//
// enum IndexConstraintType : uint8_t {
// 	NONE = 0,    // index is an index don't built to any constraint
// 	UNIQUE = 1,  // index is an index built to enforce a UNIQUE constraint
// 	PRIMARY = 2, // index is an index built to enforce a PRIMARY KEY constraint
// 	FOREIGN = 3  // index is an index built to enforce a FOREIGN KEY constraint
// };

} // namespace s62
