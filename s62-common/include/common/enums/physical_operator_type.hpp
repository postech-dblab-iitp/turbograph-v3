//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/physical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace s62 {

enum class OperatorType : uint8_t {
	UNARY,
	BINARY
};

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType : uint8_t {
// cypher operators
	INVALID,
//JOINS
	ADJ_IDX_JOIN,
	VARLEN_ADJ_IDX_JOIN,
	ID_SEEK,
	CROSS_PRODUCT,
	BLOCKWISE_NL_JOIN,
	HASH_JOIN,
	PIECEWISE_MERGE_JOIN,
// RELATIONAL
	FILTER,
	PROJECTION, 
	TOP,
	SORT,
	TOP_N_SORT,
	HASH_AGGREGATE,
//DATA SOURCE
	NODE_SCAN,
//ETC
	UNWIND,
	PRODUCE_RESULTS,
	SHORTEST_PATH,
	ALL_SHORTEST_PATH
};

} // namespace s62
