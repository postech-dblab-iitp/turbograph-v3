//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/physical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

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

//// below are duckdb operators
	// INVALID,
	// ORDER_BY,
	// LIMIT,
	// STREAMING_LIMIT,
	// LIMIT_PERCENT,
	// TOP_N,
	// WINDOW,
	// UNNEST,
	// SIMPLE_AGGREGATE,
	// HASH_GROUP_BY,
	// PERFECT_HASH_GROUP_BY,
	// FILTER,
	// PROJECTION,
	// COPY_TO_FILE,
	// RESERVOIR_SAMPLE,
	// STREAMING_SAMPLE,
	// STREAMING_WINDOW,
	// // -----------------------------
	// // Scans
	// // -----------------------------
	// TABLE_SCAN,
	// DUMMY_SCAN,
	// CHUNK_SCAN,
	// RECURSIVE_CTE_SCAN,
	// DELIM_SCAN,
	// EXPRESSION_SCAN,
	// // -----------------------------
	// // Joins
	// // -----------------------------
	// BLOCKWISE_NL_JOIN,
	// NESTED_LOOP_JOIN,
	// HASH_JOIN,
	// CROSS_PRODUCT,
	// PIECEWISE_MERGE_JOIN,
	// IE_JOIN,
	// DELIM_JOIN,
	// INDEX_JOIN,
	// // -----------------------------
	// // SetOps
	// // -----------------------------
	// UNION,
	// RECURSIVE_CTE,

	// // -----------------------------
	// // Updates
	// // -----------------------------
	// INSERT,
	// DELETE_OPERATOR,
	// UPDATE,

	// // -----------------------------
	// // Schema
	// // -----------------------------
	// CREATE_TABLE,
	// CREATE_TABLE_AS,
	// CREATE_INDEX,
	// ALTER,
	// CREATE_SEQUENCE,
	// CREATE_VIEW,
	// CREATE_SCHEMA,
	// CREATE_MACRO,
	// DROP,
	// PRAGMA,
	// TRANSACTION,
	// CREATE_TYPE,

	// // -----------------------------
	// // Helpers
	// // -----------------------------
	// EXPLAIN,
	// EXPLAIN_ANALYZE,
	// EMPTY_RESULT,
	// EXECUTE,
	// PREPARE,
	// VACUUM,
	// EXPORT,
	// SET,
	// LOAD,
	// INOUT_FUNCTION,
	// RESULT_COLLECTOR
};

// string PhysicalOperatorToString(PhysicalOperatorType type);

} // namespace duckdb
