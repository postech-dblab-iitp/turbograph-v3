//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"

namespace duckdb {

//! PhysicalJoin represents the base class of the join operators
class PhysicalJoin : public CypherPhysicalOperator {
public:
	PhysicalJoin(Schema& sch, PhysicalOperatorType type, JoinType join_type);

	JoinType join_type;

public:
	bool EmptyResultIfRHSIsEmpty() const;

	static void ConstructSemiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]);
	static void ConstructAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]);
	static void ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
	                                    bool has_null);
	static void ConstructLeftJoinResult(DataChunk &left, DataChunk &result, bool found_match[]);
	static void ConstructLeftJoinResult(DataChunk &left, DataChunk &result, bool found_match[], const vector<uint32_t>& right_col_map);
};

} // namespace duckdb
