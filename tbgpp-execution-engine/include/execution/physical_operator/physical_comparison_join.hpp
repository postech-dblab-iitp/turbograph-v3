//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator/physical_join.hpp"
#include "execution/expression_executor.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

namespace duckdb {
class ChunkCollection;

//! PhysicalJoin represents the base class of the join operators
class PhysicalComparisonJoin : public PhysicalJoin {
public:
	PhysicalComparisonJoin(Schema& sch, PhysicalOperatorType type, vector<JoinCondition> cond,
	                       JoinType join_type);

	PhysicalComparisonJoin(Schema& sch, PhysicalOperatorType type, vector<vector<JoinCondition>> cond,
	                       JoinType join_type);

	vector<JoinCondition> conditions;
	vector<vector<JoinCondition>> or_conditions;

public:
	string ParamsToString() const override;

	//! Construct the join result of a join with an empty RHS
	static void ConstructEmptyJoinResult(JoinType type, bool has_null, DataChunk &input, DataChunk &result);
	static void ConstructEmptyJoinResult(JoinType type, bool has_null, DataChunk &input, DataChunk &result, const vector<uint32_t>& lhs_col_map, const vector<uint32_t>& rhs_col_map);
	//! Construct the remainder of a Full Outer Join based on which tuples in the RHS found no match
	static void ConstructFullOuterJoinResult(bool *found_match, ChunkCollection &input, DataChunk &result,
	                                         idx_t &scan_position);
};

} // namespace duckdb
