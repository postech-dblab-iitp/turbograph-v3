//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_hash_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "common/value_operations/value_operations.hpp"
#include "execution/join_hashtable.hpp"
//#include "execution/operator/join/perfect_hash_join_executor.hpp"
#include "execution/physical_operator/physical_comparison_join.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
// #include "planner/operator/logical_join.hpp"
#include "common/enums/join_type.hpp"

#include "typedef.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalHashJoin : public PhysicalComparisonJoin {
public:

// TODO fixme
	// PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	//                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	//                  const vector<idx_t> &right_projection_map, vector<LogicalType> delim_types,
	//                  idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_stats);
	// PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	//                  vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality,
	//                  PerfectHashJoinStats join_state);

	PhysicalHashJoin(Schema sch, 
		vector<JoinCondition> cond,
		JoinType join_type,
		vector<uint32_t> &output_left_projection_map,	// s62 style projection map
		vector<uint32_t> &output_right_projection_map,	// s62 style projection map
		vector<LogicalType> &right_build_types,
		vector<idx_t> &right_build_map	// duckdb style build map - what build types
	);

	//! S62 style projection map. Indexes into the output.
	vector<uint32_t> output_left_projection_map;
	vector<uint32_t> output_right_projection_map;
	//! build side projection map
	vector<idx_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Duplicate eliminated types; only used for delim_joins (i.e. correlated subqueries)
	vector<LogicalType> delim_types;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
								OperatorState &state_p, LocalSinkState &sink_state) const override;

public:
	// Sink Interface
	// unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &state) const override;
	void Combine(ExecutionContext& context, LocalSinkState& lstate) const override;
	// SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	//                           GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}

	std::string ParamsToString() const override;
	std::string ToString() const override;
};

} // namespace duckdb
