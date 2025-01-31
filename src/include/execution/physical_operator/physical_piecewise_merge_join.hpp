//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/join_type.hpp"
#include "common/types/chunk_collection.hpp"
#include "common/value_operations/value_operations.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/physical_operator/physical_comparison_join.hpp"
#include "planner/bound_result_modifier.hpp"
#include "common/typedef.hpp"

namespace duckdb {

//! PhysicalPiecewiseMergeJoin represents a piecewise merge loop join between
//! two tables
class PhysicalPiecewiseMergeJoin : public PhysicalComparisonJoin {
   public:
    PhysicalPiecewiseMergeJoin(Schema sch, vector<JoinCondition> cond,
                               JoinType join_type,
                               vector<LogicalType> &lhs_types,
                               vector<LogicalType> &rhs_types,
                               /* s62 style projection map */
                               vector<uint32_t> &output_left_projection_map,
                               vector<uint32_t> &output_right_projection_map);

    vector<LogicalType> join_key_types;
    vector<LogicalType> lhs_types;
    vector<LogicalType> rhs_types;
    vector<BoundOrderByNode> lhs_orders;
    vector<BoundOrderByNode> rhs_orders;
	vector<uint32_t> output_left_projection_map;
	vector<uint32_t> output_right_projection_map;
    vector<unique_ptr<Expression>> lhs_expressions;

   public:
    // Operator Interface
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk, OperatorState &state,
                               LocalSinkState &sink_state) const override;

   public:
    // Sink Interface
    unique_ptr<LocalSinkState> GetLocalSinkState(
        ExecutionContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &input,
                        LocalSinkState &state_p) const override;
    void Combine(ExecutionContext &context,
                 LocalSinkState &state_p) const override;

    bool IsSink() const override { return true; }
    DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;

    std::string ParamsToString() const override;
    std::string ToString() const override;

   private:
    // resolve joins that output max N elements (SEMI, ANTI, MARK)
    void ResolveSimpleJoin(ExecutionContext &context, DataChunk &input,
                           DataChunk &chunk, OperatorState &state,
                           LocalSinkState &sink_state) const;
    // resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
    OperatorResultType ResolveComplexJoin(ExecutionContext &context,
                                          DataChunk &input, DataChunk &chunk,
                                          OperatorState &state,
                                          LocalSinkState &sink_state) const;
};

}  // namespace duckdb