//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_blockwise_nl_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/physical_operator/physical_join.hpp"
#include "planner/expression.hpp"

namespace duckdb {

//! PhysicalBlockwiseNLJoin represents a nested loop join between two tables on arbitrary expressions. This is different
//! from the PhysicalNestedLoopJoin in that it does not require expressions to be comparisons between the LHS and the
//! RHS.
class PhysicalBlockwiseNLJoin : public PhysicalJoin {
public:
	PhysicalBlockwiseNLJoin(Schema& sch, unique_ptr<Expression> condition, JoinType join_type, vector<uint32_t> &outer_col_map_p, vector<uint32_t> &inner_col_map_p);

	unique_ptr<Expression> condition;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state, LocalSinkState &sink_state) const override;
	void ConstructOutputChunk(DataChunk& chunk, DataChunk& output_chunk) const;

public:
	// Source interface
	// unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	//void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const override;

	// bool IsSource() const override {
	// 	//return IsRightOuterJoin(join_type);
	// 	return false; // S62 currently no right outer join!
	// }

public:
	// Sink interface
	// unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const override;
	DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;
	// SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	//                           GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}

public:
	string ParamsToString() const override;
	string ToString() const override;
	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;


};

} // namespace duckdb
