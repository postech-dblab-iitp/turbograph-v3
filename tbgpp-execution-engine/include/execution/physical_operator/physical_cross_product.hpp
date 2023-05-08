//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
// #include "execution/physical_operator.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {
//! PhysicalCrossProduct represents a cross product between two tables
class PhysicalCrossProduct : public CypherPhysicalOperator {
public:
	PhysicalCrossProduct(Schema &sch, vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map);

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           OperatorState &state, LocalSinkState &sink_state_p) const override;

public:
	// Sink Interface
	// unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const override;
	bool IsSink() const override { return true; }

public:

	string ParamsToString() const override;
	std::string ToString() const override;

	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;


};

} // namespace duckdb