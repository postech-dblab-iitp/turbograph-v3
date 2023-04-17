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
	PhysicalCrossProduct(CypherSchema &sch, unique_ptr<PhysicalOperator> left,
	                     unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality);

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           OperatorState &state) const override;

	// bool ParallelOperator() const override {
	// 	return true;
	// }

	// bool RequiresCache() const override {
	// 	return true;
	// }

public:
	// Sink Interface
	// unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const override;

	// bool IsSink() const override {
	// 	return true;
	// }
	// bool ParallelSink() const override {
	// 	return true;
	// }
};

} // namespace duckdb
