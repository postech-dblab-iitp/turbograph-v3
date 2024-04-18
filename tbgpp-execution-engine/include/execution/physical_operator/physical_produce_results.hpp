#pragma once

#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

class PhysicalProduceResults: public CypherPhysicalOperator {

public:
	PhysicalProduceResults(Schema& sch)
		: CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch) { }
	PhysicalProduceResults(Schema& sch, vector<uint8_t> projection_mapping)
		: CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch), projection_mapping(projection_mapping) { }
	PhysicalProduceResults(Schema& sch, vector<vector<uint8_t>> projection_mappings)
		: CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch), projection_mappings(projection_mappings) { }
	~PhysicalProduceResults() { }

public:
	
	SinkResultType Sink(ExecutionContext& context, DataChunk &input, LocalSinkState &lstate) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	void Combine(ExecutionContext& context, LocalSinkState& lstate) const override;
	bool IsSink() const override { return true; }
	DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
	vector<uint8_t> projection_mapping;
	vector<vector<uint8_t>> projection_mappings;
	mutable uint64_t num_nulls = 0;
};	

}