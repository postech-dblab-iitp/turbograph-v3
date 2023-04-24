#pragma once

#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

class PhysicalProduceResults: public CypherPhysicalOperator {

public:
	PhysicalProduceResults(CypherSchema& sch)
		: CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch) { }
	PhysicalProduceResults(CypherSchema& sch, std::vector<uint8_t> projection_mapping)
		: CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch), projection_mapping(projection_mapping) { }
	~PhysicalProduceResults() { }

public:
	
	SinkResultType Sink(ExecutionContext& context, DataChunk &input, LocalSinkState &lstate) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	void Combine(ExecutionContext& context, LocalSinkState& lstate) const override;
	bool IsSink() const override { return true; }

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
	std::vector<uint8_t> projection_mapping;
};	

}