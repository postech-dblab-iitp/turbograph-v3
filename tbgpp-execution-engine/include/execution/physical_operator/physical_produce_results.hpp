#pragma once

#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

class PhysicalProduceResults: public CypherPhysicalOperator {

public:
	// TODO actually, sink does not have output schema. we need sch for hiding adj and id. but we need more general, universal logic further.
	PhysicalProduceResults(CypherSchema& sch): CypherPhysicalOperator(sch) { }
	~PhysicalProduceResults() { }

public:
	
	SinkResultType Sink(ExecutionContext& context, DataChunk &input, LocalSinkState &lstate) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState() const override;
	void Combine(ExecutionContext& context, LocalSinkState& lstate) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

};	

}