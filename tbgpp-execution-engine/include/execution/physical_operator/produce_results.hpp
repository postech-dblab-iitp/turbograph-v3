#pragma once

#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

using namespace duckdb;

class ProduceResults: public CypherPhysicalOperator {

public:
	// TODO actually, sink does not have output schema. what then?
	ProduceResults() { }
	~ProduceResults() { }

public:
	
	SinkResultType Sink(DataChunk &input, LocalSinkState &lstate) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState() const override;
	void Combine(LocalSinkState& lstate) const override;

	std::string ParamsToString() const override;

};	