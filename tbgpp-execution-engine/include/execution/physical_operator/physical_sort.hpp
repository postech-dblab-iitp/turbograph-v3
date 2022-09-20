#pragma once

#include "typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "main/client_context.hpp"

#include "common/types/chunk_collection.hpp"
#include "parallel/pipeline.hpp"
#include "planner/bound_query_node.hpp"


namespace duckdb {

class PhysicalSort: public CypherPhysicalOperator {

public:

	PhysicalSort(CypherSchema& sch, vector<BoundOrderByNode> orders);
	~PhysicalSort();

public:

	// sink
	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	virtual void Combine(ExecutionContext& context, LocalSinkState& lstate) const;

	// source
	void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	vector<BoundOrderByNode> orders;

};

	
}