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

	PhysicalSort(Schema& sch, vector<BoundOrderByNode> orders);
	~PhysicalSort();

public:

	// sink
	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	virtual void Combine(ExecutionContext& context, LocalSinkState& lstate) const;
	bool IsSink() const override { return true; }

	// source
	void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	bool IsSource() const override { return true; }

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	vector<BoundOrderByNode> orders;

};

	
}