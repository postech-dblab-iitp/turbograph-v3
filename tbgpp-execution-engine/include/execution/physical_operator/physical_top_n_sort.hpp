#pragma once

#include "typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "main/client_context.hpp"

#include "planner/bound_result_modifier.hpp"

namespace s62 {

class PhysicalTopNSort: public CypherPhysicalOperator {

public:
	//! Represents a physical ordering of the data. Note that this will not change
	//! the data but only add a selection vector.
	PhysicalTopNSort(Schema& sch, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset);
	~PhysicalTopNSort();

public:

	// sink
	SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	void Combine(ExecutionContext& context, LocalSinkState& lstate) const override;
	bool IsSink() const override { return true; }
	DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;

	// source
	// void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	bool IsSource() const override { return true; }
	bool IsSourceDataRemaining(LocalSourceState &lstate, LocalSinkState &sink_state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	vector<BoundOrderByNode> orders;
	idx_t limit;
	idx_t offset;

};

	
}