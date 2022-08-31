#pragma once

#include "typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "main/client_context.hpp"

#include "planner/bound_result_modifier.hpp"

namespace duckdb {

class PhysicalTopNSort: public CypherPhysicalOperator {

public:
	//! Represents a physical ordering of the data. Note that this will not change
	//! the data but only add a selection vector.
	PhysicalTopNSort(CypherSchema& sch, vector<BoundOrderByNode> order_exprs, idx_t limit, idx_t offset);
	~PhysicalTopNSort();

public:

	// sink
	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState() const;
	virtual void Combine(ExecutionContext& context, LocalSinkState& lstate) const;

	// source
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState() const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	vector<BoundOrderByNode> order_exprs;
	idx_t limit;
	idx_t offset;

	// sharable global states
	

};

	
}