#pragma once
#include "typedef.hpp"

#include "storage/graph_store.hpp"
#include "main/client_context.hpp"

#include "planner/expression.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

#include <vector>

namespace duckdb {

class PhysicalNodeScan: public CypherPhysicalOperator {

public:
	
	PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping);
	~PhysicalNodeScan();

public:
	
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	
	std::string ParamsToString() const override;
	std::string ToString() const override;

	// scan parameters
	mutable vector<idx_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;

	// filter pushdown
	mutable string filter_pushdown_key;
	mutable Value filter_pushdown_value;

	
};	

}