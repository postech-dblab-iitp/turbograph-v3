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
	
	PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk);
	PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk, string filter_pushdown_key, Value filter_pushdown_value);
	~PhysicalNodeScan();

public:
	
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	
	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	LabelSet labels;
	PropertyKeys propertyKeys;

	// filter pushdown
	string filter_pushdown_key;
	Value filter_pushdown_value;

	
};	

}