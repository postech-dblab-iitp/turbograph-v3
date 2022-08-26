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
	PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk, vector<unique_ptr<Expression>> storage_predicates);
	~PhysicalNodeScan();

public:
	
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState() const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	LabelSet labels;
	PropertyKeys propertyKeys;

	unique_ptr<Expression> filter_pushdown_expression;
	
};	

}