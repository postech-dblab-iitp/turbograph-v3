#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class SimpleFilter: public CypherPhysicalOperator {

public:
	SimpleFilter(CypherSchema& sch, int targetColumn, duckdb::Value predicateValue)
		: CypherPhysicalOperator(sch), targetColumn(targetColumn), predicateValue(predicateValue) { }
	~SimpleFilter() {}

public:

	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	int targetColumn;
	duckdb::Value predicateValue;


};

}