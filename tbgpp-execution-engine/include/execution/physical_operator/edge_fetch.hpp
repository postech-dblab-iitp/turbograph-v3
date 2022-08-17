#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class EdgeFetch: public CypherPhysicalOperator {

public:
	EdgeFetch(CypherSchema& sch, std::string name, LabelSet labels, PropertyKeys propertyKeys)
		: CypherPhysicalOperator(sch), name(name), labels(labels), propertyKeys(propertyKeys) { }
	~EdgeFetch() {}

public:

	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	std::string name;
	LabelSet labels;
	PropertyKeys propertyKeys;

};

}