#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class PhysicalNodeIdSeek: public CypherPhysicalOperator {

public:
	PhysicalNodeIdSeek(CypherSchema& sch, std::string name, LabelSet labels, PropertyKeys propertyKeys)
		: CypherPhysicalOperator(sch), name(name), labels(labels), propertyKeys(propertyKeys) { }
	~PhysicalNodeIdSeek() {}

public:

	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// parameters
	std::string name;
	LabelSet labels;
		// TODO labels should not exist... but storage API needs it
	PropertyKeys propertyKeys;

};

}