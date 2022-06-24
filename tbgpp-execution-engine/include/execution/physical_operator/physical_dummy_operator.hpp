#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

using namespace duckdb;

class PhysicalDummyOperator: public CypherPhysicalOperator {

public:
	PhysicalDummyOperator(CypherSchema& sch): CypherPhysicalOperator(sch) {}
	~PhysicalDummyOperator() { }

public:
	
	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
};