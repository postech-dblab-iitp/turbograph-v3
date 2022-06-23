#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

using namespace duckdb;
using namespace std;

class PhysicalDummyOperator: public CypherPhysicalOperator {

public:
	PhysicalDummyOperator(CypherSchema& schema);

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	string ParamsToString() const override;
};