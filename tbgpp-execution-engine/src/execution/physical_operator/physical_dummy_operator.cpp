
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/physical_operator/physical_dummy_operator.hpp"


#include <string>

using namespace duckdb;
using namespace std;

class DummyOperatorState : public OperatorState {
public:
	explicit DummyOperatorState() {}
public:

	
};

PhysicalDummyOperator::PhysicalDummyOperator(CypherSchema& schema): CypherPhysicalOperator(schema) {}

OperatorResultType PhysicalDummyOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (DummyOperatorState &)state_p;
	// TODO do something
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalDummyOperator::ParamsToString() const {
	return "dummy-no-param";
}