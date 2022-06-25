
#include "typedef.hpp"

#include "execution/physical_operator/physical_dummy_operator.hpp"


#include <string>

using namespace duckdb;
using namespace std;

class DummyOperatorState : public OperatorState {
public:
	explicit DummyOperatorState() {}
public:

};

//PhysicalDummyOperator::PhysicalDummyOperator(CypherSchema& sch): CypherPhysicalOperator(sch) {}

unique_ptr<OperatorState> PhysicalDummyOperator::GetOperatorState() const {
	return make_unique<DummyOperatorState>();
}

OperatorResultType PhysicalDummyOperator::Execute(DataChunk &input, DataChunk &chunk, OperatorState &state) const {
	auto &lstate = (DummyOperatorState &)state;
	
	// doing nothing. just project with no schema alteration.
	chunk.Reference(input);

	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalDummyOperator::ParamsToString() const {
	return "dummy-no-param";
}