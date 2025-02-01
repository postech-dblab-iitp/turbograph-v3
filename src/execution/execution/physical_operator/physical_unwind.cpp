#include "execution/physical_operator/physical_unwind.hpp"

#include "common/typedef.hpp"

#include "icecream.hpp"	

namespace duckdb {

PhysicalUnwind::PhysicalUnwind(Schema& sch, idx_t col_idx):
	CypherPhysicalOperator(PhysicalOperatorType::UNWIND, sch), col_idx(col_idx) { }

PhysicalUnwind::~PhysicalUnwind() {}

class UnwindState : public OperatorState {
public:
	explicit UnwindState() {
		checkpoint.first = 0;
		checkpoint.second = 0;
	 }
public:
	std::pair<size_t,size_t> checkpoint;
};

unique_ptr<OperatorState> PhysicalUnwind::GetOperatorState(ExecutionContext &context) const {
	return make_unique<UnwindState>();
}

OperatorResultType PhysicalUnwind::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	//std::cout << "Start Filter\n";
	auto &state = (UnwindState &)lstate;
	
	bool isHaveMoreOutput = false;
	int numProducedTuples = 0;

	if( input.size() == 0 ) return OperatorResultType::NEED_MORE_INPUT;

// IC( state.checkpoint.first, state.checkpoint.second );
	for( ; state.checkpoint.first < input.size(); state.checkpoint.first++ ) {
		auto listToUnwind = ListValue::GetChildren(input.GetValue(col_idx, state.checkpoint.first));
		for( ; state.checkpoint.second < listToUnwind.size() ; state.checkpoint.second++ ) {
			if( numProducedTuples == EXEC_ENGINE_VECTOR_SIZE ) {
				isHaveMoreOutput = true;
				goto breakLoop;
			}
			// produce left
			for (idx_t colId = 0; colId < col_idx ; colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
			}
			// prduce unwinded
			auto& value = listToUnwind.at(state.checkpoint.second);
			chunk.SetValue(col_idx, numProducedTuples, value );
			// produce right
			for (idx_t colId = col_idx+1; colId < chunk.ColumnCount() ; colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, state.checkpoint.first) );
			}
			numProducedTuples += 1;
		}
		// done with second
		state.checkpoint.second = 0;
	}

breakLoop:
	chunk.SetCardinality(numProducedTuples);
	if( isHaveMoreOutput ) { return OperatorResultType::HAVE_MORE_OUTPUT; }
	state.checkpoint.first = 0;
	state.checkpoint.second = 0;
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalUnwind::ParamsToString() const {
	return "Unwind-params";
}
std::string PhysicalUnwind::ToString() const {
	return "Unwind";
}
}

