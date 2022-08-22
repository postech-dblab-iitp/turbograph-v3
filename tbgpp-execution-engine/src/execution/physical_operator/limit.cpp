#include "typedef.hpp"

#include "execution/physical_operator/limit.hpp"

#include <cassert>
#include <algorithm>

// TODO Limit is meant to be sink operator, but can work as long its 

namespace duckdb {

class LimitState : public OperatorState {
public:
	explicit LimitState(uint64_t count): current_count(count), sel(EXEC_ENGINE_VECTOR_SIZE) {}
public:
	SelectionVector sel;
	uint64_t current_count;
};

unique_ptr<OperatorState> Limit::GetOperatorState() const {
	return make_unique<LimitState>(count);
}

OperatorResultType Limit::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (LimitState &)lstate;

	uint64_t numTuplesToBeProduced = std::min( input.size(), state.current_count );
	D_ASSERT(numTuplesToBeProduced <= EXEC_ENGINE_VECTOR_SIZE );
	std::cout << numTuplesToBeProduced << " tups to be produced" << std::endl;

	for( int64_t srcIdx=0 ; srcIdx < numTuplesToBeProduced; srcIdx++) {
		state.sel.set_index( srcIdx, srcIdx );
	}
	chunk.Slice(input, state.sel, numTuplesToBeProduced );

	// update state
	state.current_count -= numTuplesToBeProduced;
	D_ASSERT(state.current_count >= 0);
	state.sel.Initialize(state.sel);
		
	// TODO need to be fixed.
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string Limit::ParamsToString() const {
	return std::to_string(count);
}

std::string Limit::ToString() const {
	return "Limit";
}


} // namespace duckdb