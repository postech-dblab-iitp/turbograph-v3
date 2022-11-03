#include "execution/physical_operator/physical_top.hpp"

#include "typedef.hpp"
#include "icecream.hpp"

#include <string>
#include "common/allocator.hpp"

namespace duckdb {

class TopState : public OperatorState {
public:
	explicit TopState(): current_offset((idx_t)0) {}
public:
	idx_t current_offset;
};

unique_ptr<OperatorState> PhysicalTop::GetOperatorState(ExecutionContext &context) const {
	return make_unique<TopState>();
}

OperatorResultType PhysicalTop::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	
	auto& state = (TopState &)lstate;
	D_ASSERT( state.current_offset >= 0 && (limit - state.current_offset >= 0 ));
	if( limit - state.current_offset == 0 ) {
		// Now all inputs are filled. finish pipeline.
		D_ASSERT( state.current_offset == limit);
		return OperatorResultType::FINISHED;
	}
	idx_t remaining = limit - state.current_offset;
	if( input.size() <= remaining ) {
		// all input survives
		chunk.Reference(input);
		state.current_offset += input.size();
		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		// some remaining, but need to slice
		// pass partially. in next function call it will return FINISHED
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < remaining; i++) {
			sel.set_index(i, i);
		}
		chunk.Slice(input, sel, remaining);
		state.current_offset += remaining;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// unreachable	
}

std::string PhysicalTop::ParamsToString() const {
	return "top-param";
}

std::string PhysicalTop::ToString() const {
	return "Top";
}

}