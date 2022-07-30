// #include "typedef.hpp"

// #include "execution/physical_operator/limit.hpp"

// #include <cassert>

// namespace duckdb {

// class LimitState : public OperatorState {
// public:
// 	explicit LimitState(): current_count(0) {}
// public:
// 	uint64_t current_count;
// };

// unique_ptr<OperatorState> Limit::GetOperatorState() const {
// 	return make_unique<LimitState>();
// }

// OperatorResultType Limit::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
// 	auto &state = (LimitState &)lstate;

// 	uint64_t numTuplesToBeProduced = 0;



// 	// update state
// 	state.current_count -= numTuplesToBeProduced;
// 	D_ASSERT(state.current_count >= 0);
		
// 	// TODO need to be fixed.
// 	return OperatorResultType::NEED_MORE_INPUT;
// }

// std::string Limit::ParamsToString() const {
// 	return std::to_string(count);
// }

// std::string Limit::ToString() const {
// 	return "Limit";
// }


// } // namespace duckdb