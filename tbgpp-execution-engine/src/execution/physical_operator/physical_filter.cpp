
// #include "typedef.hpp"

// #include "execution/physical_operator/physical_filter.hpp"

// #include "execution/expression_executor.hpp"

// #include <string>

// namespace duckdb {

// class FilterState : public OperatorState {
// public:
// 	explicit FilterState():
// 		executor(Allocator::DEF, expr), sel(EXEC_ENGINE_VECTOR_SIZE) {
// 	}
// public:
// 	SelectionVector sel;
// 	ExpressionExecutor executor;
// };

// unique_ptr<OperatorState> PhysicalFilter::GetOperatorState() const {
// 	return make_unique<FilterState>();
// }

// OperatorResultType PhysicalFilter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
// 	//std::cout << "Start Filter\n";
// 	auto &state = (FilterState &)lstate;

// 	// // seek input using targetindex.
// 	// uint64_t rhs = duckdb::UBigIntValue::Get(predicateValue);

// 	// // compare for BIGINT
// 	// int numProducedTuples = 0;
// 	// int srcIdx;
// 	// //fprintf(stdout, "%s\n", input.ToString(1).c_str());
// 	// for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
// 	// 	duckdb::Value val = input.GetValue(targetColumn, srcIdx);
// 	// 	uint64_t lhs = duckdb::UBigIntValue::Get(val);
// 	// 	if( lhs == rhs ) {
// 	// 		// pass predicate
// 	// 		state.sel.set_index( numProducedTuples, srcIdx );
// 	// 		numProducedTuples += 1;
// 	// 	}
// 	// }
// 	// //std::cout << numProducedTuples << std::endl;
// 	// chunk.Slice(input, state.sel, numProducedTuples);

// 	// // clear sel for next chunk
// 	// // TODO Im not sure about this logic.. maybe need to debug?
// 	// state.sel.Initialize(state.sel);

// 	// // always return need_more_input
// 	// //std::cout << "End Filter\n";
// 	// return OperatorResultType::NEED_MORE_INPUT;
	
// }

// std::string PhysicalFilter::ParamsToString() const {
// 	return "filter-param";
// }

// std::string PhysicalFilter::ToString() const {
// 	return "Filter";
// }

// }