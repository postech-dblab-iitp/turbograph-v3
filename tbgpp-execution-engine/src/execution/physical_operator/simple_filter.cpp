
#include "typedef.hpp"

#include "execution/physical_operator/simple_filter.hpp"

#include <string>

namespace duckdb {

class SimpleFilterState : public OperatorState {
public:
	explicit SimpleFilterState(): sel(STANDARD_VECTOR_SIZE) {}
public:
	SelectionVector sel;
};

unique_ptr<OperatorState> SimpleFilter::GetOperatorState() const {
	return make_unique<SimpleFilterState>();
}

OperatorResultType SimpleFilter::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	std::cout << "Start Filter\n";
	auto &state = (SimpleFilterState &)lstate;

	// seek input using targetindex.
	uint64_t rhs = duckdb::UBigIntValue::Get(predicateValue);

	// compare for BIGINT
	int numProducedTuples = 0;
	int srcIdx;
	fprintf(stdout, "%s\n", input.ToString(1).c_str());
	for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		duckdb::Value val = input.GetValue(targetColumn, srcIdx);
		uint64_t lhs = duckdb::UBigIntValue::Get(val);
		if( lhs == rhs ) {
			// pass predicate
			state.sel.set_index( numProducedTuples, srcIdx );
			numProducedTuples += 1;
		}
	}
	//std::cout << numProducedTuples << std::endl;
	chunk.Slice(input, state.sel, numProducedTuples);

	// clear sel for next chunk
	// TODO Im not sure about this logic.. maybe need to debug?
	state.sel.Initialize(state.sel);

	// always return need_more_input
	std::cout << "End Filter\n";
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string SimpleFilter::ParamsToString() const {
	return "filter-param";
}

std::string SimpleFilter::ToString() const {
	return "SFilter";
}

}