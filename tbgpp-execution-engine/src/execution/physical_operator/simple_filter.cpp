
#include "typedef.hpp"

#include "execution/physical_operator/simple_filter.hpp"

#include <string>

using namespace std;

namespace duckdb {

class SimpleFilterState : public OperatorState {
public:
	explicit SimpleFilterState() {}
public:

};

unique_ptr<OperatorState> SimpleFilter::GetOperatorState() const {
	return make_unique<SimpleFilterState>();
}

OperatorResultType SimpleFilter::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const {
	//auto &state = (SimpleFilterState &)state;
	// state not necessary

	// seek input using targetindex.

	int64_t rhs = duckdb::BigIntValue::Get(predicateValue);
	// compare for BIGINT
	int numProducedTuples = 0;
	int srcIdx;
	for( srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		duckdb::Value val = input.GetValue(targetColumn, srcIdx);
		int64_t lhs = duckdb::BigIntValue::Get(val);
		if( lhs == rhs ) {
			// pass predicate
			for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
				chunk.SetValue(colId, numProducedTuples, input.GetValue(colId, srcIdx) );
			}
			numProducedTuples += 1;
		}
	}
	std::cout << numProducedTuples << std::endl;
	chunk.SetCardinality(numProducedTuples);

	// always return need_more_input
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string SimpleFilter::ParamsToString() const {
	return "filter-param";
}

std::string SimpleFilter::ToString() const {
	return "SimpleFilter";
}

}