#include "typedef.hpp"

#include "execution/physical_operator/simple_projection.hpp"

#include <cassert>

namespace duckdb {

class SimpleProjectionState : public OperatorState {
public:
	explicit SimpleProjectionState() {}
public:

};

unique_ptr<OperatorState> SimpleProjection::GetOperatorState() const {
	return make_unique<SimpleProjectionState>();
}

OperatorResultType SimpleProjection::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const {
	//auto &state = (SimpleFilterState &)state;
	// state not necessary

	// int newIdx = 0;
	// for( int srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
	// 	for( auto oldIdx: colOrdering) {
	// 		chunk.SetValue(newIdx, srcIdx, input.GetValue(oldIdx, srcIdx) );
	// 		newIdx +=1;
	// 	}
	// 	newIdx = 0;
	// }

	int newIdx = 0;
	for(auto oldIdx: colOrdering) {
		chunk.data[newIdx].Reference( input.data[oldIdx] );
		newIdx += 1;
	}
	chunk.SetCardinality( input.size() );

	// always return need_more_input
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string SimpleProjection::ParamsToString() const {
	return "projection-param";
}

std::string SimpleProjection::ToString() const {
	return "SProjection";
}


} // namespace duckdb