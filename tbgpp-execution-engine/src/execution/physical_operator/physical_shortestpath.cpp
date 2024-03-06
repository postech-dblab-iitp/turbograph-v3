
#include "typedef.hpp"
#include "execution/physical_operator/physical_shortestpath.hpp"

namespace duckdb {


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

unique_ptr<LocalSinkState> PhysicalShortestPath::GetLocalSinkState(ExecutionContext &context) const {
	return nullptr;
}

SinkResultType PhysicalShortestPath::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const {
    return SinkResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//

void PhysicalShortestPath::Combine(ExecutionContext &context, LocalSinkState &lstate_p) const {
	return;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

unique_ptr<LocalSourceState> PhysicalShortestPath::GetLocalSourceState(ExecutionContext &context) const {
	return nullptr;
}

void PhysicalShortestPath::GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const{
	return;
}


//===--------------------------------------------------------------------===//
// ETC
//===--------------------------------------------------------------------===//

std::string PhysicalShortestPath::ParamsToString() const {
	std::string result = "";
	result += "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
	result += "inner_col_map.size()=" + std::to_string(inner_col_map.size()) + ", ";
	result += "adjidx_obj_id=" + std::to_string(adjidx_obj_id) + ", ";
	result += "sid_col_idx=" + std::to_string(sid_col_idx) + ", ";
	result += "id_col_idx=" + std::to_string(id_col_idx) + ", ";
    result += "min_length=" + std::to_string(min_length) + ", ";
    result += "max_length=" + std::to_string(max_length) + ", ";
	result += "projection_mapping.size()=" + std::to_string(projection_mapping.size()) + ", ";
	result += "projection_mapping[0].size()=" + std::to_string(projection_mapping[0].size()) + ", ";
	result += "target_types.size()=" + std::to_string(target_types.size()) + ", ";	
	return result;	
}

std::string PhysicalShortestPath::ToString() const {
	return "ShortestPath";
}

}
