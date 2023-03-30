
#include "typedef.hpp"

#include "execution/physical_operator/physical_id_seek.hpp"
#include "extent/extent_iterator.hpp"

#include "icecream.hpp"

#include <string>

namespace duckdb {

class IdSeekState : public OperatorState {
public:
	explicit IdSeekState() {
		targetChunkInitialized = false;
	}
public:
	std::queue<ExtentIterator *> ext_its;
	DataChunk targetChunk;	// initialized when first execute() is called
	bool targetChunkInitialized;
};

unique_ptr<OperatorState> PhysicalIdSeek::GetOperatorState(ExecutionContext &context) const {
	return make_unique<IdSeekState>();
}

OperatorResultType PhysicalIdSeek::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {

	auto &state = (IdSeekState &)lstate;
	if(input.size() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

// icecream::ic.enable();
// IC(input.size());
// if (input.size() > 0) {
// 	IC(input.ToString(std::min((idx_t)10, input.size())));
// }
// IC(id_col_idx);
// icecream::ic.disable();

	idx_t nodeColIdx = id_col_idx;
	D_ASSERT(nodeColIdx < input.ColumnCount());

// IC();

	// target_types => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	if( ! state.targetChunkInitialized ) {
		state.targetChunk.Initialize(target_types, STANDARD_VECTOR_SIZE);
		state.targetChunkInitialized = true;
	}

	std::vector<LabelSet> empty_els;

	// initialize indexseek
	vector<ExtentID> target_eids;		// target extent ids to access
	vector<idx_t> boundary_position;	// boundary position of the input chunk

	context.client->graph_store->InitializeVertexIndexSeek(state.ext_its, oids, projection_mapping, input, nodeColIdx, target_types, target_eids, boundary_position);
	D_ASSERT( target_eids.size() == boundary_position.size() );
	// if (target_eids.size() != boundary_position.size()) {
	// 	fprintf(stderr, "target_eids.size() = %ld, boundary_position.size() = %ld\n", target_eids.size(), boundary_position.size());
	// 	for (size_t i = 0; i < target_eids.size(); i++) fprintf(stderr, "%d, ", target_eids[i]);
	// 	fprintf(stderr, "\n");
	// 	for (size_t i = 0; i < boundary_position.size(); i++) fprintf(stderr, "%ld, ", boundary_position[i]);
	// 	fprintf(stderr, "\n");
	// 	for (size_t i = 0; i < input.size(); i++) {
	// 		uint64_t vid = UBigIntValue::Get(input.GetValue(nodeColIdx, i));
	// 		ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	// 		fprintf(stderr, "(%ld, %d), ", vid, target_eid);
	// 	}
	// 	fprintf(stderr, "\n");
	// 	throw InvalidInputException("target_eids.size() != boundary_position.size()");
	// }
	vector<idx_t> output_col_idx;
	for (idx_t i = 0; i < inner_col_map.size(); i++) {
		output_col_idx.push_back(inner_col_map[i]);
	}
	for( u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++ ) {
		context.client->graph_store->doVertexIndexSeek(state.ext_its, chunk, input, nodeColIdx, target_types, target_eids, boundary_position, extentIdx, output_col_idx);
	}
	// TODO temporary code for deleting the existing iter
	auto ext_it_exist = state.ext_its.front();
	state.ext_its.pop();
	delete ext_it_exist;
	
icecream::ic.disable();

	// for original ones reference existing columns
	D_ASSERT(input.ColumnCount() == outer_col_map.size());
	for(int i = 0; i < input.ColumnCount(); i++) {
		if( outer_col_map[i] != std::numeric_limits<uint32_t>::max() ) {
			D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
			chunk.data[outer_col_map[i]].Reference(input.data[i]);
		}
	}
	chunk.SetCardinality( input.size() );

// icecream::ic.enable();
// IC(chunk.size());
// if (chunk.size() != 0)
// 	IC(chunk.ToString(std::min(10, (int)chunk.size())));
// icecream::ic.disable();

	return OperatorResultType::NEED_MORE_INPUT;


}

std::string PhysicalIdSeek::ParamsToString() const {
	std::string result = "";
	result += "id_col_idx=" + std::to_string(id_col_idx) + ", ";
	result += "projection_mapping.size()=" + std::to_string(projection_mapping.size()) + ", ";
	result += "projection_mapping[0].size()=" + std::to_string(projection_mapping[0].size()) + ", ";
	result += "target_types.size()=" + std::to_string(target_types.size()) + ", ";	
	return result;	
}

std::string PhysicalIdSeek::ToString() const {
	return "IdSeek";
}

}