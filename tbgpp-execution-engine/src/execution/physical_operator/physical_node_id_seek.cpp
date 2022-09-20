
#include "typedef.hpp"

#include "execution/physical_operator/physical_node_id_seek.hpp"
#include "extent/extent_iterator.hpp"

#include "icecream.hpp"

#include <string>

namespace duckdb {

class NodeIdSeekState : public OperatorState {
public:
	explicit NodeIdSeekState() {
		targetChunkInitialized = false;
	}
public:
	ExtentIterator* ext_it = nullptr;
	DataChunk targetChunk;	// initialized when first execute() is called
	bool targetChunkInitialized;
};

unique_ptr<OperatorState> PhysicalNodeIdSeek::GetOperatorState(ExecutionContext &context) const {
	return make_unique<NodeIdSeekState>();
}

OperatorResultType PhysicalNodeIdSeek::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
// icecream::ic.enable();
// IC(input.size());
// if (input.size() != 0)
// 	IC( input.ToString(std::min(10, (int)input.size())) );
// icecream::ic.disable();

	auto &state = (NodeIdSeekState &)lstate;
// IC();
	CypherSchema outputNodeSchema = schema.getSubSchemaOfKey( name );
	
	idx_t nodeColIdx = schema.getColIdxOfKey( name ); // position of pid
	idx_t alreadyExistingCols = outputNodeSchema.getTypes().size() - 1 - propertyKeys.size();
	idx_t colIdxToStartFetch = nodeColIdx + alreadyExistingCols + 1;

// IC();
	vector<LogicalType> targetTypes;
	targetTypes.push_back(LogicalType::ID); // for node ids
	for( auto& key: propertyKeys ) {
		targetTypes.push_back( outputNodeSchema.getType(key) );
	}

	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	if( ! state.targetChunkInitialized ) {
		state.targetChunk.Initialize(targetTypes, STANDARD_VECTOR_SIZE);
		state.targetChunkInitialized = true;
	}
	D_ASSERT( propertyKeys.size()+1 == targetTypes.size() );

	std::vector<LabelSet> empty_els;

	// initialize indexseek
	vector<ExtentID> target_eids;		// target extent ids to access
	vector<idx_t> boundary_position;	// boundary position of the input chunk
	
	context.client->graph_store->InitializeVertexIndexSeek(state.ext_it, chunk, input, nodeColIdx, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes, target_eids, boundary_position);
	D_ASSERT( target_eids.size() == boundary_position.size() );
	vector<idx_t> output_col_idx;
	for (idx_t colId = 0; colId < state.targetChunk.ColumnCount(); colId++) {
		output_col_idx.push_back( colId-1+colIdxToStartFetch );
	}

	for( u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++ ) {
		context.client->graph_store->doVertexIndexSeek(state.ext_it, chunk, input, nodeColIdx, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes, target_eids, boundary_position, extentIdx, output_col_idx);
	}

icecream::ic.disable();

	// for original ones reference existing columns
	for(int i = 0; i <= nodeColIdx+alreadyExistingCols; i++) {
		chunk.data[i].Reference( input.data[i] );
	}
	idx_t idxToPutRightOnes = nodeColIdx + outputNodeSchema.getTypes().size();
	idx_t numAddedColumns = state.targetChunk.ColumnCount() - 1 - alreadyExistingCols;
	for(int i = idxToPutRightOnes ; i < chunk.ColumnCount() ; i++) {
		chunk.data[i].Reference( input.data[ i-numAddedColumns ] );
	}
	chunk.SetCardinality( input.size() );

// icecream::ic.enable();
// IC(chunk.size());
// if (chunk.size() != 0)
// 	IC(chunk.ToString(std::min(10, (int)chunk.size())));
// icecream::ic.disable();

	return OperatorResultType::NEED_MORE_INPUT;


}

std::string PhysicalNodeIdSeek::ParamsToString() const {
	return "NodeIdSeek-param";
}

std::string PhysicalNodeIdSeek::ToString() const {
	return "NodeIdSeek";
}

}