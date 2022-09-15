
#include "typedef.hpp"

#include "execution/physical_operator/physical_node_id_seek.hpp"
#include "extent/extent_iterator.hpp"

#include "icecream.hpp"

#include <string>

namespace duckdb {

class NodeIdSeekState : public OperatorState {
public:
	explicit NodeIdSeekState() {}
public:
	ExtentIterator* ext_it = nullptr;
};

unique_ptr<OperatorState> PhysicalNodeIdSeek::GetOperatorState(ExecutionContext &context) const {
	return make_unique<NodeIdSeekState>();
}

OperatorResultType PhysicalNodeIdSeek::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {

// icecream::ic.enable();
// IC();
// IC( input.ToString(1) );

	auto &state = (NodeIdSeekState &)lstate;
IC();
	DataChunk targetTupleChunk;
	CypherSchema outputNodeSchema = schema.getSubSchemaOfKey( name );
	
	idx_t nodeColIdx = schema.getColIdxOfKey( name ); // position of pid
IC(nodeColIdx);
	idx_t alreadyExistingCols = outputNodeSchema.getTypes().size() - 1 - propertyKeys.size();
IC(alreadyExistingCols);
	idx_t colIdxToStartFetch = nodeColIdx + alreadyExistingCols + 1;
IC(colIdxToStartFetch);

IC();
	vector<LogicalType> targetTypes;
	targetTypes.push_back(LogicalType::ID); // for node ids
	for( auto& key: propertyKeys ) {
// icecream::ic.enable();
// IC(outputNodeSchema.toString());
// IC(key);
// icecream::ic.disable();
		targetTypes.push_back( outputNodeSchema.getType(key) );
	}

for( auto& type: targetTypes) { IC(type.ToString());}
for( auto& k: propertyKeys) { IC(k); }

	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	targetTupleChunk.Initialize(targetTypes);
	D_ASSERT( propertyKeys.size()+1 == targetTypes.size() );

	std::vector<LabelSet> empty_els;
	int numProducedTuples = 0;

	// initialize indexseek
	context.client->graph_store->InitializeVertexIndexSeek(state.ext_it, targetTupleChunk, input, nodeColIdx, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes);

	// for fetched columns, call api
	for( u_int64_t srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		// fetch value
		uint64_t vid = UBigIntValue::Get(input.GetValue(nodeColIdx, srcIdx));
		// pass value
		context.client->graph_store->doVertexIndexSeek(state.ext_it, targetTupleChunk, vid, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes); // TODO need to fix API
		assert( targetTupleChunk.size() == 1 && "did not fetch well");
		// set value
		for (idx_t colId = 1; colId < targetTupleChunk.ColumnCount(); colId++) {	// abandon pid and use only newly added columns
			chunk.SetValue(colId-1+colIdxToStartFetch, numProducedTuples, targetTupleChunk.GetValue(colId, 0) ); 
		}
		targetTupleChunk.Reset();
		numProducedTuples +=1;
	}
icecream::ic.disable();
IC();
	// for original ones reference existing columns
	for(int i = 0; i <= nodeColIdx+alreadyExistingCols; i++) {
		chunk.data[i].Reference( input.data[i] );
	}
	idx_t idxToPutRightOnes = nodeColIdx + outputNodeSchema.getTypes().size();
	idx_t numAddedColumns = targetTupleChunk.ColumnCount() - 1 - alreadyExistingCols;
IC( int(numAddedColumns) );
	for(int i = idxToPutRightOnes ; i < chunk.ColumnCount() ; i++) {
		chunk.data[i].Reference( input.data[ i-numAddedColumns ] );
	}
	chunk.SetCardinality( input.size() );
// IC(chunk.ToString(1));
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