
#include "typedef.hpp"

#include "execution/physical_operator/physical_edge_id_seek.hpp"
#include "extent/extent_iterator.hpp"

#include "icecream.hpp"

#include <string>

namespace duckdb {

class EdgeIdSeekState : public OperatorState {
public:
	explicit EdgeIdSeekState() {}
public:
	ExtentIterator* ext_it;
};

unique_ptr<OperatorState> PhysicalEdgeIdSeek::GetOperatorState(ExecutionContext &context) const {
	return make_unique<EdgeIdSeekState>();
}

OperatorResultType PhysicalEdgeIdSeek::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {

icecream::ic.enable();
IC( input.ToString(1) );
	auto &state = (EdgeIdSeekState &)lstate;
IC();
	DataChunk targetTupleChunk;
	CypherSchema outputEdgeSchema = schema.getSubSchemaOfKey( name );
	
	idx_t edgeColIdx = schema.getColIdxOfKey( name ); // position of pid
IC(edgeColIdx);
	idx_t alreadyExistingCols = outputEdgeSchema.getTypes().size() - 1 - propertyKeys.size();
IC(alreadyExistingCols);
	idx_t colIdxToStartFetch = edgeColIdx + alreadyExistingCols + 1;
IC(colIdxToStartFetch);

IC();
	vector<LogicalType> targetTypes;
	targetTypes.push_back(LogicalType::ID); // for edge ids
	for( auto& key: propertyKeys ) {
		targetTypes.push_back( outputEdgeSchema.getType(key) );
	}
	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	targetTupleChunk.Initialize(targetTypes);
	D_ASSERT( propertyKeys.size()+1 == targetTypes.size() );

	std::vector<LabelSet> empty_els;
IC();
	int numProducedTuples = 0;
	// for fetched columns, call api 
	for( u_int64_t srcIdx=0 ; srcIdx < input.size(); srcIdx++) {
		// fetch value
		uint64_t vid = UBigIntValue::Get(input.GetValue(edgeColIdx, srcIdx));
		// pass value
		context.client->graph_store->doIndexSeek(state.ext_it, targetTupleChunk, vid, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes); // TODO need to fix API
		assert( targetTupleChunk.size() == 1 && "did not fetch well");
		// set value
		for (idx_t colId = 1; colId < targetTupleChunk.ColumnCount(); colId++) {	// abandon pid and use only newly added columns
			chunk.SetValue(colId-1+colIdxToStartFetch, numProducedTuples, targetTupleChunk.GetValue(colId, 0) ); 
		}
		targetTupleChunk.Reset();
		numProducedTuples +=1;
	}
IC();
	// for original ones reference existing columns
	for(int i = 0; i <= edgeColIdx+alreadyExistingCols; i++) {
		chunk.data[i].Reference( input.data[i] );
	}
	idx_t idxToPutRightOnes = edgeColIdx + outputEdgeSchema.getTypes().size();
	idx_t numAddedColumns = targetTupleChunk.ColumnCount() - 1 - alreadyExistingCols;
IC( int(numAddedColumns) );
	for(int i = idxToPutRightOnes ; i < chunk.ColumnCount() ; i++) {
		chunk.data[i].Reference( input.data[ i-numAddedColumns ] );
	}
	chunk.SetCardinality( input.size() );
IC(chunk.ToString(1));
icecream::ic.disable();

	return OperatorResultType::NEED_MORE_INPUT;


}

std::string PhysicalEdgeIdSeek::ParamsToString() const {
	return "EdgeIdSeek-param";
}

std::string PhysicalEdgeIdSeek::ToString() const {
	return "EdgeIdSeek";
}

}