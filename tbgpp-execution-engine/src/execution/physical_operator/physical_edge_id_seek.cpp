
// #include "typedef.hpp"

// #include "execution/physical_operator/physical_edge_id_seek.hpp"
// #include "extent/extent_iterator.hpp"

// #include "icecream.hpp"

// #include <string>

// namespace duckdb {

// class EdgeIdSeekState : public OperatorState {
// public:
// 	explicit EdgeIdSeekState() {
// 		targetChunkInitialized = false;
// 	}
// public:
// 	ExtentIterator* ext_it = nullptr;
// 	DataChunk targetChunk;	// initialized when first execute() is called
// 	bool targetChunkInitialized;
// };

// unique_ptr<OperatorState> PhysicalEdgeIdSeek::GetOperatorState(ExecutionContext &context) const {
// 	return make_unique<EdgeIdSeekState>();
// }

// OperatorResultType PhysicalEdgeIdSeek::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
// 	auto &state = (EdgeIdSeekState &)lstate;

// 	if(input.size() == 0) {
// 		chunk.SetCardinality(0);
// 		return OperatorResultType::NEED_MORE_INPUT;
// 	}
// 	//CypherSchema outputEdgeSchema = schema.getSubSchemaOfKey( name );
	
// 	idx_t nodeColIdx = id_col_idx;
// 	idx_t alreadyExistingCols = outputEdgeSchema.getTypes().size() - 1 - propertyKeys.size();
// 	idx_t colIdxToStartFetch = nodeColIdx + alreadyExistingCols + 1;

// // IC();
// 	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
// 	if( ! state.targetChunkInitialized ) {
// 		state.targetChunk.Initialize(target_types, STANDARD_VECTOR_SIZE);
// 		state.targetChunkInitialized = true;
// 	}
// 	D_ASSERT( propertyKeys.size()+1 == target_types.size() );

// 	std::vector<LabelSet> empty_els;

// 	// initialize indexseek
// 	vector<ExtentID> target_eids;		// target extent ids to access
// 	vector<idx_t> boundary_position;	// boundary position of the input chunk


// //	context.client->graph_store->InitializeEdgeIndexSeek(state.ext_it, chunk, input, id_col_idx, target_eids, boundary_position);	// TODO s62 figure out sid

// 	D_ASSERT( target_eids.size() == boundary_position.size() );
// 	if (target_eids.size() != boundary_position.size()) {
// 		fprintf(stderr, "target_eids.size() = %ld, boundary_position.size() = %ld\n", target_eids.size(), boundary_position.size());
// 		for (size_t i = 0; i < target_eids.size(); i++) fprintf(stderr, "%d, ", target_eids[i]);
// 		fprintf(stderr, "\n");
// 		for (size_t i = 0; i < boundary_position.size(); i++) fprintf(stderr, "%ld, ", boundary_position[i]);
// 		fprintf(stderr, "\n");
// 		for (size_t i = 0; i < input.size(); i++) {
// 			uint64_t vid = UBigIntValue::Get(input.GetValue(nodeColIdx, i));
// 			ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
// 			fprintf(stderr, "(%ld, %d), ", vid, target_eid);
// 		}
// 		fprintf(stderr, "\n");
// 		throw InvalidInputException("target_eids.size() != boundary_position.size()");
// 	}
// 	vector<idx_t> output_col_idx;
// 	for (idx_t colId = 0; colId < state.targetChunk.ColumnCount(); colId++) {
// 		output_col_idx.push_back( colId-1+colIdxToStartFetch );
// 	}
// // icecream::ic.enable(); IC(); icecream::ic.disable();
// 	for( u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++ ) {

// 		// TODO figure out sid
// 		//context.client->graph_store->doEdgeIndexSeek(state.ext_it, chunk, input, nodeColIdx, labels, empty_els, LoadAdjListOption::NONE, propertyKeys, targetTypes, target_eids, boundary_position, extentIdx, output_col_idx);
// 	}
// // icecream::ic.enable(); IC(); icecream::ic.disable();
// icecream::ic.disable();

// 	// for original ones reference existing columns
// 	for(int i = 0; i <= nodeColIdx+alreadyExistingCols; i++) {
// 		chunk.data[i].Reference( input.data[i] );
// 	}
// 	idx_t idxToPutRightOnes = nodeColIdx + outputEdgeSchema.getTypes().size();
// 	idx_t numAddedColumns = state.targetChunk.ColumnCount() - 1 - alreadyExistingCols;
// 	for(int i = idxToPutRightOnes ; i < chunk.ColumnCount() ; i++) {
// 		chunk.data[i].Reference( input.data[ i-numAddedColumns ] );
// 	}
// 	chunk.SetCardinality( input.size() );

// // icecream::ic.enable();
// // IC(chunk.size());
// // if (chunk.size() != 0)
// // 	IC(chunk.ToString(std::min(10, (int)chunk.size())));
// // icecream::ic.disable();

// 	return OperatorResultType::NEED_MORE_INPUT;
// }

// std::string PhysicalEdgeIdSeek::ParamsToString() const {
// 	return "EdgeIdSeek-param";
// }

// std::string PhysicalEdgeIdSeek::ToString() const {
// 	return "EdgeIdSeek";
// }

// }