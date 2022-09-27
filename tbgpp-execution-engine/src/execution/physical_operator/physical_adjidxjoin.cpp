
#include "typedef.hpp"

#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "extent/extent_iterator.hpp"
#include "common/types/selection_vector.hpp"
#include "planner/joinside.hpp"

#include <string>
#include "icecream.hpp"

namespace duckdb {

PhysicalAdjIdxJoin::PhysicalAdjIdxJoin(CypherSchema& sch,
	std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, bool load_eid, bool enumerate)
	: PhysicalAdjIdxJoin(sch, srcName, srcLabelSet, edgeLabelSet, expandDir, tgtLabelSet, join_type, move(vector<JoinCondition>()), load_eid, enumerate) { }

PhysicalAdjIdxJoin::PhysicalAdjIdxJoin(CypherSchema& sch,
	std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, vector<JoinCondition> remaining_conditions_p, bool load_eid, bool enumerate)
	: CypherPhysicalOperator(sch), srcName(srcName), srcLabelSet(srcLabelSet), edgeLabelSet(edgeLabelSet), expandDir(expandDir), tgtLabelSet(tgtLabelSet), join_type(join_type), remaining_conditions(move(remaining_conditions_p)), load_eid(load_eid), enumerate(enumerate) {

	// operator rules
	bool check = (enumerate) ? true : (!load_eid);
	D_ASSERT( check && "load_eid should be set to false(=not returning edge ids) when `enumerate` set to `false` (=range)");

	D_ASSERT( enumerate == true && "always enumerate for now");
	D_ASSERT( srcLabelSet.size() == 1 && "src label shuld be assigned and be only one for now");
	D_ASSERT( tgtLabelSet.size() <= 1 && "no multiple targets"); // TODO needs support from the storage
	D_ASSERT( edgeLabelSet.size() <= 1 && "no multiple edges Storage API support needed"); // TODO needs support from the storage
	D_ASSERT( enumerate && "need careful debugging on range mode"); // TODO needs support from the storage

	D_ASSERT( remaining_conditions.size() == 0 && "currently not support additional predicate" );
	
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class AdjIdxJoinState : public OperatorState {
public:
	explicit AdjIdxJoinState() {
		resetForNewInput();
		adj_it = new AdjacencyListIterator();
		rhs_sel.Initialize(STANDARD_VECTOR_SIZE);
		src_nullity.resize(STANDARD_VECTOR_SIZE);
		join_sizes.resize(STANDARD_VECTOR_SIZE);
		total_join_size.resize(STANDARD_VECTOR_SIZE);
	}
	//! Called when starting processing for new chunk
	inline void resetForNewInput() {
		resetForMoreOutput();
		first_fetch = false;
		equi_join_finished = false;
		left_join_finished = false;
		lhs_idx = 0;
		adj_idx = 0;
		rhs_idx = 0;
		left_lhs_idx = 0;
		// init vectors
		// adj_col_idxs.clear();
		// adj_col_types.clear();
		std::fill(total_join_size.begin(), total_join_size.end(), 0);
	}
	inline void resetForMoreOutput() {
		output_idx = 0;
	}
public:
	// operator data
	AdjacencyListIterator *adj_it;
	// initialize rest of operator members
	idx_t srcColIdx;
	idx_t edgeColIdx;
	idx_t tgtColIdx;
	
	// join state - initialized per output
	idx_t output_idx;

	// join state - initialized per new input, and updated while processing
	bool first_fetch;
	idx_t lhs_idx;
	idx_t adj_idx;
	idx_t rhs_idx;
	bool equi_join_finished;	// equi join match finished
	idx_t left_lhs_idx;
	bool left_join_finished;	// when equi_finished and then left finished
	
	// join metadata - initialized per new input
	vector<int> adj_col_idxs;					// indices
	vector<LogicalType> adj_col_types;			// forward_adj or backward_adj

	// join data - initialized per new input
	vector<bool> src_nullity;
	vector<vector<idx_t>> join_sizes;	// can be multiple when there are many adjlists per vertex
	vector<idx_t> total_join_size;		// sum of entries in join_sizes

	SelectionVector rhs_sel;	// used multiple times without initialization
};

unique_ptr<OperatorState> PhysicalAdjIdxJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<AdjIdxJoinState>( );
}

// helper functions
inline ExpandDirection adjListLogicalTypeToExpandDir(LogicalType adjType) {
	return adjType == LogicalType::FORWARD_ADJLIST ? ExpandDirection::OUTGOING : ExpandDirection::INCOMING;
}

inline uint64_t& getIdRefFromVector(Vector& vector, idx_t index) {
	switch( vector.GetVectorType() ) {
		case VectorType::DICTIONARY_VECTOR: {
			return ((uint64_t *)vector.GetData())[DictionaryVector::SelVector(vector).get_index(index)];
		}
		case VectorType::FLAT_VECTOR: {
			return ((uint64_t *)vector.GetData())[index];
		}
		default: {
			D_ASSERT(false);
		}
	}
}

void PhysicalAdjIdxJoin::ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
	
	D_ASSERT(join_type == JoinType::SEMI || join_type == JoinType::ANTI);

	uint64_t* adj_start;
	uint64_t* adj_end;
	uint64_t src_vid;
	Vector& src_vid_column_vector = input.data[state.srcColIdx];	// can be dictionaryvector
	size_t adjlist_size;

	for( ; state.lhs_idx < input.size(); state.lhs_idx++ ) {
		
		uint64_t& src_vid = getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], src_vid, adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]));
		// FIXME debug calculate size directly here
		int adj_size_debug = (adj_end - adj_start)/2;
		const bool predicate_satisfied = (join_type == JoinType::SEMI)
							? ( !state.src_nullity[state.lhs_idx] && adj_size_debug > 0 ) 		// semi join
							: ( state.src_nullity[state.lhs_idx] || adj_size_debug == 0 );		// anti join
		if( predicate_satisfied ) {
			state.rhs_sel.set_index(state.output_idx++, state.lhs_idx);
		}
	}
	// use sel vector and filter lhs to chunk
	// chunk determined. now fill in lhs using slice operation
	for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
		chunk.data[colId].Slice(input.data[colId], state.rhs_sel, state.output_idx);
	}
	D_ASSERT( state.output_idx <= STANDARD_VECTOR_SIZE );
	chunk.SetCardinality(state.output_idx);


}

void PhysicalAdjIdxJoin::GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
	
	// check nullity (debug note : this part has no burden in execution time. verified!)
	switch( input.data[state.srcColIdx].GetVectorType() ) {
		case VectorType::DICTIONARY_VECTOR: {
			const auto &sel_vector = DictionaryVector::SelVector(input.data[state.srcColIdx]);
			const auto &child = DictionaryVector::Child(input.data[state.srcColIdx]);
			for(idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = FlatVector::IsNull( child, sel_vector.get_index(i) );
			}
			break;
		}
		default: {
			for(idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = FlatVector::IsNull(input.data[state.srcColIdx], i);
			}
			break;
		}
	}

	// fill in adjacency col info
// DEBUG 
	if( state.adj_col_idxs.size() == 0 ) {
		context.client->graph_store->getAdjColIdxs(srcLabelSet, edgeLabelSet, expandDir, state.adj_col_idxs, state.adj_col_types);	
	}
	
	// resize join sizes using adj_col_idxs
	auto adjColCnt = state.adj_col_idxs.size();
	for( auto& v: state.join_sizes) {
		v.resize(adjColCnt);
	}
	
	// ask for sizes foreach srcvid
// FIXME commented until replaced by lightweight storage API
	// Vector& src_vid_column_vector = input.data[state.srcColIdx];
	// uint64_t *adj_start; uint64_t *adj_end;
	// size_t adjListSize = adj_end - adj_start;

	// for(idx_t a_i = 0; a_i < state.adj_col_idxs.size(); a_i++) {
	// 	for(idx_t v_i = 0; v_i < input.size(); v_i++) {
	// 		// FIXME need to use different APIs : that only returns size not whoe adjacency
	// 		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[a_i], getIdRefFromVector(src_vid_column_vector, v_i), adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[a_i]));
	// 		adjListSize = (adj_end - adj_start)/2;
	// 		state.join_sizes[v_i][a_i] = adjListSize;
	// 		state.total_join_size[v_i] += adjListSize;
	// 	}
	// }

}

// TODO function handlepredicates
	// x has label blabla
	// x == x
	// x != x
	// x in y

void PhysicalAdjIdxJoin::ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate;
	
	uint64_t* adj_start;
	uint64_t* adj_end;
	uint64_t src_vid;
	Vector& src_vid_column_vector = input.data[state.srcColIdx];	// can be dictionaryvector
	uint64_t *tgt_adj_column = (uint64_t *)chunk.data[state.tgtColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	uint64_t *eid_adj_column = (uint64_t *)chunk.data[state.edgeColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	size_t adjlist_size;

	// iterate source vids
	while( state.output_idx < STANDARD_VECTOR_SIZE && state.lhs_idx < input.size() ) {

		// bypass null src or empty adjs
	// FIXME debug need this!! when src null!!
		// if( state.total_join_size[state.lhs_idx] == 0 ) {
		// 	state.lhs_idx++;
		// 	continue;
		// }
		uint64_t& src_vid = getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
		context.client->graph_store->getAdjListFromVid(*state.adj_it, state.adj_col_idxs[state.adj_idx], src_vid, adj_start, adj_end, adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]));

		// calculate size
	// FIXME debug calculate size directly here
	int adj_size_debug = (adj_end - adj_start)/2;
		const size_t num_rhs_left = adj_size_debug - state.rhs_idx;
		const size_t num_rhs_to_try_fetch = ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left )
										? num_rhs_left : (EXEC_ENGINE_VECTOR_SIZE - state.output_idx);
		// TODO apply filter predicates

		// set sel vector on lhs	// TODO apply filter predicates
		auto tmp_output_idx = state.output_idx;	// do not alter output_idx here
		for( ; tmp_output_idx < state.output_idx + num_rhs_to_try_fetch ; tmp_output_idx++ ) {
			state.rhs_sel.set_index(tmp_output_idx, state.lhs_idx);
		}
		// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
		for( ; state.rhs_idx < num_rhs_to_try_fetch; state.rhs_idx++ ) {
			tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
			if( load_eid ) {
				eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
			}
			state.output_idx++;
		}
		D_ASSERT(tmp_output_idx == state.output_idx);

		// update lhs_idx and adj_idx for next iteration
		if( state.rhs_idx >= state.join_sizes[state.lhs_idx][state.adj_idx] ) {
			// for this (lhs_idx, adj_idx), equi join is done
			state.rhs_idx = 0;
			if( state.adj_idx == state.adj_col_idxs.size() - 1) { state.lhs_idx++; state.adj_idx = 0;}
			else { state.adj_idx++; }
		}
	}
	// chunk determined. now fill in lhs using slice operation
	for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
		chunk.data[colId].Slice(input.data[colId], state.rhs_sel, state.output_idx);
	}
	D_ASSERT( state.output_idx <= STANDARD_VECTOR_SIZE );
	chunk.SetCardinality(state.output_idx);
}

void PhysicalAdjIdxJoin::ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
	// TODO later
	// (here we do not apply predicates as rhs is null)
	// however this logic needs further verification
	// if nonempty predicate,
		// mark leftjoin finished

	// use left_lhs_idx
	// for empty lefts, produce lhs, null

}


OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (AdjIdxJoinState &)lstate; 
 

	if( !state.first_fetch ) {
		// values used while processing
		state.srcColIdx = schema.getColIdxOfKey(srcName);
		state.edgeColIdx = input.ColumnCount();
		state.tgtColIdx = input.ColumnCount() + int(load_eid);
		// Get join matches (sizes) for the LHS. Initialized one time per LHS
		GetJoinMatches(context, input, lstate);
		state.first_fetch = true;
	}

	if( join_type == JoinType::SEMI || join_type == JoinType::ANTI ) {
		// these joins can be processed in single call. explicitly process them and return
		ProcessSemiAntiJoin(context, input, chunk, lstate);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	const bool isProcessingTerminated =
		state.equi_join_finished && (join_type != JoinType::LEFT || state.left_join_finished);
	if ( isProcessingTerminated ) {	
		// initialize state for next chunk
		state.resetForNewInput();

		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		if( ! state.equi_join_finished ) {
			ProcessEquiJoin(context, input, chunk, lstate);
			// update states
			state.resetForMoreOutput();
			if( state.lhs_idx >= input.size() ) {
				state.equi_join_finished = true;
			}
			return OperatorResultType::HAVE_MORE_OUTPUT;
		} else {
			D_ASSERT( ! state.left_join_finished );
			ProcessLeftJoin(context, input, chunk, lstate);
			// update states
			state.resetForMoreOutput();
			if( state.left_lhs_idx >= input.size() ) {
				state.left_join_finished = true;
			}
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalAdjIdxJoin::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
icecream::ic.enable();
	if( schema.getCypherType(srcName) == CypherValueType::RANGE ) {
		D_ASSERT( false && "currently not supporting when range is given as input");
		return ExecuteRangedInput(context, input, chunk, lstate);
	} else {
		return ExecuteNaiveInput(context, input, chunk, lstate);
	}
icecream::ic.disable();
}

std::string PhysicalAdjIdxJoin::ParamsToString() const {
	return "AdjIdxJoin-params-TODO";
}

std::string PhysicalAdjIdxJoin::ToString() const {
	return "AdjIdxJoin";
}


}
