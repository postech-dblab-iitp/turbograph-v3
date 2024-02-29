#include "execution/physical_operator/physical_cross_product.hpp"

#include "common/vector_operations/vector_operations.hpp"

namespace duckdb {

PhysicalCrossProduct::PhysicalCrossProduct(Schema &sch, vector<uint32_t> &outer_col_map_p, vector<uint32_t> &inner_col_map_p)
    : CypherPhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT, sch), outer_col_map(move(outer_col_map_p)), inner_col_map(move(inner_col_map_p)) {

	// the inputs of PhysicalCrossProduct must be used.
	for(auto& it: outer_col_map) { D_ASSERT(it != std::numeric_limits<uint32_t>::max()); }
	for(auto& it: inner_col_map) { D_ASSERT(it != std::numeric_limits<uint32_t>::max()); }
	D_ASSERT(sch.getStoredTypes().size() == outer_col_map.size() + inner_col_map.size());
}

string PhysicalCrossProduct::ParamsToString() const {
	std::string result = "";
	result += "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
	result += "inner_col_map.size()=" + std::to_string(inner_col_map.size()) + ", ";
	return result;
}
std::string PhysicalCrossProduct::ToString() const {
	return "CrossProduct";
}


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// class CrossProductGlobalState : public GlobalSinkState {
// public:
// 	CrossProductGlobalState() {
// 	}

// 	ChunkCollection rhs_materialized;
// 	mutex rhs_lock;
// };

// unique_ptr<GlobalSinkState> PhysicalCrossProduct::GetGlobalSinkState(ClientContext &context) const {
// 	return make_unique<CrossProductGlobalState>();
// }

// SinkResultType PhysicalCrossProduct::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
//                                           DataChunk &input) const {
// 	auto &sink = (CrossProductGlobalState &)state;
// 	lock_guard<mutex> client_guard(sink.rhs_lock);
// 	sink.rhs_materialized.Append(input);
// 	return SinkResultType::NEED_MORE_INPUT;
// }

class CrossProductLocalState : public LocalSinkState {
public:
	CrossProductLocalState() {
	}
	ChunkCollection rhs_materialized;
	mutex rhs_lock;
};

unique_ptr<LocalSinkState> PhysicalCrossProduct::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<CrossProductLocalState>();
}

SinkResultType PhysicalCrossProduct::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate_p) const {
	auto &sink = (CrossProductLocalState &)lstate_p;
	lock_guard<mutex> client_guard(sink.rhs_lock);
	sink.rhs_materialized.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

DataChunk &PhysicalCrossProduct::GetLastSinkedData(LocalSinkState &lstate) const {
	auto &sink = (CrossProductLocalState &)lstate;
	return sink.rhs_materialized.GetChunk(sink.rhs_materialized.ChunkCount() - 1);
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class CrossProductOperatorState : public OperatorState {
public:
	CrossProductOperatorState() : right_position(0) {
	}

	idx_t right_position;
};

unique_ptr<OperatorState> PhysicalCrossProduct::GetOperatorState(ExecutionContext &context) const {
	return make_unique<CrossProductOperatorState>();
}

OperatorResultType PhysicalCrossProduct::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                 OperatorState &state_p, LocalSinkState &sink_state_p) const {
	auto &state = (CrossProductOperatorState &)state_p;
	auto &sink_state = (CrossProductLocalState &)sink_state_p;
	auto &right_collection = sink_state.rhs_materialized;

	if (right_collection.Count() == 0) {
		// no RHS: empty result
		return OperatorResultType::FINISHED;
	}
	if (state.right_position >= right_collection.Count()) {
		// ran out of entries on the RHS
		// reset the RHS and move to the next chunk on the LHS
		state.right_position = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &left_chunk = input;
	// now match the current vector of the left relation with the current row
	// from the right relation
	chunk.SetCardinality(left_chunk.size());
	// create a reference to the vectors of the left column
	// for (idx_t i = 0; i < left_chunk.ColumnCount(); i++) {
	// 	chunk.data[outer_col_map[i]].Reference(left_chunk.data[i]);
	// }
	for (idx_t i = 0; i < outer_col_map.size(); i++) { // TODO check correctness
		chunk.data[outer_col_map[i]].Reference(left_chunk.data[i]);
	}
	// duplicate the values on the right side
	auto &right_chunk = right_collection.GetChunkForRow(state.right_position);
	auto row_in_chunk = state.right_position % STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < inner_col_map.size(); i++) { // TODO check correctness
		ConstantVector::Reference(chunk.data[inner_col_map[i]], right_chunk.data[i], row_in_chunk,
		                          right_chunk.size());
	}
	// for (idx_t i = 0; i < right_collection.ColumnCount(); i++) {
	// 	ConstantVector::Reference(chunk.data[inner_col_map[i]], right_chunk.data[i], row_in_chunk,
	// 	                          right_chunk.size());
	// }

	// for the next iteration, move to the next position on the right side
	state.right_position++;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
