
#include "execution/physical_operator/physical_shortestpathjoin.hpp"
#include "common/typedef.hpp"
#include "common/output_util.hpp"

namespace duckdb {

class ShortestPathState : public OperatorState {
public:
	explicit ShortestPathState() {
		srtp_iter = new ShortestPathAdvancedIterator();
		input_idx = 0;
		output_idx = 0;
		is_initialized = false;
	 }

	 ~ShortestPathState()
	 {
		 delete srtp_iter;
	 }

	 void resetForMoreInput() {
		input_idx = 0;
		output_idx = 0;
	 }

	 void resetForMoreOutput() {
		output_idx = 0;
	 }
public:
    ShortestPathAdvancedIterator *srtp_iter;

	idx_t input_idx;
	idx_t output_idx;

	bool is_initialized;

	// These are setted only once.
	// Due to const constraint of Execute function, we cannot set these in Execute function.
	vector<int> adj_col_idxs;
	vector<LogicalType> adj_col_types;	
};

// TODO remove all getIdRefFromVector
inline uint64_t &getIdRefFromVector(Vector &vector, idx_t index)
{
    switch (vector.GetVectorType()) {
        case VectorType::DICTIONARY_VECTOR: {
            return ((uint64_t *)vector.GetData())
                [DictionaryVector::SelVector(vector).get_index(index)];
        }
        case VectorType::FLAT_VECTOR: {
            return ((uint64_t *)vector.GetData())[index];
        }
        case VectorType::CONSTANT_VECTOR: {
            return ((uint64_t *)ConstantVector::GetData<uintptr_t>(vector))[0];
        }
        default: {
            D_ASSERT(false);
        }
    }
}

//===--------------------------------------------------------------------===//
// Execute
//===--------------------------------------------------------------------===//

unique_ptr<OperatorState> PhysicalShortestPathJoin::GetOperatorState(
    ExecutionContext &context) const
{
    return make_unique<ShortestPathState>();
}

OperatorResultType PhysicalShortestPathJoin::Execute(ExecutionContext &context,
                                                     DataChunk &input,
                                                     DataChunk &chunk,
                                                     OperatorState &state) const
{
	auto &srtp_state = (ShortestPathState &)state;
	if(!srtp_state.is_initialized) {
		context.client->graph_storage_wrapper->getAdjColIdxs((idx_t)adjidx_obj_id_fwd, srtp_state.adj_col_idxs, srtp_state.adj_col_types);
		context.client->graph_storage_wrapper->getAdjColIdxs((idx_t)adjidx_obj_id_bwd, srtp_state.adj_col_idxs, srtp_state.adj_col_types);
		srtp_state.is_initialized = true;
		D_ASSERT(srtp_state.adj_col_idxs.size() == 2);
		D_ASSERT(srtp_state.adj_col_types.size() == 2);
		D_ASSERT(srtp_state.adj_col_types[0] == LogicalType::FORWARD_ADJLIST);
		D_ASSERT(srtp_state.adj_col_types[1] == LogicalType::BACKWARD_ADJLIST);
	}

	Vector &src_id_vec = input.data[src_id_idx];
	Vector &dst_id_vec = input.data[dst_id_idx];

	while (srtp_state.input_idx < input.size()) {
		uint64_t src_id = getIdRefFromVector(src_id_vec, srtp_state.input_idx);
		uint64_t dst_id = getIdRefFromVector(dst_id_vec, srtp_state.input_idx);
		std::vector<uint64_t> edges;
		std::vector<uint64_t> nodes;
		srtp_state.srtp_iter->initialize(*context.client, src_id, dst_id, srtp_state.adj_col_idxs[0], srtp_state.adj_col_idxs[1], 
										lower_bound, upper_bound);
		bool found = srtp_state.srtp_iter->getShortestPath(*context.client, edges, nodes);
		if(found) {
			D_ASSERT(edges.size() == nodes.size() - 1);
			std::vector<Value> path_vec(edges.size() + nodes.size());
			// node-edge-node-edge-...-node
			for (size_t i = 0; i < nodes.size(); i++) {
				path_vec[i * 2] = Value::UBIGINT(nodes[i]);
			}
			for (size_t i = 0; i < edges.size(); i++) {
				path_vec[i * 2 + 1] = Value::UBIGINT(edges[i]);
			}
			Value path_val = Value::LIST(path_vec);
			chunk.data[output_idx].SetValue(srtp_state.output_idx, path_val);
			srtp_state.output_idx++;
		}
		srtp_state.input_idx++;
	}

	D_ASSERT(input.ColumnCount() == input_col_map.size());
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		if (input_col_map[i] == std::numeric_limits<uint32_t>::max()) continue;
		chunk.data[input_col_map[i]].Reference(input.data[i]);
	}

	chunk.SetCardinality(srtp_state.output_idx);
	srtp_state.resetForMoreInput();
	return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// ETC
//===--------------------------------------------------------------------===//

std::string PhysicalShortestPathJoin::ParamsToString() const
{
    std::string result = "";
    result +=
        "input_col_map.size()=" + std::to_string(input_col_map.size()) + ", ";
    result +=
        "output_idx=" + std::to_string(output_idx) + ", ";
    result += "adjidx_obj_id=" + std::to_string(adjidx_obj_id) + ", ";
    result += "src_id_idx=" + std::to_string(src_id_idx) + ", ";
    result += "dst_id_idx=" + std::to_string(dst_id_idx);
    return result;
}

std::string PhysicalShortestPathJoin::ToString() const
{
    return "ShortestPath";
}

}  // namespace duckdb
