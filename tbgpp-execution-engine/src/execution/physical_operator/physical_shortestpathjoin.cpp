
#include "execution/physical_operator/physical_shortestpathjoin.hpp"
#include "typedef.hpp"

namespace duckdb {

class ShortestPathState : public OperatorState {
public:
	explicit ShortestPathState() {
		srtp_iter = new ShortestPathIterator();
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
    ShortestPathIterator *srtp_iter;

	idx_t input_idx;
	idx_t output_idx;

	bool is_initialized;

	// These are setted only once.
	// Due to const constraint of Execute function, we cannot set these in Execute function.
	vector<int> adj_col_idxs;
	vector<LogicalType> adj_col_types;	
};

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
		context.client->graph_store->getAdjColIdxs((idx_t)adjidx_obj_id, srtp_state.adj_col_idxs, srtp_state.adj_col_types);
		srtp_state.is_initialized = true;
		D_ASSERT(srtp_state.adj_col_idxs.size() == 1);
		D_ASSERT(srtp_state.adj_col_types.size() == 1);
		D_ASSERT(srtp_state.adj_col_types[0] == LogicalType::FORWARD_ADJLIST);
	}

	uint64_t *src_id_column = (uint64_t *)input.data[src_id_idx].GetData();
	uint64_t *dst_id_column = (uint64_t *)input.data[dst_id_idx].GetData();

	while (srtp_state.input_idx < input.size()) {
		uint64_t src_id = src_id_column[srtp_state.input_idx];
		uint64_t dst_id = dst_id_column[srtp_state.input_idx];
		std::vector<uint64_t> edges;
		std::vector<uint64_t> nodes;
		srtp_state.srtp_iter->initialize(*context.client, src_id, dst_id, srtp_state.adj_col_idxs[0]);
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
