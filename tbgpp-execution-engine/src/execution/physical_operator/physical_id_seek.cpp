
#include "typedef.hpp"

#include "execution/physical_operator/physical_id_seek.hpp"
#include "extent/extent_iterator.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"

#include "icecream.hpp"

#include <string>

namespace duckdb {

class IdSeekState : public OperatorState {
public:
	explicit IdSeekState() {
		targetChunkInitialized = false;
		sel.Initialize(STANDARD_VECTOR_SIZE);
	}
public:
	std::queue<ExtentIterator *> ext_its;
	DataChunk targetChunk;	// initialized when first execute() is called
	bool targetChunkInitialized;
	SelectionVector sel;
};

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)), scan_projection_mapping(projection_mapping),
		  filter_pushdown_key_idx(-1) {
			
	D_ASSERT(projection_mapping.size() == 1 ); // 230303

	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	// schema = (original cols, projected cols)
	// 		if (4, 2) => target_types_index starts from: (2+4)-2 = 4
	for (int col_idx = 0; col_idx < this->inner_col_map.size(); col_idx++) {
		target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
		scan_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
	}

	do_filter_pushdown = false;
}

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, vector<unique_ptr<Expression>> predicates)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)), scan_projection_mapping(projection_mapping),
		  filter_pushdown_key_idx(-1) {
			
	D_ASSERT(projection_mapping.size() == 1 ); // 230303

	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	// schema = (original cols, projected cols)
	// 		if (4, 2) => target_types_index starts from: (2+4)-2 = 4
	for (int col_idx = 0; col_idx < this->inner_col_map.size(); col_idx++) {
		target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
		scan_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
	}

	D_ASSERT(predicates.size() > 0);
	if (predicates.size() > 1) {
		auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : predicates) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else {
		expression = move(predicates[0]);
	}

	executor.AddExpression(*expression);

	do_filter_pushdown = false;
	has_expression = true;
}

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, std::vector<duckdb::LogicalType> scan_types,
				   vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)), scan_types(scan_types),
		  scan_projection_mapping(scan_projection_mapping), filter_pushdown_key_idx(filterKeyIndex),
		  filter_pushdown_value(filterValue) { 
			
	D_ASSERT(projection_mapping.size() == 1 ); // 230303

	for (int col_idx = 0; col_idx < this->inner_col_map.size(); col_idx++) {
		target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
	}

	do_filter_pushdown = (filter_pushdown_key_idx >= 0);
}

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
// std::cout << "[PhysicalIdSeek] input" << std::endl;
// IC(input.size(), id_col_idx);
// if (input.size() > 0) {
// 	IC(input.ToString(std::min((idx_t)10, input.size())));
// }
// icecream::ic.disable();

	idx_t nodeColIdx = id_col_idx;
	D_ASSERT(nodeColIdx < input.ColumnCount());
	idx_t output_idx = 0;

	// target_types => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	if (!state.targetChunkInitialized) {
		if (!target_types.empty())
			state.targetChunk.Initialize(target_types, STANDARD_VECTOR_SIZE);
		state.targetChunkInitialized = true;
	}

	// initialize indexseek
	vector<ExtentID> target_eids;		// target extent ids to access
	vector<idx_t> boundary_position;	// boundary position of the input chunk

	context.client->graph_store->InitializeVertexIndexSeek(state.ext_its, oids, scan_projection_mapping, input, nodeColIdx, scan_types, target_eids, boundary_position);
	D_ASSERT(target_eids.size() == boundary_position.size());
	
	vector<idx_t> output_col_idx;
	for (idx_t i = 0; i < inner_col_map.size(); i++) {
		output_col_idx.push_back(inner_col_map[i]);
	}
	if (!do_filter_pushdown) {
		for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
			context.client->graph_store->doVertexIndexSeek(state.ext_its, chunk, input, nodeColIdx, target_types, target_eids, boundary_position, extentIdx, output_col_idx);
		}
		if (has_expression) {
			if (!is_tmp_chunk_initialized) {
				auto input_chunk_type = std::move(input.GetTypes());
				for (idx_t i = 0; i < inner_col_map.size(); i++) {
					input_chunk_type.push_back(chunk.data[inner_col_map[i]].GetType());
				}
				tmp_chunk.InitializeEmpty(input_chunk_type);
				is_tmp_chunk_initialized = true;
			}
			for (idx_t i = 0; i < input.ColumnCount(); i++) {
				tmp_chunk.data[i].Reference(input.data[i]);
			}
			for (idx_t i = 0; i < inner_col_map.size(); i++) {
				tmp_chunk.data[input.ColumnCount() + i].Reference(chunk.data[inner_col_map[i]]);
			}
			executor.SelectExpression(tmp_chunk, state.sel);
		}
	} else {
		for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
			context.client->graph_store->doVertexIndexSeek(state.ext_its, chunk, input, nodeColIdx, target_types, target_eids, boundary_position, extentIdx, 
			output_col_idx, output_idx, state.sel, filter_pushdown_key_idx, filter_pushdown_value);
		}
	}
	// TODO temporary code for deleting the existing iter
	auto ext_it_exist = state.ext_its.front();
	state.ext_its.pop();
	delete ext_it_exist;

	// for original ones reference existing columns
	if (!do_filter_pushdown && !has_expression) {
		D_ASSERT(input.ColumnCount() == outer_col_map.size());
		for (int i = 0; i < input.ColumnCount(); i++) {
			if( outer_col_map[i] != std::numeric_limits<uint32_t>::max() ) {
				D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
				chunk.data[outer_col_map[i]].Reference(input.data[i]);
			}
		}
		chunk.SetCardinality(input.size());
	} else {
		D_ASSERT(input.ColumnCount() == outer_col_map.size());
		for (int i = 0; i < input.ColumnCount(); i++) {
			if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
				D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
				chunk.data[outer_col_map[i]].Slice(input.data[i], state.sel, output_idx);
			}
		}
		chunk.SetCardinality(output_idx);
	}

// icecream::ic.enable();
// std::cout << "[PhysicalIdSeek] output" << std::endl;
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