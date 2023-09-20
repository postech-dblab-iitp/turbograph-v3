
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
		sel.Initialize(STANDARD_VECTOR_SIZE);
	}
public:
	std::queue<ExtentIterator *> ext_its;
	SelectionVector sel;
};

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  scan_projection_mapping(projection_mapping), filter_pushdown_key_idx(-1) {

	// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
	// schema = (original cols, projected cols)
	// 		if (4, 2) => target_types_index starts from: (2+4)-2 = 4
	this->inner_col_maps.push_back(std::move(inner_col_map));
	this->outer_col_maps.push_back(std::move(outer_col_map));
	
	scan_types.resize(1);
	for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
		// target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
		scan_types[0].push_back(sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
	}

	D_ASSERT(oids.size() == projection_mapping.size());
	for (auto i = 0; i < oids.size(); i++) {
		ps_oid_to_projection_mapping.insert({oids[i], i});
	}

	do_filter_pushdown = false;
}

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<vector<uint32_t>> &outer_col_maps, vector<vector<uint32_t>> &inner_col_maps, vector<vector<uint64_t>> scan_projection_mapping)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  outer_col_maps(move(outer_col_maps)), inner_col_maps(move(inner_col_maps)), scan_projection_mapping(scan_projection_mapping),
		  filter_pushdown_key_idx(-1) {
	scan_types.resize(this->inner_col_maps.size());
	for (auto i = 0; i < this->inner_col_maps.size(); i++) {
		for (int col_idx = 0; col_idx < this->inner_col_maps[i].size(); col_idx++) {
			// target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
			scan_types[i].push_back(sch.getStoredTypes()[this->inner_col_maps[i][col_idx]]);
		}
	}

	D_ASSERT(oids.size() == projection_mapping.size());
	for (auto i = 0; i < oids.size(); i++) {
		ps_oid_to_projection_mapping.insert({oids[i], i});
	}

	do_filter_pushdown = false;
}

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, vector<unique_ptr<Expression>> predicates)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  scan_projection_mapping(projection_mapping), filter_pushdown_key_idx(-1) {

	this->inner_col_maps.push_back(std::move(inner_col_map));
	this->outer_col_maps.push_back(std::move(outer_col_map));

	scan_types.resize(1);
	for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
		target_types.push_back(sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
		scan_types[0].push_back(sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
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

	D_ASSERT(oids.size() == projection_mapping.size());
	for (auto i = 0; i < oids.size(); i++) {
		ps_oid_to_projection_mapping.insert({oids[i], i});
	}

	do_filter_pushdown = false;
	has_expression = true;
}

PhysicalIdSeek::PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, std::vector<duckdb::LogicalType> scan_type,
				   vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue)
		: CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  scan_projection_mapping(scan_projection_mapping), filter_pushdown_key_idx(filterKeyIndex), filter_pushdown_value(filterValue) {
	
	this->inner_col_maps.push_back(std::move(inner_col_map));
	this->outer_col_maps.push_back(std::move(outer_col_map));
	this->scan_types.push_back(std::move(scan_type));
	for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
		target_types.push_back(sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
	}

	D_ASSERT(oids.size() == projection_mapping.size());
	for (auto i = 0; i < oids.size(); i++) {
		ps_oid_to_projection_mapping.insert({oids[i], i});
	}

	do_filter_pushdown = (filter_pushdown_key_idx >= 0);
	has_expression = false;
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
// std::cout << "[PhysicalIdSeek] input schema idx: " << input.GetSchemaIdx() << 
// 		", output schema idx: " << chunk.GetSchemaIdx() << std::endl;
// IC(input.size(), id_col_idx, do_filter_pushdown, has_expression);
// IC(inner_col_map);
// if (input.size() > 0) {
// 	IC(input.ToString(std::min((idx_t)10, input.size())));
// }
// icecream::ic.disable();

	idx_t nodeColIdx = id_col_idx;
	D_ASSERT(nodeColIdx < input.ColumnCount());
	idx_t output_idx = 0;

	// initialize indexseek
	vector<ExtentID> target_eids;		// target extent ids to access
	vector<idx_t> boundary_position;	// boundary position of the input chunk
	vector<vector<idx_t>> target_seqnos_per_extent;
	vector<idx_t> mapping_idxs;

	context.client->graph_store->InitializeVertexIndexSeek(state.ext_its, oids, scan_projection_mapping, input, 
		nodeColIdx, scan_types, target_eids, target_seqnos_per_extent, ps_oid_to_projection_mapping, mapping_idxs);
	
	if (!do_filter_pushdown) {
		// vector<idx_t> invalid_columns = {10, 11, 12, 13, 14, 15};
		// for (auto i = 0; i < invalid_columns.size(); i++) {
		// 	auto &validity = FlatVector::Validity(chunk.data[invalid_columns[i]]);
		// 	validity.SetAllInvalid(input.size());
		// }
		
		for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
			vector<idx_t> output_col_idx;
			for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
				output_col_idx.push_back(inner_col_maps[mapping_idxs[extentIdx]][i]);
			}
			context.client->graph_store->doVertexIndexSeek(state.ext_its, chunk, input, nodeColIdx, target_types, 
				target_eids, target_seqnos_per_extent, extentIdx, output_col_idx);
		}
		if (has_expression) {
			if (!is_tmp_chunk_initialized) {
				auto input_chunk_type = std::move(input.GetTypes());
				for (idx_t i = 0; i < inner_col_maps[0].size(); i++) { // TODO inner_col_maps[schema_idx]
					input_chunk_type.push_back(chunk.data[inner_col_maps[0][i]].GetType());
				}
				tmp_chunk.InitializeEmpty(input_chunk_type);
				is_tmp_chunk_initialized = true;
			}
			for (idx_t i = 0; i < input.ColumnCount(); i++) {
				tmp_chunk.data[i].Reference(input.data[i]);
			}
			for (idx_t i = 0; i < inner_col_maps[0].size(); i++) {
				tmp_chunk.data[input.ColumnCount() + i].Reference(chunk.data[inner_col_maps[0][i]]);
			}
			tmp_chunk.SetCardinality(input.size());
			output_idx = executor.SelectExpression(tmp_chunk, state.sel);
		}
	} else {
		for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
			vector<idx_t> output_col_idx;
			for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
				output_col_idx.push_back(inner_col_maps[mapping_idxs[extentIdx]][i]);
			}
			context.client->graph_store->doVertexIndexSeek(state.ext_its, chunk, input, nodeColIdx, target_types, 
				target_eids, target_seqnos_per_extent, extentIdx, output_col_idx, output_idx, state.sel, filter_pushdown_key_idx, 
				filter_pushdown_value);
		}
	}
	// TODO temporary code for deleting the existing iter
	auto ext_it_exist = state.ext_its.front();
	state.ext_its.pop();
	delete ext_it_exist;

	// for original ones reference existing columns
	if (!do_filter_pushdown && !has_expression) {
		idx_t schema_idx = input.GetSchemaIdx();
		D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
		for (int i = 0; i < input.ColumnCount(); i++) {
			if (outer_col_maps[schema_idx][i] != std::numeric_limits<uint32_t>::max()) {
				D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
				chunk.data[outer_col_maps[schema_idx][i]].Reference(input.data[i]);
			}
		}
		chunk.SetCardinality(input.size());
	} else if (do_filter_pushdown && !has_expression) {
		idx_t schema_idx = input.GetSchemaIdx();
		D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
		for (int i = 0; i < input.ColumnCount(); i++) {
			if (outer_col_maps[schema_idx][i] != std::numeric_limits<uint32_t>::max()) {
				D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
				chunk.data[outer_col_maps[schema_idx][i]].Slice(input.data[i], state.sel, output_idx);
			}
		}
		chunk.SetCardinality(output_idx);
	} else if (!do_filter_pushdown && has_expression) {
		idx_t schema_idx = input.GetSchemaIdx();
		D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
		for (int i = 0; i < input.ColumnCount(); i++) {
			if (outer_col_maps[schema_idx][i] != std::numeric_limits<uint32_t>::max()) {
				D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
				chunk.data[outer_col_maps[schema_idx][i]].Slice(input.data[i], state.sel, output_idx);
			}
		}
		for (int i = 0; i < inner_col_maps[schema_idx].size(); i++) { // TODO inner_col_maps[schema_idx]
			chunk.data[inner_col_maps[schema_idx][i]].Slice(chunk.data[inner_col_maps[schema_idx][i]], state.sel, output_idx);
		}
		chunk.SetCardinality(output_idx);
	} else {
		D_ASSERT(false);
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
	result += "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
	result += "inner_col_map.size()=" + std::to_string(inner_col_map.size()) + ", ";
	return result;	
}

std::string PhysicalIdSeek::ToString() const {
	return "IdSeek";
}

}