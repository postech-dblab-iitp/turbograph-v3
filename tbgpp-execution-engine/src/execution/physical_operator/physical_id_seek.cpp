
#include "typedef.hpp"

#include "common/output_util.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "extent/extent_iterator.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"

#include "icecream.hpp"

#include <string>

namespace duckdb {

class IdSeekState : public OperatorState {
   public:
    explicit IdSeekState() { sel.Initialize(STANDARD_VECTOR_SIZE); }

   public:
    std::queue<ExtentIterator *> ext_its;
    SelectionVector sel;
    bool need_initialize_extit = true;
    bool has_remaining_output = false;
    idx_t cur_schema_idx;
    idx_t num_total_schemas;
    vector<SelectionVector> sels;  // TODO do we need this?
};

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<uint32_t> &outer_col_map,
                               vector<uint32_t> &inner_col_map)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(projection_mapping),
      filter_pushdown_key_idx(-1)
{

    // targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
    // schema = (original cols, projected cols)
    // 		if (4, 2) => target_types_index starts from: (2+4)-2 = 4
    this->inner_col_maps.push_back(std::move(inner_col_map));
    this->outer_col_maps.push_back(std::move(outer_col_map));

    scan_types.resize(1);
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        // target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
        scan_types[0].push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }

    D_ASSERT(oids.size() == projection_mapping.size());
    for (auto i = 0; i < oids.size(); i++) {
        ps_oid_to_projection_mapping.insert({oids[i], i});
    }

    do_filter_pushdown = false;
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<vector<uint32_t>> &outer_col_maps,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               vector<vector<duckdb::LogicalType>> scan_types)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_maps(move(outer_col_maps)),
      inner_col_maps(move(inner_col_maps)),
      scan_projection_mapping(scan_projection_mapping),
      filter_pushdown_key_idx(-1),
      scan_types(scan_types)
{
    std::unordered_set<uint32_t> inner_col_set;
    for (auto i = 0; i < this->inner_col_maps.size(); i++) {
        for (auto j = 0; j < this->inner_col_maps[i].size(); j++) {
            inner_col_set.insert(this->inner_col_maps[i][j]);
        }
    }
    union_inner_col_map.insert(union_inner_col_map.end(), inner_col_set.begin(),
                               inner_col_set.end());

    D_ASSERT(oids.size() == projection_mapping.size());
    for (auto i = 0; i < oids.size(); i++) {
        ps_oid_to_projection_mapping.insert({oids[i], i});
    }

    do_filter_pushdown = false;
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<uint32_t> &outer_col_map,
                               vector<uint32_t> &inner_col_map,
                               vector<unique_ptr<Expression>> predicates)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(projection_mapping),
      filter_pushdown_key_idx(-1)
{

    this->inner_col_maps.push_back(std::move(inner_col_map));
    this->outer_col_maps.push_back(std::move(outer_col_map));

    scan_types.resize(1);
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        target_types.push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
        scan_types[0].push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }
    // tmp_chunk.InitializeEmpty(scan_types[0]);

    D_ASSERT(predicates.size() > 0);
    if (predicates.size() > 1) {
        auto conjunction = make_unique<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        for (auto &expr : predicates) {
            conjunction->children.push_back(move(expr));
        }
        expression = move(conjunction);
    }
    else {
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

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<uint32_t> &outer_col_map,
                               vector<uint32_t> &inner_col_map,
                               vector<duckdb::LogicalType> scan_type,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               int64_t filterKeyIndex,
                               duckdb::Value filterValue)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(scan_projection_mapping),
      filter_pushdown_key_idx(filterKeyIndex),
      filter_pushdown_value(filterValue)
{

    this->inner_col_maps.push_back(std::move(inner_col_map));
    this->outer_col_maps.push_back(std::move(outer_col_map));
    this->scan_types.push_back(std::move(scan_type));
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        target_types.push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }

    D_ASSERT(oids.size() == projection_mapping.size());
    for (auto i = 0; i < oids.size(); i++) {
        ps_oid_to_projection_mapping.insert({oids[i], i});
    }

    do_filter_pushdown = (filter_pushdown_key_idx >= 0);
    has_expression = false;
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<uint32_t> &outer_col_map,
                               vector<uint32_t> &inner_col_map,
                               vector<duckdb::LogicalType> scan_type,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               vector<unique_ptr<Expression>> predicates)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(scan_projection_mapping)
{

    this->inner_col_maps.push_back(std::move(inner_col_map));
    this->outer_col_maps.push_back(std::move(outer_col_map));
    this->scan_types.push_back(std::move(scan_type));
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        target_types.push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }
    // tmp_chunk.InitializeEmpty(scan_types[0]);

    D_ASSERT(predicates.size() > 0);
    if (predicates.size() > 1) {
        auto conjunction = make_unique<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        for (auto &expr : predicates) {
            conjunction->children.push_back(move(expr));
        }
        expression = move(conjunction);
    }
    else {
        expression = move(predicates[0]);
    }

    executor.AddExpression(*expression);

    D_ASSERT(oids.size() == projection_mapping.size());
    for (auto i = 0; i < oids.size(); i++) {
        ps_oid_to_projection_mapping.insert({oids[i], i});
    }

    D_ASSERT(projection_mapping.size() == scan_projection_mapping.size());
    tmp_chunk_mapping.resize(scan_projection_mapping.size());
    for (idx_t i = 0; i < tmp_chunk_mapping.size(); i++) {
        idx_t proj_map_idx = 0;
        if (projection_mapping[i].size() == 0)
            continue;
        for (idx_t scanproj_map_idx = 0;
             scanproj_map_idx < scan_projection_mapping[i].size();
             scanproj_map_idx++) {
            if (scan_projection_mapping[i][scanproj_map_idx] ==
                projection_mapping[i][proj_map_idx]) {
                tmp_chunk_mapping[i].push_back(scanproj_map_idx);
                proj_map_idx++;
            }
        }
    }

    do_filter_pushdown = false;
    has_expression = true;
}

unique_ptr<OperatorState> PhysicalIdSeek::GetOperatorState(
    ExecutionContext &context) const
{
    return make_unique<IdSeekState>();
}

OperatorResultType PhysicalIdSeek::Execute(ExecutionContext &context,
                                           DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate) const
{
    if (input.size() == 0) {
        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    D_ASSERT(nodeColIdx < input.ColumnCount());
    idx_t output_idx = 0;

    // initialize indexseek
    vector<ExtentID> target_eids;     // target extent ids to access
    vector<idx_t> boundary_position;  // boundary position of the input chunk
    vector<vector<idx_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;

    context.client->graph_store->InitializeVertexIndexSeek(
        state.ext_its, oids, scan_projection_mapping, input, nodeColIdx,
        scan_types, target_eids, target_seqnos_per_extent,
        ps_oid_to_projection_mapping, mapping_idxs);

    bool do_unionall = false;

    if (do_unionall) {
        doSeekUnionAll(context, input, chunk, state, target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_idx);
    }
    else {
        doSeekSchemaless(context, input, chunk, state, target_eids,
                         target_seqnos_per_extent, mapping_idxs, output_idx);
    }

    // TODO temporary code for deleting the existing iter
    auto ext_it_exist = state.ext_its.front();
    state.ext_its.pop();
    delete ext_it_exist;

    referInputChunk(input, chunk, state, output_idx);

    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalIdSeek::Execute(
    ExecutionContext &context, DataChunk &input,
    vector<unique_ptr<DataChunk>> &chunks, OperatorState &lstate,
    idx_t &output_chunk_idx) const
{
    auto &state = (IdSeekState &)lstate;
    if (input.size() == 0) {
        // D_ASSERT(false);  // not implemented yet
        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(0);
        }
        return OperatorResultType::NEED_MORE_INPUT;
                          // chunk.SetCardinality(0);
                          // return OperatorResultType::NEED_MORE_INPUT;
    }

    idx_t nodeColIdx = id_col_idx;
    D_ASSERT(nodeColIdx < input.ColumnCount());
    idx_t output_idx = 0;

    // initialize indexseek
    vector<ExtentID> target_eids;     // target extent ids to access
    vector<idx_t> boundary_position;  // boundary position of the input chunk
    vector<vector<idx_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;
    vector<idx_t> num_tuples_per_chunk;

    if (state.need_initialize_extit) {
        context.client->graph_store->InitializeVertexIndexSeek(
            state.ext_its, oids, scan_projection_mapping, input, nodeColIdx,
            scan_types, target_eids, target_seqnos_per_extent,
            ps_oid_to_projection_mapping, mapping_idxs);
        state.need_initialize_extit = false;
        state.has_remaining_output = false;
        state.cur_schema_idx = 0;
        num_tuples_per_chunk.resize(mapping_idxs.size(), 0);
        state.sels.clear();
        state.sels.resize(mapping_idxs.size());
        for (auto i = 0; i < state.sels.size(); i++) {
            state.sels[i].Initialize();
        }
    }

    if (!state.has_remaining_output) {
        if (!do_filter_pushdown) {
            // vector<idx_t> invalid_columns = {10, 11, 12, 13, 14, 15};
            // for (auto i = 0; i < invalid_columns.size(); i++) {
            // 	auto &validity = FlatVector::Validity(chunk.data[invalid_columns[i]]);
            // 	validity.SetAllInvalid(input.size());
            // }

            if (has_expression) {
                D_ASSERT(false);  // not implemented yet
                                  // init intermediate chunk
                                  // if (!is_tmp_chunk_initialized) {
                // 	auto input_chunk_type = std::move(input.GetTypes());
                // 	for (idx_t i = 0; i < scan_types[0].size(); i++) { // TODO inner_col_maps[schema_idx]
                // 		input_chunk_type.push_back(scan_types[0][i]);
                // 	}
                // 	tmp_chunk.Initialize(input_chunk_type);
                // 	is_tmp_chunk_initialized = true;
                // } else {
                // 	tmp_chunk.Reset();
                // }

                // // do VertexIdSeek
                // vector<idx_t> output_col_idx;
                // for (idx_t i = 0; i <  scan_types[0].size(); i++) {
                // 	output_col_idx.push_back(input.ColumnCount() + i);
                // }
                // for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {

                // 	// for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
                // 	// 	output_col_idx.push_back(inner_col_maps[mapping_idxs[extentIdx]][i]);
                // 	// }

                // 	context.client->graph_store->doVertexIndexSeek(state.ext_its, tmp_chunk, input, nodeColIdx, target_types,
                // 		target_eids, target_seqnos_per_extent, extentIdx, output_col_idx);
                // }

                // // Refer input chunk & execute expression
                // for (idx_t i = 0; i < input.ColumnCount(); i++) {
                // 	tmp_chunk.data[i].Reference(input.data[i]);
                // }
                // // for (idx_t i = 0; i < inner_col_maps[0].size(); i++) {
                // // 	tmp_chunk.data[input.ColumnCount() + i].Reference(chunk.data[inner_col_maps[0][i]]);
                // // }
                // tmp_chunk.SetCardinality(input.size());
                // output_idx = executor.SelectExpression(tmp_chunk, state.sel);
            }
            else {
                // for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
                // 	for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
                // 		auto &validity = FlatVector::Validity(chunk.data[inner_col_maps[mapping_idxs[extentIdx]][i]]);
                // 		validity.SetAllInvalid(input.size()); // todo optimize this process; if already allinvalid then skip?
                // 	}
                // }

                /**
                 * In InitializeVertexIndexSeek, we sort src vids by target extent id.
                 * So, we can access the same extent id in a row.
                */

                for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                     extentIdx++) {
                    vector<idx_t> output_col_idx;
                    for (idx_t i = 0;
                         i < inner_col_maps[mapping_idxs[extentIdx]].size();
                         i++) {
                        output_col_idx.push_back(
                            inner_col_maps[mapping_idxs[extentIdx]][i]);
                        // TODO we should change this into result sets
                    }
                    context.client->graph_store->doVertexIndexSeek(
                        state.ext_its, *(chunks[mapping_idxs[extentIdx]].get()),
                        input, nodeColIdx, target_types, target_eids,
                        target_seqnos_per_extent, extentIdx, output_col_idx,
                        num_tuples_per_chunk[mapping_idxs[extentIdx]]);

                    /**
                     * Currently, we cannot handle rhs multi-schema case!
                     * I mean, if there is a null column in a schema, we cannot handle
                     * this. We need to fix this, but currently, we just skip this case.
                    */

                    for (auto i = 0;
                         i < target_seqnos_per_extent[extentIdx].size(); i++) {
                        state.sels[mapping_idxs[extentIdx]].set_index(
                            num_tuples_per_chunk[mapping_idxs[extentIdx]] -
                                target_seqnos_per_extent[extentIdx].size() + i,
                            target_seqnos_per_extent[extentIdx][i]);
                    }
                }
                state.has_remaining_output = true;
                for (auto i = 0; i < chunks.size(); i++) {
                    chunks[i]->SetCardinality(num_tuples_per_chunk[i]);
                }
            }
        }
        else {
            D_ASSERT(false);  // not implemented yet
            // for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
            // 	vector<idx_t> output_col_idx;
            // 	for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
            // 		output_col_idx.push_back(inner_col_maps[mapping_idxs[extentIdx]][i]);
            // 	}
            // 	context.client->graph_store->doVertexIndexSeek(state.ext_its, chunk, input, nodeColIdx, target_types,
            // 		target_eids, target_seqnos_per_extent, extentIdx, output_col_idx, output_idx, state.sel, filter_pushdown_key_idx,
            // 		filter_pushdown_value);
            // }
        }
        // TODO temporary code for deleting the existing iter
        auto ext_it_exist = state.ext_its.front();
        state.ext_its.pop();
        delete ext_it_exist;
    }
    else {
        for (auto chunk_idx = state.cur_schema_idx; chunk_idx < chunks.size();
             chunk_idx++) {
            if (chunks[chunk_idx]->size() == 0)
                continue;
            output_chunk_idx = chunk_idx;
            state.cur_schema_idx = chunk_idx + 1;
            return OperatorResultType::HAVE_MORE_OUTPUT;
        }
        state.has_remaining_output = false;
        state.need_initialize_extit = true;
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // for original ones reference existing columns
    if (!do_filter_pushdown && !has_expression) {
        idx_t schema_idx = input.GetSchemaIdx();
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_maps[chunk_idx][i] !=
                    std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_maps[chunk_idx][i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]->data[outer_col_maps[chunk_idx][i]].Slice(
                        input.data[i], state.sels[chunk_idx],
                        num_tuples_per_chunk[chunk_idx]);
                }
            }
        }
#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
        for (auto i = 0; i < chunks.size(); i++) {
            OutputUtil::PrintTop10TuplesInDataChunk(*(chunks[i].get()));
        }
#endif
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            output_chunk_idx = chunk_idx;
            state.cur_schema_idx = chunk_idx + 1;
            return OperatorResultType::HAVE_MORE_OUTPUT;
        }
        return OperatorResultType::NEED_MORE_INPUT;
    }
    else if (do_filter_pushdown && !has_expression) {
        throw NotImplementedException("");
        // idx_t schema_idx = input.GetSchemaIdx();
        // D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
        // for (int i = 0; i < input.ColumnCount(); i++) {
        // 	if (outer_col_maps[schema_idx][i] != std::numeric_limits<uint32_t>::max()) {
        // 		D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
        // 		chunk.data[outer_col_maps[schema_idx][i]].Slice(input.data[i], state.sel, output_idx);
        // 	}
        // }
        // chunk.SetCardinality(output_idx);
    }
    else if (!do_filter_pushdown && has_expression) {
        throw NotImplementedException("");
        // idx_t schema_idx = input.GetSchemaIdx();
        // D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
        // for (int i = 0; i < input.ColumnCount(); i++) {
        // 	if (outer_col_maps[schema_idx][i] != std::numeric_limits<uint32_t>::max()) {
        // 		D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
        // 		chunk.data[outer_col_maps[schema_idx][i]].Slice(input.data[i], state.sel, output_idx);
        // 	}
        // }
        // for (int i = 0; i < inner_col_maps[schema_idx].size(); i++) { // TODO inner_col_maps[schema_idx]
        // 	chunk.data[inner_col_maps[schema_idx][i]].Slice(tmp_chunk.data[input.ColumnCount() + tmp_chunk_mapping[schema_idx][i]], state.sel, output_idx);
        // }
        // chunk.SetCardinality(output_idx);
    }
    else {
        D_ASSERT(false);
    }

    return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalIdSeek::doSeekUnionAll(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate, vector<ExtentID> &target_eids,
    vector<vector<idx_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, idx_t &output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    if (!do_filter_pushdown) {
        // vector<idx_t> invalid_columns = {10, 11, 12, 13, 14, 15};
        // for (auto i = 0; i < invalid_columns.size(); i++) {
        // 	auto &validity = FlatVector::Validity(chunk.data[invalid_columns[i]]);
        // 	validity.SetAllInvalid(input.size());
        // }

        if (has_expression) {
            // init intermediate chunk
            if (!is_tmp_chunk_initialized) {
                auto input_chunk_type = std::move(input.GetTypes());
                for (idx_t i = 0; i < scan_types[0].size();
                     i++) {  // TODO inner_col_maps[schema_idx]
                    input_chunk_type.push_back(scan_types[0][i]);
                }
                tmp_chunk.Initialize(input_chunk_type);
                is_tmp_chunk_initialized = true;
            }
            else {
                tmp_chunk.Reset();
            }

            // do VertexIdSeek
            vector<idx_t> output_col_idx;
            for (idx_t i = 0; i < scan_types[0].size(); i++) {
                output_col_idx.push_back(input.ColumnCount() + i);
            }
            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {

                // for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
                // 	output_col_idx.push_back(inner_col_maps[mapping_idxs[extentIdx]][i]);
                // }

                context.client->graph_store->doVertexIndexSeek(
                    state.ext_its, tmp_chunk, input, nodeColIdx, target_types,
                    target_eids, target_seqnos_per_extent, extentIdx,
                    output_col_idx);
            }

            // Refer input chunk & execute expression
            for (idx_t i = 0; i < input.ColumnCount(); i++) {
                tmp_chunk.data[i].Reference(input.data[i]);
            }
            // for (idx_t i = 0; i < inner_col_maps[0].size(); i++) {
            // 	tmp_chunk.data[input.ColumnCount() + i].Reference(chunk.data[inner_col_maps[0][i]]);
            // }
            tmp_chunk.SetCardinality(input.size());
            output_idx = executor.SelectExpression(tmp_chunk, state.sel);
        }
        else {
            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                for (idx_t i = 0;
                     i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
                    auto &validity = FlatVector::Validity(
                        chunk.data[inner_col_maps[mapping_idxs[extentIdx]][i]]);
                    // todo optimize this process; if already allinvalid then skip?
                    validity.SetAllInvalid(input.size());
                }
            }
            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                vector<idx_t> output_col_idx;
                for (idx_t i = 0;
                     i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
                    output_col_idx.push_back(
                        inner_col_maps[mapping_idxs[extentIdx]][i]);
                    // TODO we should change this into result sets
                }
                context.client->graph_store->doVertexIndexSeek(
                    state.ext_its, chunk, input, nodeColIdx, target_types,
                    target_eids, target_seqnos_per_extent, extentIdx,
                    output_col_idx);
            }
        }
    }
    else {
        for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
             extentIdx++) {
            vector<idx_t> output_col_idx;
            for (idx_t i = 0;
                 i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
                output_col_idx.push_back(
                    inner_col_maps[mapping_idxs[extentIdx]][i]);
            }
            context.client->graph_store->doVertexIndexSeek(
                state.ext_its, chunk, input, nodeColIdx, target_types,
                target_eids, target_seqnos_per_extent, extentIdx,
                output_col_idx, output_idx, state.sel, filter_pushdown_key_idx,
                filter_pushdown_value);
        }
    }
}

void PhysicalIdSeek::doSeekSchemaless(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate, vector<ExtentID> &target_eids,
    vector<vector<idx_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, idx_t &output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    SchemalessDataChunk &schless_chunk = (SchemalessDataChunk &)chunk;

    if (!do_filter_pushdown) {
        if (has_expression) {
            // no filter pushdown but has expression
            throw NotImplementedException("");
        }
        else {
            // no filter pushdown & has no filter expression
            schless_chunk.SetHasRowChunk(true);

            // create rowcol_t column for the row chunk
            schless_chunk.CreateRowCol(union_inner_col_map, input.size());
            Vector &rowcol = schless_chunk.GetRowCol(union_inner_col_map[0]);
            rowcol_t *rowcol_arr = (rowcol_t *)rowcol.GetData();

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                for (idx_t i = 0;
                     i < target_seqnos_per_extent[extentIdx].size(); i++) {
                    // TODO point schema info
                    // Schema *schema_ptr = (Schema *)&context.schema_infos[0];
                    // rowcol_ptr[target_seqnos_per_extent[extentIdx][i]].schema_ptr = (char *)schema_ptr;
                    // rowcol_ptr[target_seqnos_per_extent[extentIdx][i]].offset = schema_ptr->getStoredTypesSize();
                    // TODO temporary code
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]]
                        .schema_ptr = nullptr;
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]].offset =
                        24;
                }
            }

            uint64_t accm_offset = 0;
            for (idx_t i = 0; i < input.size(); i++) {
                accm_offset += rowcol_arr[i].offset;
                rowcol_arr[i].offset = accm_offset;
            }
            schless_chunk.CreateRowMajorStore(accm_offset);

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                context.client->graph_store->doVertexIndexSeek(
                    state.ext_its, chunk, input, nodeColIdx, target_types,
                    target_eids, target_seqnos_per_extent, extentIdx, rowcol,
                    schless_chunk.GetRowMajorStore(union_inner_col_map[0]));
            }
        }
    }
    else {
        // filter pushdown
        throw NotImplementedException("");
    }
}

void PhysicalIdSeek::referInputChunk(DataChunk &input, DataChunk &chunk,
                                     OperatorState &lstate,
                                     idx_t output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    // for original ones reference existing columns
    if (!do_filter_pushdown && !has_expression) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_maps[schema_idx][i] !=
                std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
                chunk.data[outer_col_maps[schema_idx][i]].Reference(
                    input.data[i]);
            }
        }
        chunk.SetCardinality(input.size());
    }
    else if (do_filter_pushdown && !has_expression) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_maps[schema_idx][i] !=
                std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
                chunk.data[outer_col_maps[schema_idx][i]].Slice(
                    input.data[i], state.sel, output_idx);
            }
        }
        chunk.SetCardinality(output_idx);
    }
    else if (!do_filter_pushdown && has_expression) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(input.ColumnCount() == outer_col_maps[schema_idx].size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_maps[schema_idx][i] !=
                std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_maps[schema_idx][i] < chunk.ColumnCount());
                chunk.data[outer_col_maps[schema_idx][i]].Slice(
                    input.data[i], state.sel, output_idx);
            }
        }
        for (int i = 0; i < inner_col_maps[schema_idx].size();
             i++) {  // TODO inner_col_maps[schema_idx]
            chunk.data[inner_col_maps[schema_idx][i]].Slice(
                tmp_chunk.data[input.ColumnCount() +
                               tmp_chunk_mapping[schema_idx][i]],
                state.sel, output_idx);
        }
        chunk.SetCardinality(output_idx);
    }
    else {
        D_ASSERT(false);
    }
}

std::string PhysicalIdSeek::ParamsToString() const
{
    std::string result = "";
    result += "id_col_idx=" + std::to_string(id_col_idx) + ", ";
    result += "projection_mapping.size()=" +
              std::to_string(projection_mapping.size()) + ", ";
    result += "projection_mapping[0].size()=" +
              std::to_string(projection_mapping[0].size()) + ", ";
    result +=
        "target_types.size()=" + std::to_string(target_types.size()) + ", ";
    result +=
        "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
    result +=
        "inner_col_map.size()=" + std::to_string(inner_col_map.size()) + ", ";
    return result;
}

std::string PhysicalIdSeek::ToString() const
{
    return "IdSeek";
}

}  // namespace duckdb