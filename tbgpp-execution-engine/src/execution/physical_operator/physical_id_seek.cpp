
#include "typedef.hpp"

// catalog related
#include "catalog/catalog.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

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
    explicit IdSeekState(ClientContext& client, vector<uint64_t> oids) { 
        sel.Initialize(STANDARD_VECTOR_SIZE); 
        io_cache.io_buf_ptrs_cache.resize(INITIAL_EXTENT_ID_SPACE);
        io_cache.io_buf_sizes_cache.resize(INITIAL_EXTENT_ID_SPACE);
        io_cache.io_cdf_ids_cache.resize(INITIAL_EXTENT_ID_SPACE);
        io_cache.num_tuples_cache.resize(INITIAL_EXTENT_ID_SPACE);
        eid_to_schema_idx.resize(INITIAL_EXTENT_ID_SPACE, -1);
    }

   public:
    std::queue<ExtentIterator *> ext_its;
    SelectionVector sel;
    bool need_initialize_extit = true;
    bool has_remaining_output = false;
    idx_t cur_schema_idx;
    idx_t num_total_schemas;
    vector<SelectionVector> sels;  // TODO do we need this?
    vector<idx_t> eid_to_schema_idx;
    IOCache io_cache;
};

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<vector<uint32_t>> &outer_col_maps,
                               vector<vector<uint32_t>> &inner_col_maps)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_maps(move(outer_col_maps)),
      inner_col_maps(move(inner_col_maps)),
      scan_projection_mapping(projection_mapping),
      filter_pushdown_key_idx(-1)
{
    num_total_schemas =
        this->outer_col_maps.size() * this->inner_col_maps.size();

    scan_types.resize(1);
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        // target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
        scan_types[0].push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }

    D_ASSERT(oids.size() == projection_mapping.size());

    do_filter_pushdown = false;

    generatePartialSchemaInfos();
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<vector<uint32_t>> &outer_col_maps,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<uint32_t> &union_inner_col_map,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               vector<vector<duckdb::LogicalType>> scan_types,
                               bool is_output_union_schema)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_maps(move(outer_col_maps)),
      inner_col_maps(move(inner_col_maps)),
      union_inner_col_map(move(union_inner_col_map)),
      scan_projection_mapping(scan_projection_mapping),
      filter_pushdown_key_idx(-1),
      scan_types(scan_types),
      is_output_union_schema(is_output_union_schema)
{
    D_ASSERT(oids.size() == projection_mapping.size());
    num_total_schemas =
        this->outer_col_maps.size() * this->inner_col_maps.size();

    do_filter_pushdown = false;

    generatePartialSchemaInfos();
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<vector<uint32_t>> &outer_col_maps,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<unique_ptr<Expression>> predicates)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_maps(move(outer_col_maps)),
      inner_col_maps(move(inner_col_maps)),
      scan_projection_mapping(projection_mapping),
      filter_pushdown_key_idx(-1)
{
    num_total_schemas =
        this->outer_col_maps.size() * this->inner_col_maps.size();

    scan_types.resize(1);
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        target_types.push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
        scan_types[0].push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }

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

    do_filter_pushdown = false;
    has_unpushdowned_expressions = true;

    generatePartialSchemaInfos();
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<vector<uint32_t>> &outer_col_maps,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<duckdb::LogicalType> scan_type,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               int64_t filterKeyIndex,
                               duckdb::Value filterValue)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_maps(move(outer_col_maps)),
      inner_col_maps(move(inner_col_maps)),
      scan_projection_mapping(scan_projection_mapping),
      filter_pushdown_key_idx(filterKeyIndex),
      filter_pushdown_value(filterValue)
{
    this->scan_types.push_back(std::move(scan_type));
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        target_types.push_back(
            sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
    }
    num_total_schemas =
        this->outer_col_maps.size() * this->inner_col_maps.size();

    D_ASSERT(oids.size() == projection_mapping.size());

    do_filter_pushdown = (filter_pushdown_key_idx >= 0);
    has_unpushdowned_expressions = false;

    generatePartialSchemaInfos();
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<vector<uint32_t>> &outer_col_maps,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<uint32_t> &union_inner_col_map,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               vector<vector<duckdb::LogicalType>> scan_types,
                               vector<unique_ptr<Expression>> &predicates,
                               bool is_output_union_schema)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      scan_types(scan_types),
      projection_mapping(projection_mapping),
      outer_col_maps(move(outer_col_maps)),
      inner_col_maps(move(inner_col_maps)),
      union_inner_col_map(move(union_inner_col_map)),
      scan_projection_mapping(scan_projection_mapping),
      is_output_union_schema(is_output_union_schema)
{
    /**
     * I think target_types is not needed anymore.
     * TODO: remove this.
    */
    for (int col_idx = 0; col_idx < this->inner_col_maps[0].size(); col_idx++) {
        if (this->inner_col_maps[0][col_idx] < sch.getStoredTypes().size()) {
            target_types.push_back(
                sch.getStoredTypes()[this->inner_col_maps[0][col_idx]]);
        }
    }
    num_total_schemas =
        this->outer_col_maps.size() * this->inner_col_maps.size();

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
    has_unpushdowned_expressions = true;
    for (auto i = 0; i < num_total_schemas; i++) {
        tmp_chunks.push_back(std::make_unique<DataChunk>());
    }
    is_tmp_chunk_initialized_per_schema.resize(num_total_schemas, false);

    generatePartialSchemaInfos();
}

unique_ptr<OperatorState> PhysicalIdSeek::GetOperatorState(
    ExecutionContext &context) const
{
    auto state =  make_unique<IdSeekState>(*(context.client), oids);
    context.client->graph_store->fillEidToMappingIdx(oids, state->eid_to_schema_idx);
    return state;
}

void PhysicalIdSeek::InitializeOutputChunks(
    std::vector<unique_ptr<DataChunk>> &output_chunks, Schema &output_schema,
    idx_t idx)
{
    idx_t inner_idx = idx % inner_col_maps.size();
    D_ASSERT(inner_idx < inner_col_maps.size());

    // auto opOutputChunk = std::make_unique<SchemalessDataChunk>();
    auto opOutputChunk = std::make_unique<DataChunk>();
    opOutputChunk->Initialize(output_schema.getStoredTypes());

    if (!is_output_union_schema) {
        for (auto i = 0; i < union_inner_col_map.size(); i++) {
            if (union_inner_col_map[i] < opOutputChunk->ColumnCount()) {
                opOutputChunk->data[union_inner_col_map[i]].SetIsValid(false);
            }
        }
        for (auto i = 0; i < inner_col_maps[inner_idx].size(); i++) {
            if (inner_col_maps[inner_idx][i] < opOutputChunk->ColumnCount()) {
                opOutputChunk->data[inner_col_maps[inner_idx][i]].SetIsValid(
                    true);
            }
        }
    }
    output_chunks.push_back(std::move(opOutputChunk));
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
        mapping_idxs, state.eid_to_schema_idx, &state.io_cache);

    // TODO
    bool do_unionall = true;

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
        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(0);
        }
        return OperatorResultType::OUTPUT_EMPTY;
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
            mapping_idxs, state.eid_to_schema_idx, &state.io_cache);
        state.need_initialize_extit = false;
        state.has_remaining_output = false;
        state.cur_schema_idx = 0;
        num_tuples_per_chunk.resize(num_total_schemas, 0);
        // TODO seems inefficient
        state.sels.clear();
        state.sels.resize(chunks.size());
        for (auto i = 0; i < state.sels.size(); i++) {
            state.sels[i].Initialize();
        }
        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetSchemaIdx(i);
            chunks[i]->Reset();
        }
    }

    /**
     * TODO: cannot handle filter-only-column case.
    */
    if (!state.has_remaining_output) {
        if (!do_filter_pushdown) {
            if (has_unpushdowned_expressions) {
                D_ASSERT(chunks.size() == 1); // TODO handling multi-schema case
                for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                     extentIdx++) {
                    // get chunk index
                    idx_t chunk_idx =
                        input.GetSchemaIdx() * this->inner_col_maps.size() +
                        mapping_idxs[extentIdx];

                    // init intermediate chunk
                    auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
                    if (!is_tmp_chunk_initialized_per_schema[chunk_idx]) {
                        vector<LogicalType> tmp_chunk_type;
                        auto lhs_type = input.GetTypes();
                        getOutputTypesForFilteredSeek(
                            lhs_type, scan_types[mapping_idxs[extentIdx]],
                            tmp_chunk_type);
                        tmp_chunk.Initialize(tmp_chunk_type);
                        is_tmp_chunk_initialized_per_schema[chunk_idx] = true;
                    }
                    else {
                        tmp_chunk.Reset();
                    }

                    // Get output col idx
                    vector<idx_t> output_col_idx;
                    getOutputIdxsForFilteredSeek(chunk_idx, output_col_idx);
                    // do VertexIdSeek
                    // TODO in schemaless case, we need to change this API carefully. it should cover both cases
                    context.client->graph_store->doVertexIndexSeek(
                        state.ext_its, tmp_chunk, input, nodeColIdx,
                        target_types, target_eids, target_seqnos_per_extent,
                        extentIdx, output_col_idx);
                    num_tuples_per_chunk[chunk_idx] += target_seqnos_per_extent[extentIdx].size();
                }

                for (auto chunk_idx = 0; chunk_idx < chunks.size();
                     chunk_idx++) {
                    auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
                    tmp_chunk.SetCardinality(num_tuples_per_chunk[chunk_idx]);

                    // Filter may have column on lhs. Make tmp_chunk reference it
                    for (int i = 0; i < input.ColumnCount(); i++) {
                        tmp_chunk.data[i].Reference(input.data[i]);
                    }

                    // Execute filter (note. this is not efficient if no filter pred on inner cols)
                    num_tuples_per_chunk[chunk_idx] = executor.SelectExpression(
                        tmp_chunk, state.sels[chunk_idx]);
                }

                state.has_remaining_output = true;
            }
            else {
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
                    idx_t chunk_idx =
                        input.GetSchemaIdx() * this->inner_col_maps.size() +
                        mapping_idxs[extentIdx];
                    context.client->graph_store->doVertexIndexSeek(
                        state.ext_its, *(chunks[chunk_idx].get()), input,
                        nodeColIdx, target_types, target_eids,
                        target_seqnos_per_extent, extentIdx, output_col_idx,
                        num_tuples_per_chunk[chunk_idx]);

                    /**
                     * Currently, we cannot handle rhs multi-schema case!
                     * I mean, if there is a null column in a schema, we cannot handle
                     * this. We need to fix this, but currently, we just skip this case.
                    */

                    for (auto i = 0;
                         i < target_seqnos_per_extent[extentIdx].size(); i++) {
                        state.sels[chunk_idx].set_index(
                            num_tuples_per_chunk[chunk_idx] -
                                target_seqnos_per_extent[extentIdx].size() + i,
                            target_seqnos_per_extent[extentIdx][i]);
                    }
                }

                state.has_remaining_output = true;
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
        return OperatorResultType::OUTPUT_EMPTY;
    }

    // for original ones reference existing columns
    if (!do_filter_pushdown && !has_unpushdowned_expressions) {
        idx_t schema_idx = input.GetSchemaIdx();
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            auto outer_col_maps_idx = chunk_idx / inner_col_maps.size();
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_maps[outer_col_maps_idx][i] !=
                    std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_maps[outer_col_maps_idx][i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]
                        ->data[outer_col_maps[outer_col_maps_idx][i]]
                        .Slice(input.data[i], state.sels[chunk_idx],
                               num_tuples_per_chunk[chunk_idx]);
                }
            }
        }

        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(num_tuples_per_chunk[i]);
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
        state.has_remaining_output = false;
        state.need_initialize_extit = true;
        return OperatorResultType::NEED_MORE_INPUT;
    }
    else if (do_filter_pushdown && !has_unpushdowned_expressions) {
        throw NotImplementedException(
            "PhysicalIdSeek-Refer do_filter_pushdown && "
            "!has_unpushdowned_expressions");
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
    else if (!do_filter_pushdown && has_unpushdowned_expressions) {
        idx_t schema_idx = input.GetSchemaIdx();

        // outer columns
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            /**
             * Logic for calculating outer_col_maps_idx using chunk_idx and inner_col_maps.size().
             * See, we generate schemas by cartesian product of outer_col_maps and inner_col_maps.
             * We number schemas from 0 to num_total_schemas - 1.
             * So, we can calculate outer_col_maps_idx using chunk_idx and inner_col_maps.size().
            */
            auto outer_col_maps_idx = chunk_idx / inner_col_maps.size();
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_maps[outer_col_maps_idx][i] !=
                    std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_maps[outer_col_maps_idx][i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]
                        ->data[outer_col_maps[outer_col_maps_idx][i]]
                        .Slice(input.data[i], state.sels[chunk_idx],
                               num_tuples_per_chunk[chunk_idx]);
                }
            }
        }

        // inner columnms
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            auto inner_col_maps_idx = chunk_idx % inner_col_maps.size();
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < inner_col_maps[inner_col_maps_idx].size();
                 i++) {
                // Else case means  the filter-only column case.
                if (inner_col_maps[inner_col_maps_idx][i] <
                    chunks[chunk_idx]->ColumnCount()) {
                    D_ASSERT(inner_col_maps[inner_col_maps_idx][i] <
                             chunks[chunk_idx]->ColumnCount());
                    auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
                    chunks[chunk_idx]
                        ->data[inner_col_maps[inner_col_maps_idx][i]]
                        .Slice(tmp_chunk.data[i + input.ColumnCount()],
                               state.sels[chunk_idx],
                               num_tuples_per_chunk[chunk_idx]);
                }
            }
        }

        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(num_tuples_per_chunk[i]);
        }

        if (chunks.size() == 1) {
            state.has_remaining_output = false;
            state.need_initialize_extit = true;
            return OperatorResultType::NEED_MORE_INPUT;
        } else {
            for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
                if (num_tuples_per_chunk[chunk_idx] == 0)
                    continue;
                output_chunk_idx = chunk_idx;
                state.cur_schema_idx = chunk_idx + 1;
                return OperatorResultType::HAVE_MORE_OUTPUT;
            }
            state.has_remaining_output = false;
            state.need_initialize_extit = true;
            return OperatorResultType::NEED_MORE_INPUT;
        }
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

        if (has_unpushdowned_expressions) {
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
    // SchemalessDataChunk &schless_chunk = (SchemalessDataChunk &)chunk;

    if (!do_filter_pushdown) {
        if (has_unpushdowned_expressions) {
            // no filter pushdown but has expression
            throw NotImplementedException(
                "PhysicalIdSeek !do_filter_pushdown && "
                "has_unpushdowned_expressions");
        }
        else {
            // no filter pushdown & has no filter expression
            // schless_chunk.SetHasRowChunk(true);
            chunk.SetHasRowChunk(true);

            // create rowcol_t column for the row chunk
            // schless_chunk.CreateRowCol(union_inner_col_map, input.size());
            chunk.InitializeRowColumn(union_inner_col_map, input.size());
            // Vector &rowcol = schless_chunk.GetRowCol(union_inner_col_map[0]);
            Vector &rowcol = chunk.data[union_inner_col_map[0]];
            rowcol_t *rowcol_arr = (rowcol_t *)rowcol.GetData();

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                for (idx_t i = 0;
                     i < target_seqnos_per_extent[extentIdx].size(); i++) {
                    PartialSchema *schema_ptr =
                        (PartialSchema
                             *)(&partial_schemas[mapping_idxs[extentIdx]]);
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]]
                        .schema_ptr = (char *)schema_ptr;
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]].offset =
                        schema_ptr->getStoredTypesSize();
                }
            }

            uint64_t accm_offset = 0;
            for (idx_t i = 0; i < input.size(); i++) {
                idx_t total_types_size = rowcol_arr[i].offset;
                rowcol_arr[i].offset = accm_offset;
                accm_offset += total_types_size;
            }
            chunk.CreateRowMajorStore(union_inner_col_map, accm_offset);

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                context.client->graph_store->doVertexIndexSeek(
                    state.ext_its, chunk, input, nodeColIdx, target_types,
                    target_eids, target_seqnos_per_extent, extentIdx, rowcol,
                    chunk.GetRowMajorStore(union_inner_col_map[0]));
            }
        }
    }
    else {
        // filter pushdown
        throw NotImplementedException("PhysicalIdSeek do_filter_pushdown");
    }
}

void PhysicalIdSeek::referInputChunk(DataChunk &input, DataChunk &chunk,
                                     OperatorState &lstate,
                                     idx_t output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    // for original ones reference existing columns
    if (!do_filter_pushdown && !has_unpushdowned_expressions) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(schema_idx < outer_col_maps.size());
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
    else if (do_filter_pushdown && !has_unpushdowned_expressions) {
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
    else if (!do_filter_pushdown && has_unpushdowned_expressions) {
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

void PhysicalIdSeek::generatePartialSchemaInfos()
{
    auto &union_types = this->schema.getStoredTypesRef();
    if (union_inner_col_map.size() == 0) {
        for (auto i = 0; i < inner_col_maps[0].size(); i++) {
            union_inner_col_map.push_back(inner_col_maps[0][i]);
        }
    }
    for (auto i = 0; i < inner_col_maps.size(); i++) {
        uint64_t accumulated_offset = 0;
        partial_schemas.push_back(PartialSchema());
        partial_schemas[i].offset_info.resize(union_inner_col_map.size(), -1);

        for (auto j = 0; j < inner_col_maps[i].size(); j++) {
            // TODO check if inefficient
            if (inner_col_maps[i][j] == std::numeric_limits<uint32_t>::max()) // this case is not handled well, please fix this
                continue;
            auto it =
                std::find(union_inner_col_map.begin(),
                          union_inner_col_map.end(), inner_col_maps[i][j]);
            auto pos = it - union_inner_col_map.begin();
            partial_schemas[i].offset_info[pos] = accumulated_offset;
            accumulated_offset +=
                GetTypeIdSize(union_types[inner_col_maps[i][j]].InternalType());
        }
        partial_schemas[i].stored_types_size = accumulated_offset;
    }
}

void PhysicalIdSeek::getOutputTypesForFilteredSeek(
    vector<LogicalType> &lhs_type, vector<LogicalType> &scan_type,
    vector<LogicalType> &out_type) const
{
    out_type = lhs_type;
    for (auto i = 0; i < scan_type.size(); i++) {
        out_type.push_back(scan_type[i]);
    }
}

void PhysicalIdSeek::getOutputIdxsForFilteredSeek(
    idx_t chunk_idx, vector<idx_t> &output_col_idx) const
{
    /**
     * In filtered seek, we do seek on outer cols + inner cols chunk
     * without any projection.
     * However, output_col_idx is based on the output cols, which is
     * the result of the projection.
     * Therefore, we need to postprocess output_col_idx.
     * 
     * Strong assumption: inner cols are appended to the outer cols in the output.
    */
    auto outer_col_maps_idx = chunk_idx / inner_col_maps.size();
    auto inner_col_maps_idx = chunk_idx % inner_col_maps.size();
    auto outer_size = outer_col_maps[outer_col_maps_idx].size();
    auto inner_size = inner_col_maps[inner_col_maps_idx].size();
    for (idx_t i = 0; i < inner_size; i++) {
        output_col_idx.push_back(i + outer_size);
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
    result += "inner_col_map.size()=" + std::to_string(inner_col_map.size());
    if (expression != nullptr) {
        result += ", expression=" + expression->ToString();
    }
    return result;
}

std::string PhysicalIdSeek::ToString() const
{
    return "IdSeek";
}

}  // namespace duckdb