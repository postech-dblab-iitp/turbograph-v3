
#include <algorithm>
#include <cmath>
#include <numeric>
#include <queue>
#include "typedef.hpp"

// catalog related
#include "catalog/catalog.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

#include <string>
#include "common/output_util.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "extent/extent_iterator.hpp"
#include "icecream.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"

static bool unionall_forced = false;

namespace s62 {

class IdSeekState : public OperatorState {
   public:
    explicit IdSeekState(ClientContext &client, vector<uint64_t> oids)
    {
        seqno_to_eid_idx.resize(STANDARD_VECTOR_SIZE, -1);
        io_cache.io_buf_ptrs_cache.resize(INITIAL_EXTENT_ID_SPACE);
        io_cache.io_buf_sizes_cache.resize(INITIAL_EXTENT_ID_SPACE);
        io_cache.io_cdf_ids_cache.resize(INITIAL_EXTENT_ID_SPACE);
        io_cache.num_tuples_cache.resize(INITIAL_EXTENT_ID_SPACE);
        eid_to_schema_idx.resize(INITIAL_EXTENT_ID_SPACE, -1);
        ext_it = new ExtentIterator(&io_cache);
    }

    void InitializeSels(size_t num_schemas)
    {
        if (sels.size() == num_schemas) {
            return;
        }

        sels.resize(num_schemas);
        filter_sels.resize(num_schemas);
        for (auto i = 0; i < num_schemas; i++) {
            sels[i].Initialize();
            filter_sels[i].Initialize();
        }
    }

   public:
    ExtentIterator *ext_it;
    bool need_initialize_extit = true;
    bool has_remaining_output = false;
    idx_t cur_schema_idx;
    vector<idx_t> null_tuples_idx;
    vector<idx_t> eid_to_schema_idx;
    vector<idx_t> seqno_to_eid_idx;

    vector<SelectionVector> sels;
    vector<SelectionVector> filter_sels;

    IOCache io_cache;
};

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<uint32_t> &outer_col_map,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<uint32_t> &union_inner_col_map,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               vector<vector<s62::LogicalType>> scan_types,
                               bool force_output_union, JoinType join_type)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_map(outer_col_map),
      inner_col_maps(move(inner_col_maps)),
      union_inner_col_map(move(union_inner_col_map)),
      scan_projection_mapping(scan_projection_mapping),
      scan_types(scan_types),
      force_output_union(force_output_union),
      join_type(join_type)
{
    D_ASSERT(oids.size() == projection_mapping.size());
    num_outer_schemas = 1;
    num_inner_schemas = this->inner_col_maps.size();
    num_total_schemas = num_outer_schemas * num_inner_schemas;
    D_ASSERT(num_total_schemas > 0);

    do_filter_pushdown = false;

    genNonPredColIdxs();
    generatePartialSchemaInfos();
    getUnionScanTypes();
    generateOutputColIdxsForOuter();
    generateOutputColIdxsForInner();

    target_eids.reserve(INITIAL_EXTENT_ID_SPACE);
    num_tuples_per_schema.resize(num_total_schemas, 0);
}

PhysicalIdSeek::PhysicalIdSeek(
    Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<uint32_t> &outer_col_map, vector<vector<uint32_t>> &inner_col_maps,
    vector<uint32_t> &union_inner_col_map,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<vector<s62::LogicalType>> scan_types,
    vector<vector<unique_ptr<Expression>>> &predicates,
    vector<vector<idx_t>> &pred_col_idxs_per_schema, bool force_output_union,
    JoinType join_type)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      scan_types(scan_types),
      projection_mapping(projection_mapping),
      outer_col_map(outer_col_map),
      inner_col_maps(move(inner_col_maps)),
      union_inner_col_map(move(union_inner_col_map)),
      scan_projection_mapping(scan_projection_mapping),
      force_output_union(force_output_union),
      pred_col_idxs_per_schema(pred_col_idxs_per_schema),
      join_type(join_type)
{
    D_ASSERT(oids.size() == projection_mapping.size());
    D_ASSERT(oids.size() == projection_mapping.size());
    num_outer_schemas = 1;
    num_inner_schemas = this->inner_col_maps.size();
    num_total_schemas = num_outer_schemas * num_inner_schemas;
    D_ASSERT(num_total_schemas > 0);

    buildExpressionExecutors(predicates);

    // Filter settings
    do_filter_pushdown = true;
    for (auto i = 0; i < num_total_schemas; i++) {
        tmp_chunks.push_back(std::make_unique<DataChunk>());
    }
    is_tmp_chunk_initialized_per_schema.resize(num_total_schemas, false);

    genNonPredColIdxs();
    generatePartialSchemaInfos();
    getUnionScanTypes();
    generateOutputColIdxsForOuter();
    generateOutputColIdxsForInner();

    target_eids.reserve(INITIAL_EXTENT_ID_SPACE);
    num_tuples_per_schema.resize(num_total_schemas, 0);
}

unique_ptr<OperatorState> PhysicalIdSeek::GetOperatorState(
    ExecutionContext &context) const
{
    auto state = make_unique<IdSeekState>(*(context.client), oids);
    context.client->graph_store->fillEidToMappingIdx(oids,
                                                     state->eid_to_schema_idx);
    return state;
}

OperatorResultType PhysicalIdSeek::Execute(ExecutionContext &context,
                                           DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate) const
{
    if (join_type == JoinType::INNER) {
        return ExecuteInner(context, input, chunk, lstate);
    }
    else if (join_type == JoinType::LEFT) {
        return ExecuteLeft(context, input, chunk, lstate);
    }
    else {
        throw NotImplementedException("PhysicalIdSeek-Execute");
    }
}

OperatorResultType PhysicalIdSeek::ExecuteInner(ExecutionContext &context,
                                                DataChunk &input,
                                                DataChunk &chunk,
                                                OperatorState &lstate) const
{
    if (input.size() == 0) {
        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    D_ASSERT(nodeColIdx < input.ColumnCount());

    // initialize indexseek
    vector<vector<uint32_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;
    idx_t output_size = 0;

    if (state.need_initialize_extit) {
        initializeSeek(context, input, chunk, state, nodeColIdx, target_eids,
                       target_seqnos_per_extent, mapping_idxs);

        if (target_eids.size() == 0) {
            chunk.SetCardinality(0);
            state.has_remaining_output = false;
            state.need_initialize_extit = true;
            return OperatorResultType::OUTPUT_EMPTY;
        }
    }

    // Calculate Format
    auto total_nulls = calculateTotalNulls(
        chunk, target_eids, target_seqnos_per_extent, mapping_idxs);
    fillOutSizePerSchema(target_eids, target_seqnos_per_extent, mapping_idxs);
    auto format = determineFormatByCostModel(false, total_nulls);

    if (format == OutputFormat::ROW) {
        doSeekSchemaless(context, input, chunk, state, target_eids,
                         target_seqnos_per_extent, mapping_idxs, output_size);
    }
    else if (format == OutputFormat::UNIONALL) {
        doSeekUnionAll(context, input, chunk, state, target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_size);
        markInvalidForUnseekedValues(chunk, state, target_eids,
                                      target_seqnos_per_extent, mapping_idxs);
    }

    nullifyValuesForPrunedExtents(chunk, state, target_eids.size(),
                                  target_seqnos_per_extent);
    return referInputChunk(input, chunk, state, output_size);
}

OperatorResultType PhysicalIdSeek::ExecuteLeft(ExecutionContext &context,
                                               DataChunk &input,
                                               DataChunk &chunk,
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
    vector<vector<uint32_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;

    context.client->graph_store->InitializeVertexIndexSeek(
        state.ext_it, scan_projection_mapping, input, nodeColIdx, scan_types,
        target_eids, target_seqnos_per_extent, mapping_idxs,
        state.null_tuples_idx, state.eid_to_schema_idx, &state.io_cache);

    fillSeqnoToEIDIdx(target_seqnos_per_extent, state.seqno_to_eid_idx);

    bool do_unionall = true;

    if (do_unionall) {
        doSeekUnionAll(context, input, chunk, state, target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_idx);
    }
    else {
        doSeekSchemaless(context, input, chunk, state, target_eids,
                         target_seqnos_per_extent, mapping_idxs, output_idx);
    }

    return referInputChunkLeft(input, chunk, state, output_idx);
}

OperatorResultType PhysicalIdSeek::Execute(
    ExecutionContext &context, DataChunk &input,
    vector<unique_ptr<DataChunk>> &chunks, OperatorState &lstate,
    idx_t &output_chunk_idx) const
{
    D_ASSERT(false);  // deprecated
    if (join_type == JoinType::INNER) {
        return ExecuteInner(context, input, chunks, lstate, output_chunk_idx);
    }
    else if (join_type == JoinType::LEFT) {
        return ExecuteLeft(context, input, chunks, lstate, output_chunk_idx);
    }
    else {
        throw NotImplementedException("PhysicalIdSeek-Execute");
    }
}

OperatorResultType PhysicalIdSeek::ExecuteInner(
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

    // initialize indexseek
    vector<vector<uint32_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;
    vector<idx_t> num_tuples_per_chunk;

    if (state.need_initialize_extit) {
        initializeSeek(context, input, chunks, state, nodeColIdx, target_eids,
                       target_seqnos_per_extent, mapping_idxs,
                       num_tuples_per_chunk);
    }

    if (target_eids.size() == 0) {
        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(0);
        }
        return OperatorResultType::OUTPUT_EMPTY;
    }

    if (state.has_remaining_output) {
        return moveToNextOutputChunk(chunks, lstate, output_chunk_idx);
    }

    // Calculate Format
    const idx_t UNIFIED_CHUNK_IDX = 0;
    auto &unified_chunk = *(chunks[UNIFIED_CHUNK_IDX].get());
    auto total_nulls = calculateTotalNulls(
        unified_chunk, target_eids, target_seqnos_per_extent, mapping_idxs);
    fillOutSizePerSchema(target_eids, target_seqnos_per_extent, mapping_idxs);
    auto format = determineFormatByCostModel(false, total_nulls);

    // Format-based Execution
    if (format == OutputFormat::GROUPING) {
        doSeekGrouping(context, input, chunks, state, nodeColIdx, target_eids,
                       target_seqnos_per_extent, mapping_idxs,
                       num_tuples_per_chunk);

        return referInputChunks(input, chunks, state, num_tuples_per_chunk,
                                output_chunk_idx);
    }
    else {
        output_chunk_idx = UNIFIED_CHUNK_IDX;

        if (format == OutputFormat::ROW) {
            doSeekSchemaless(context, input, unified_chunk, state, target_eids,
                             target_seqnos_per_extent, mapping_idxs,
                             num_tuples_per_chunk[output_chunk_idx]);
        }
        else {
            doSeekUnionAll(context, input, unified_chunk, state, target_eids,
                           target_seqnos_per_extent, mapping_idxs,
                           num_tuples_per_chunk[output_chunk_idx]);
            markInvalidForUnseekedValues(unified_chunk, state, target_eids,
                                          target_seqnos_per_extent,
                                          mapping_idxs);
        }
        return referInputChunk(input, unified_chunk, state,
                               num_tuples_per_chunk[output_chunk_idx]);
    }
}

OperatorResultType PhysicalIdSeek::ExecuteLeft(
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

    // initialize indexseek
    vector<vector<uint32_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;
    vector<idx_t> num_tuples_per_chunk;

    if (state.need_initialize_extit) {
        initializeSeek(context, input, chunks, state, nodeColIdx, target_eids,
                       target_seqnos_per_extent, mapping_idxs,
                       num_tuples_per_chunk);
    }

    /**
     * TODO: cannot handle filter-only-column case.
    */

    if (state.has_remaining_output) {
        return moveToNextOutputChunk(chunks, lstate, output_chunk_idx);
    }

    doSeekGrouping(context, input, chunks, state, nodeColIdx, target_eids,
                   target_seqnos_per_extent, mapping_idxs,
                   num_tuples_per_chunk);
    return referInputChunksLeft(input, chunks, state, num_tuples_per_chunk,
                                output_chunk_idx);
}

void PhysicalIdSeek::initializeSeek(
    ExecutionContext &context, DataChunk &input,
    vector<unique_ptr<DataChunk>> &chunks, IdSeekState &state, idx_t nodeColIdx,
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, vector<idx_t> &num_tuples_per_chunk) const
{
    state.null_tuples_idx.clear();
    context.client->graph_store->InitializeVertexIndexSeek(
        state.ext_it, scan_projection_mapping, input, nodeColIdx, scan_types,
        target_eids, target_seqnos_per_extent, mapping_idxs,
        state.null_tuples_idx, state.eid_to_schema_idx, &state.io_cache);
    state.need_initialize_extit = false;
    state.has_remaining_output = false;
    state.cur_schema_idx = 0;
    num_tuples_per_chunk.resize(num_total_schemas, 0);
    state.InitializeSels(chunks.size());
    for (auto i = 0; i < chunks.size(); i++) {
        chunks[i]->SetSchemaIdx(i);
        chunks[i]->Reset();
    }
    fillSeqnoToEIDIdx(target_eids.size(), target_seqnos_per_extent, state.seqno_to_eid_idx);
    markInvalidForColumnsToUnseek(input, target_eids, mapping_idxs);
}

void PhysicalIdSeek::initializeSeek(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    IdSeekState &state, idx_t nodeColIdx, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    state.null_tuples_idx.clear();
    context.client->graph_store->InitializeVertexIndexSeek(
        state.ext_it, scan_projection_mapping, input, nodeColIdx, scan_types,
        target_eids, target_seqnos_per_extent, mapping_idxs,
        state.null_tuples_idx, state.eid_to_schema_idx, &state.io_cache);
    state.need_initialize_extit = false;
    state.has_remaining_output = false;
    state.cur_schema_idx = 0;
    state.InitializeSels(1);
    chunk.SetSchemaIdx(input.GetSchemaIdx());
    chunk.Reset();
    fillSeqnoToEIDIdx(target_eids.size(), target_seqnos_per_extent, state.seqno_to_eid_idx);
    markInvalidForColumnsToUnseek(input, target_eids, mapping_idxs);
}

void PhysicalIdSeek::InitializeOutputChunks(
    std::vector<unique_ptr<DataChunk>> &output_chunks, Schema &output_schema,
    idx_t idx)
{
    idx_t inner_idx = idx % inner_col_maps.size();
    D_ASSERT(inner_idx < inner_col_maps.size());

    auto opOutputChunk = std::make_unique<DataChunk>();
    opOutputChunk->Initialize(output_schema.getStoredTypes());

    if (!force_output_union) {
        for (auto i = 0; i < union_inner_col_map.size(); i++) {
            if (union_inner_col_map[i] < opOutputChunk->ColumnCount()) {
                opOutputChunk->data[union_inner_col_map[i]].SetIsValid(true);
            }
        }
    }
    output_chunks.push_back(std::move(opOutputChunk));
}

void PhysicalIdSeek::doSeekUnionAll(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, idx_t &output_size) const
{
    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    if (!do_filter_pushdown) {
        // Special handling for OPTIONAL MATCH (ALL NULL Case)
        if (target_eids.size() == 0) {
            for (auto inner_col_idx: union_inner_col_map) {
                chunk.data[inner_col_idx].SetIsValid(false);
            }
            return;
        }
        for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
             extentIdx++) {
            const vector<uint32_t> &output_col_idx =
                inner_output_col_idxs[mapping_idxs[extentIdx]];
            auto &non_pred_col_idxs =
                non_pred_col_idxs_per_schema[mapping_idxs[extentIdx]];
            context.client->graph_store->doVertexIndexSeek(
                state.ext_it, chunk, input, nodeColIdx, target_eids,
                target_seqnos_per_extent, non_pred_col_idxs, extentIdx,
                output_col_idx);
        }
        output_size = input.size();
    }
    else {
        // Assume single schema
        idx_t chunk_idx = input.GetSchemaIdx();
        auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
        vector<vector<uint32_t>> chunk_idx_to_output_cols_idx(1);
        getOutputIdxsForFilteredSeek(chunk_idx,
                                     chunk_idx_to_output_cols_idx[0]);

        if (is_tmp_chunk_initialized_per_schema[chunk_idx]) {
            tmp_chunk.Reset();
        }

        for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
             extentIdx++) {
            // init intermediate chunk
            if (!is_tmp_chunk_initialized_per_schema[chunk_idx]) {
                vector<LogicalType> tmp_chunk_type;
                auto lhs_type = input.GetTypes();
                getOutputTypesForFilteredSeek(
                    lhs_type, scan_types[mapping_idxs[extentIdx]],
                    tmp_chunk_type);
                tmp_chunk.Initialize(tmp_chunk_type);
                is_tmp_chunk_initialized_per_schema[chunk_idx] = true;
            }

            // Get output col idx
            const auto &output_col_idx = chunk_idx_to_output_cols_idx[0];
            auto &pred_col_idxs =
                pred_col_idxs_per_schema[mapping_idxs[extentIdx]];
            // do VertexIdSeek (but only scan cols used in filter)
            context.client->graph_store->doVertexIndexSeek(
                state.ext_it, tmp_chunk, input, nodeColIdx, target_eids,
                target_seqnos_per_extent, pred_col_idxs, extentIdx,
                output_col_idx);
        }

        // Filter may have column on lhs. Make tmp_chunk reference it
        for (int i = 0; i < input.ColumnCount(); i++) {
            tmp_chunk.data[i].Reference(input.data[i]);
        }
        tmp_chunk.SetCardinality(input.size());

        output_size = executors[0].SelectExpression(tmp_chunk, state.sels[0]);

        // Scan for remaining columns
        state.ext_it->Rewind();  // temporary code for rewind
        auto &non_pred_col_idxs = non_pred_col_idxs_per_schema[0];
        if (non_pred_col_idxs.size() > 0) {
            vector<vector<uint32_t>> target_seqnos_per_extent_after_filter;
            getFilteredTargetSeqno(state.seqno_to_eid_idx,
                                   target_seqnos_per_extent.size(),
                                   state.sels[0].data(), output_size,
                                   target_seqnos_per_extent_after_filter);
            // Perform actual scan
            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                idx_t chunk_idx =
                    input.GetSchemaIdx() * this->inner_col_maps.size() +
                    mapping_idxs[extentIdx];

                auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
                auto &output_col_idx = chunk_idx_to_output_cols_idx[0];
                context.client->graph_store->doVertexIndexSeek(
                    state.ext_it, tmp_chunk, input, nodeColIdx, target_eids,
                    target_seqnos_per_extent_after_filter, non_pred_col_idxs,
                    extentIdx, output_col_idx);
            }
        }
    }
}

void PhysicalIdSeek::doSeekSchemaless(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, idx_t &output_idx) const
{
    D_ASSERT(target_eids.size() > 0);
    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;

    if (target_eids.size() == 0) throw NotImplementedException("doSeekSchemaless No TargetEIDs");

    if (!do_filter_pushdown) {
        if (union_inner_col_map_wo_id.size() == 0) {
            doSeekUnionAll(context, input, chunk, state, target_eids,
                           target_seqnos_per_extent, mapping_idxs, output_idx);
        }
        else {
            chunk.SetHasRowChunk(true);
            // create rowcol_t column for the row chunk

            chunk.InitializeRowColumn(union_inner_col_map_wo_id, input.size());
            Vector &rowcol = chunk.data[union_inner_col_map_wo_id[0]];
            rowcol_t *rowcol_arr = (rowcol_t *)rowcol.GetData();

            // For valid seqnos
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

            // For pruned seqnos
            for (u_int64_t extentIdx = target_eids.size(); extentIdx < target_seqnos_per_extent.size();
                 extentIdx++) {
                for (idx_t i = 0;
                     i < target_seqnos_per_extent[extentIdx].size(); i++) {
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]]
                        .schema_ptr = (char *)&EMPTY_PARTIAL_SCHEMA;
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]].offset = 0;
                }
            }

            uint64_t accm_offset = 0;
            for (idx_t i = 0; i < input.size(); i++) {
                idx_t total_types_size = rowcol_arr[i].offset;
                rowcol_arr[i].offset = accm_offset;
                accm_offset += total_types_size;
            }
            try {
                chunk.CreateRowMajorStore(union_inner_col_map_wo_id, accm_offset);
            }
            catch (const std::exception &e) {
                std::cerr << e.what() << '\n';
            }

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                // Note: Row store is shared accross all column
                const vector<uint32_t> &output_col_idx =
                    inner_output_col_idxs[mapping_idxs[extentIdx]];
                context.client->graph_store->doVertexIndexSeek(
                    state.ext_it, chunk, input, nodeColIdx, target_eids,
                    target_seqnos_per_extent, extentIdx, out_id_col_idx, rowcol,
                    chunk.GetRowMajorStore(union_inner_col_map_wo_id[0]),
                    output_col_idx, output_idx);
            }
        }
    }
    else {
        throw NotImplementedException(
            "PhysicalIdSeek do_filter_pushdown in row format");
    }
}

void PhysicalIdSeek::doSeekGrouping(
    ExecutionContext &context, DataChunk &input,
    vector<unique_ptr<DataChunk>> &chunks, IdSeekState &state, idx_t nodeColIdx,
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, vector<idx_t> &num_tuples_per_chunk) const
{
    D_ASSERT(false);  
}

OperatorResultType PhysicalIdSeek::referInputChunk(DataChunk &input,
                                                   DataChunk &chunk,
                                                   OperatorState &lstate,
                                                   idx_t output_size) const
{
    auto &state = (IdSeekState &)lstate;
    // for original ones reference existing columns
    if (!do_filter_pushdown) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(schema_idx < num_outer_schemas);
        D_ASSERT(input.ColumnCount() == outer_col_map.size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Reference(input.data[i]);
            }
        }
        chunk.SetCardinality(input.size());
    }
    else {
        idx_t schema_idx = input.GetSchemaIdx();
        auto &tmp_chunk = *(tmp_chunks[schema_idx].get());
        D_ASSERT(input.ColumnCount() == outer_col_map.size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Slice(input.data[i], state.sels[0],
                                                   output_size);
            }
        }
        // Slice filter columns (note that those columns are appened to the outer cols)
        for (int i = 0; i < inner_col_maps[0].size(); i++) {
            if (inner_col_maps[0][i] != std::numeric_limits<uint32_t>::max()) {
                chunk.data[inner_col_maps[0][i]].Slice(
                    tmp_chunk.data[i + input.ColumnCount()], state.sels[0],
                    output_size);
            }
        }
        chunk.SetCardinality(output_size);
    }

    state.has_remaining_output = false;
    state.need_initialize_extit = true;

    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalIdSeek::referInputChunkLeft(DataChunk &input,
                                                       DataChunk &chunk,
                                                       OperatorState &lstate,
                                                       idx_t output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    // for original ones reference existing columns
    if (!do_filter_pushdown) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(schema_idx < num_outer_schemas);
        D_ASSERT(input.ColumnCount() == outer_col_map.size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Reference(input.data[i]);
            }
        }

        for (int i = 0; i < inner_col_maps[schema_idx].size(); i++) {
            // Else case means  the filter-only column case.
            if (inner_col_maps[schema_idx][i] < chunk.ColumnCount()) {
                D_ASSERT(
                    chunk.data[inner_col_maps[schema_idx][i]].GetVectorType() ==
                    VectorType::FLAT_VECTOR);
                auto &validity = FlatVector::Validity(
                    chunk.data[inner_col_maps[schema_idx][i]]);
                D_ASSERT(inner_col_maps[schema_idx][i] < chunk.ColumnCount());
                for (auto j = 0; j < state.null_tuples_idx.size(); j++) {
                    validity.SetInvalid(state.null_tuples_idx[j]);
                }
            }
        }
        chunk.SetCardinality(input.size());
    }
    else {
        idx_t schema_idx = input.GetSchemaIdx();
        auto &tmp_chunk = *(tmp_chunks[schema_idx].get());
        D_ASSERT(input.ColumnCount() == outer_col_map.size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Slice(input.data[i], state.sels[0],
                                                   output_idx);
            }
        }
        for (int i = 0; i < inner_col_maps[schema_idx].size();
             i++) { 
            chunk.data[inner_col_maps[schema_idx][i]].Slice(
                tmp_chunk.data[i + input.ColumnCount()], state.sels[0],
                output_idx);
        }
        chunk.SetCardinality(output_idx);
    }
    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalIdSeek::referInputChunks(
    DataChunk &input, vector<unique_ptr<DataChunk>> &chunks, IdSeekState &state,
    vector<idx_t> &num_tuples_per_chunk, idx_t &output_chunk_idx) const
{
    // for original ones reference existing columns
    if (!do_filter_pushdown) {
        idx_t schema_idx = input.GetSchemaIdx();
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_map[i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]->data[outer_col_map[i]].Slice(
                        input.data[i], state.sels[chunk_idx],
                        num_tuples_per_chunk[chunk_idx]);
                }
            }
        }

        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(num_tuples_per_chunk[i]);
        }

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
    else {
        idx_t schema_idx = input.GetSchemaIdx();

        // outer columns
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_map[i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]->data[outer_col_map[i]].Reference(
                        tmp_chunk.data[i]);
                }
            }
        }

        // inner columns
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            auto inner_col_maps_idx = chunk_idx % inner_col_maps.size();
            auto &tmp_chunk = *(tmp_chunks[chunk_idx].get());
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < inner_col_maps[inner_col_maps_idx].size();
                 i++) {
                // Else case means  the filter-only column case.
                if (inner_col_maps[inner_col_maps_idx][i] <
                    chunks[chunk_idx]->ColumnCount()) {
                    D_ASSERT(inner_col_maps[inner_col_maps_idx][i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]
                        ->data[inner_col_maps[inner_col_maps_idx][i]]
                        .Reference(tmp_chunk.data[i + input.ColumnCount()]);
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
        }
        else {
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
}

OperatorResultType PhysicalIdSeek::referInputChunksLeft(
    DataChunk &input, vector<unique_ptr<DataChunk>> &chunks, IdSeekState &state,
    vector<idx_t> &num_tuples_per_chunk, idx_t &output_chunk_idx) const
{
    // for original ones reference existing columns
    if (!do_filter_pushdown) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(chunks.size() == 1);
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            auto inner_col_maps_idx = chunk_idx % inner_col_maps.size();
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_map[i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]->data[outer_col_map[i]].Reference(
                        input.data[i]);
                }
            }
            for (int i = 0; i < inner_col_maps[inner_col_maps_idx].size();
                 i++) {
                // Else case means  the filter-only column case.
                if (inner_col_maps[inner_col_maps_idx][i] <
                    chunks[chunk_idx]->ColumnCount()) {
                    D_ASSERT(chunks[chunk_idx]
                                 ->data[inner_col_maps[inner_col_maps_idx][i]]
                                 .GetVectorType() == VectorType::FLAT_VECTOR);
                    auto &validity = FlatVector::Validity(
                        chunks[chunk_idx]
                            ->data[inner_col_maps[inner_col_maps_idx][i]]);
                    D_ASSERT(inner_col_maps[inner_col_maps_idx][i] <
                             chunks[chunk_idx]->ColumnCount());
                    for (auto j = 0; j < state.null_tuples_idx.size(); j++) {
                        validity.SetInvalid(state.null_tuples_idx[j]);
                    }
                }
            }
        }

        for (auto i = 0; i < chunks.size(); i++) {
            chunks[i]->SetCardinality(num_tuples_per_chunk[i] +
                                      state.null_tuples_idx.size());
        }

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
    else {
        D_ASSERT(state.null_tuples_idx.size() == 0);  // not implemented yet
        idx_t schema_idx = input.GetSchemaIdx();

        // outer columns
        for (auto chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
            if (num_tuples_per_chunk[chunk_idx] == 0)
                continue;
            for (int i = 0; i < input.ColumnCount(); i++) {
                if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                    D_ASSERT(outer_col_map[i] <
                             chunks[chunk_idx]->ColumnCount());
                    chunks[chunk_idx]->data[outer_col_map[i]].Slice(
                        input.data[i], state.sels[chunk_idx],
                        num_tuples_per_chunk[chunk_idx]);
                }
            }
        }

        // inner columns
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
        }
        else {
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
        // Remove ID column for rowcol_t
        auto &ith_scan_type = scan_types[i];
        auto num_id_columns = 0;
        std::queue<idx_t> id_col_queue;
        for (auto j = 0; j < ith_scan_type.size(); j++) {
            if (ith_scan_type[j].id() == LogicalTypeId::ID) {
                num_id_columns++;
                id_col_queue.push(j);
            }
        }

        idx_t idx_shift_for_non_id_columns = 0;
        uint64_t accumulated_offset = 0;
        partial_schemas.push_back(PartialSchema());
        partial_schemas[i].offset_info.resize(
            union_inner_col_map.size() - num_id_columns, -1);

        for (auto j = 0; j < inner_col_maps[i].size(); j++) {
            if (inner_col_maps[i][j] ==
                std::numeric_limits<uint32_t>::
                    max())  // this case is not handled well, please fix this
                continue;
            if (ith_scan_type[j].id() == LogicalTypeId::ID)
                continue;

            // Check if current index is greater than the front of the queue
            if (!id_col_queue.empty() && j >= id_col_queue.front()) {
                // Increment the shift and pop the queue
                idx_shift_for_non_id_columns++;
                id_col_queue.pop();
            }

            auto it =
                std::find(union_inner_col_map.begin(),
                          union_inner_col_map.end(), inner_col_maps[i][j]);
            auto pos =
                it - union_inner_col_map.begin() - idx_shift_for_non_id_columns;
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
    idx_t chunk_idx, vector<uint32_t> &output_col_idx) const
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
    auto inner_col_maps_idx = chunk_idx % inner_col_maps.size();
    auto outer_size = outer_col_map.size();
    auto inner_size = inner_col_maps[inner_col_maps_idx].size();
    output_col_idx.reserve(inner_size);
    for (idx_t i = 0; i < inner_size; i++) {
        output_col_idx.push_back(i + outer_size);
    }
}

void PhysicalIdSeek::getFilteredTargetSeqno(
    vector<idx_t> &seqno_to_eid_idx, size_t num_extents, const sel_t *sel_idxs,
    size_t count, vector<vector<uint32_t>> &out_seqnos) const
{
    out_seqnos.clear();  // Ensure the output is empty before starting.
    out_seqnos.resize(
        num_extents);  // Prepare the output with the correct number of inner vectors.
    for (auto &out_vec : out_seqnos) {
        out_vec.reserve(count);
    }

    for (auto i = 0; i < count; i++) {
        auto seqno = sel_idxs[i];
        auto eid_idx = seqno_to_eid_idx[seqno];
        if (eid_idx != -1) {
            out_seqnos[eid_idx].push_back(seqno);
        }
    }
}

void PhysicalIdSeek::genNonPredColIdxs()
{
    non_pred_col_idxs_per_schema.resize(inner_col_maps.size());
    for (auto i = 0; i < this->inner_col_maps.size(); i++) {
        auto &inner_col_map = this->inner_col_maps[i];
        if (!do_filter_pushdown) {
            for (auto j = 0; j < inner_col_map.size(); j++) {
                non_pred_col_idxs_per_schema[i].push_back(inner_col_map[j]);
            }
        }
        else {
            // In filter pushdown case, IdSeek first scans predicate columns
            // and append it to the outer columns, which creates temp chunk.
            // Compiler is aware of this, thus the pred_col_idxs_per_schema
            // have column indexes of temp chunk, not the actual output chunk.
            // Therefore, we should iterate from 0 to
            // inner_col_map.size() + this->outer_col_map.size()
            auto &pred_col_idxs = this->pred_col_idxs_per_schema[i];
            for (auto j = 0;
                 j < inner_col_map.size() + this->outer_col_map.size(); j++) {
                if (std::find(pred_col_idxs.begin(), pred_col_idxs.end(), j) ==
                    pred_col_idxs.end()) {
                    non_pred_col_idxs_per_schema[i].push_back(j);
                }
            }
        }
    }
}

void PhysicalIdSeek::fillSeqnoToEIDIdx(
    size_t num_valid_extents,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &seqno_to_eid_idx) const
{
    std::fill(seqno_to_eid_idx.begin(), seqno_to_eid_idx.end(), -1);
    for (auto i = 0; i < num_valid_extents; i++) {
        auto &vec = target_seqnos_per_extent[i];
        for (auto &idx : vec) {
            seqno_to_eid_idx[idx] = i;
        }
    }
}

void PhysicalIdSeek::fillSeqnoToEIDIdx(
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &seqno_to_eid_idx) const
{
    std::fill(seqno_to_eid_idx.begin(), seqno_to_eid_idx.end(), -1);
    for (auto i = 0; i < target_seqnos_per_extent.size(); i++) {
        auto &vec = target_seqnos_per_extent[i];
        for (auto &idx : vec) {
            seqno_to_eid_idx[idx] = i;
        }
    }
}

void PhysicalIdSeek::remapSeqnoToEidIdx(
    vector<idx_t> &in_seqno_to_eid_idx, const sel_t *sel_idxs, size_t sel_size,
    vector<idx_t> &out_seqno_to_eid_idx) const
{
    out_seqno_to_eid_idx.resize(sel_size);
    for (auto i = 0; i < sel_size; i++) {
        out_seqno_to_eid_idx[i] = in_seqno_to_eid_idx[sel_idxs[i]];
    }
}

void PhysicalIdSeek::getReverseMappingIdxs(
    size_t num_chunks, idx_t base_chunk_idx, vector<idx_t> &mapping_idxs,
    vector<vector<idx_t>> &reverse_mapping_idxs) const
{
    // mapping idxs is from extent idx to chunk idx
    // reverse mapping idx is from chunk idx to extent idxs
    reverse_mapping_idxs.clear();
    reverse_mapping_idxs.resize(num_chunks);
    for (auto i = 0; i < mapping_idxs.size(); i++) {
        reverse_mapping_idxs[base_chunk_idx + mapping_idxs[i]].push_back(i);
    }
}

// Since the difference comes from the null vector, we only consider the ratio of null values, not actual bytes.
size_t PhysicalIdSeek::calculateTotalNulls(
    DataChunk &unified_chunk, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    vector<size_t> size_per_extent(target_eids.size(), 0);
    vector<size_t> num_nulls_per_extent(target_eids.size(), 0);

    // Get outer cols that are in the output
    size_t num_outer_output_cols = outer_output_col_idxs.size();

    for (u_int64_t extent_idx = 0; extent_idx < target_eids.size();
         extent_idx++) {
        // Fill size_per_extent
        size_t extent_size = target_seqnos_per_extent[extent_idx].size();
        size_per_extent[extent_idx] = extent_size;

        // Fill num_nulls_per_extent
        size_t num_nulls = 0;
        vector<idx_t> inner_output_col_idxs;
        getOutputColIdxsForInner(extent_idx, mapping_idxs,
                                 inner_output_col_idxs);

        for (auto columnIdx = 0; columnIdx < unified_chunk.ColumnCount();
             columnIdx++) {
            // Inner column only
            if (std::find(outer_output_col_idxs.begin(),
                          outer_output_col_idxs.end(),
                          columnIdx) != outer_output_col_idxs.end()) {
                continue;
            }

            // If the column is not in the output, it is a null column
            if (std::find(inner_output_col_idxs.begin(),
                          inner_output_col_idxs.end(),
                          columnIdx) == inner_output_col_idxs.end()) {
                num_nulls += extent_size;
            }
        }

        num_nulls_per_extent[extent_idx] = num_nulls;
    }

    size_t total_nulls = std::accumulate(num_nulls_per_extent.begin(),
                                         num_nulls_per_extent.end(), 0);
    return total_nulls;
}

PhysicalIdSeek::OutputFormat PhysicalIdSeek::determineFormatByCostModel(
    bool sort_order_enforced, size_t total_nulls) const
{
    const double COLUMNAR_PROCESSING_UNIT_COST = 0.8;
    const double ROW_PROCESSING_UNIT_COST = 2.4;
    const double NULL_PROCESSING_UNIT_COST = 0.009;
    if (sort_order_enforced) {
        throw NotImplementedException(
            "PhysicalIdSeek::determineFormatByCostModel - sort_order_enforced");
    }
    else if (unionall_forced) {
        return OutputFormat::UNIONALL;
    }
    else {
        /**
         * The cost is calculated by two terms, 1) per schema processing cost, 2) null processing cost
         * To be detailed, we can use width or so, but this could introduce too much overhead.
         * Per schema processing cost is modeled as C1*log(x+1), where x is number of tuples belong to a schema
         * Null processing cost is modeled as C2*y, where y is the number of null values 
        */
        double union_cost, row_cost;

        // calculate per schema processing cost
        double union_processing_cost, row_processing_cost;
        size_t total_tuples = std::accumulate(num_tuples_per_schema.begin(),
                                              num_tuples_per_schema.end(), 0);
        union_processing_cost =
            COLUMNAR_PROCESSING_UNIT_COST * log2(total_tuples + 1);
        row_processing_cost = ROW_PROCESSING_UNIT_COST * log2(total_tuples + 1);

        // calculate cost
        union_cost =
            union_processing_cost + NULL_PROCESSING_UNIT_COST * total_nulls;
        row_cost = row_processing_cost;

        if (union_cost < row_cost) {
            return OutputFormat::UNIONALL;
        }
        else {
            return OutputFormat::ROW;
        }
    }
}

void PhysicalIdSeek::fillOutSizePerSchema(
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    D_ASSERT(num_tuples_per_schema.size() == num_total_schemas);
    std::fill(num_tuples_per_schema.begin(), num_tuples_per_schema.end(), 0);
    for (u_int64_t extent_idx = 0; extent_idx < target_eids.size();
         extent_idx++) {
        auto mapping_idx = mapping_idxs[extent_idx];
        num_tuples_per_schema[mapping_idx] +=
            target_seqnos_per_extent[extent_idx].size();
    }
}

void PhysicalIdSeek::markInvalidForUnseekedValues(
    DataChunk &chunk, IdSeekState &state, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    static size_t num_nulls_added = 0;
    for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
        vector<idx_t> inner_output_col_idx;
        getOutputColIdxsForInner(extentIdx, mapping_idxs, inner_output_col_idx);
        auto &target_seqnos = target_seqnos_per_extent[extentIdx];

        for (auto columnIdx = 0; columnIdx < chunk.ColumnCount(); columnIdx++) {
            // Two cases, 1) outer column, 2) inner column, but not in the output
            if (std::find(outer_output_col_idxs.begin(),
                          outer_output_col_idxs.end(),
                          columnIdx) != outer_output_col_idxs.end()) {
                continue;
            }
            if (std::find(inner_output_col_idx.begin(),
                          inner_output_col_idx.end(),
                          columnIdx) == inner_output_col_idx.end()) {
                auto &vec = chunk.data[columnIdx];
                vec.SetIsValid(true);
                auto &validity = FlatVector::Validity(vec);
                if (validity.GetData() == nullptr) {
                    validity.Initialize(STANDARD_VECTOR_SIZE);
                }
                for (auto seqno : target_seqnos) {
                    validity.SetInvalid(seqno);
                }
            }
        }
    }
}

void PhysicalIdSeek::nullifyValuesForPrunedExtents(
    DataChunk &chunk, IdSeekState &state, size_t num_unpruned_extents,
    vector<vector<uint32_t>> &target_seqnos_per_extent) const
{
    for (u_int64_t extentIdx = num_unpruned_extents;
         extentIdx < target_seqnos_per_extent.size(); extentIdx++) {
        auto &target_seqnos = target_seqnos_per_extent[extentIdx];
        for (auto columnIdx = 0; columnIdx < chunk.ColumnCount(); columnIdx++) {
            auto &vec = chunk.data[columnIdx];
            auto &validity = FlatVector::Validity(vec);
            if (validity.GetData() == nullptr) {
                validity.Initialize(STANDARD_VECTOR_SIZE);
            }
            for (auto seqno : target_seqnos) {
                validity.SetInvalid(seqno);
            }
        }
    }
}

OperatorResultType PhysicalIdSeek::moveToNextOutputChunk(
    vector<unique_ptr<DataChunk>> &chunks, OperatorState &lstate,
    idx_t &output_chunk_idx) const
{
    auto &state = (IdSeekState &)lstate;
    for (auto chunk_idx = state.cur_schema_idx; chunk_idx < chunks.size();
         chunk_idx++) {
        if (chunks[chunk_idx]->size() == 0)
            continue;
        output_chunk_idx = chunk_idx;
        state.cur_schema_idx = chunk_idx + 1;
        return OperatorResultType::HAVE_MORE_OUTPUT;
    }
    output_chunk_idx = 0;
    state.has_remaining_output = false;
    state.need_initialize_extit = true;
    return OperatorResultType::OUTPUT_EMPTY;
}

void PhysicalIdSeek::getOutputColIdxsForInner(
    idx_t extentIdx, vector<idx_t> &mapping_idxs,
    vector<idx_t> &output_col_idx) const
{
    for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
        if (inner_col_maps[mapping_idxs[extentIdx]][i] !=
            std::numeric_limits<uint32_t>::max()) {
            output_col_idx.push_back(
                inner_col_maps[mapping_idxs[extentIdx]][i]);
        }
    }
}

void PhysicalIdSeek::getOutputColIdxsForOuter(
    vector<idx_t> &output_col_idx) const
{
    for (idx_t i = 0; i < outer_col_map.size(); i++) {
        if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
            output_col_idx.push_back(outer_col_map[i]);
        }
    }
}

void PhysicalIdSeek::generateOutputColIdxsForOuter()
{
    for (idx_t i = 0; i < outer_col_map.size(); i++) {
        if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
            outer_output_col_idxs.push_back(outer_col_map[i]);
        }
    }
}

void PhysicalIdSeek::generateOutputColIdxsForInner()
{
    inner_output_col_idxs.resize(inner_col_maps.size());
    for (idx_t i = 0; i < inner_col_maps.size(); i++) {
        inner_output_col_idxs[i].reserve(inner_col_maps[i].size());
        for (idx_t j = 0; j < inner_col_maps[i].size(); j++) {
            if (inner_col_maps[i][j] != std::numeric_limits<uint32_t>::max()) {
                inner_output_col_idxs[i].push_back(inner_col_maps[i][j]);
            }
        }
    }
}

void PhysicalIdSeek::getUnionScanTypes()
{
    if (num_total_schemas == 1)
        return;

    bool found = false;
    for (auto i = 0; i < inner_col_maps.size(); i++) {
        auto &per_schema_scan_type = scan_types[i];
        auto &per_schema_inner_col_map = inner_col_maps[i];

        for (auto j = 0; j < per_schema_scan_type.size(); j++) {
            if (per_schema_scan_type[j].id() == LogicalTypeId::ID) {
                out_id_col_idx = per_schema_inner_col_map[j];
                found = true;
                break;
            }
        }
        if (found)
            break;
    }

    if (!found) {
        out_id_col_idx = -1;
    }

    union_inner_col_map_wo_id.reserve(union_inner_col_map.size());
    for (auto i = 0; i < union_inner_col_map.size(); i++) {
        if (union_inner_col_map[i] != out_id_col_idx) {
            union_inner_col_map_wo_id.push_back(union_inner_col_map[i]);
        }
    }
}

void PhysicalIdSeek::buildExpressionExecutors(
    vector<vector<unique_ptr<Expression>>> &predicates)
{
    executors.resize(predicates.size());
    for (auto i = 0; i < predicates.size(); i++) {
        if (predicates[i].size() > 1) {
            auto conjunction = make_unique<BoundConjunctionExpression>(
                ExpressionType::CONJUNCTION_AND);
            for (auto &expr : predicates[i]) {
                conjunction->children.push_back(move(expr));
            }
            expressions.push_back(move(conjunction));
        }
        else {
            expressions.push_back(move(predicates[i][0]));
        }
        executors[i].AddExpression(*(expressions[i]));
    }
}

void PhysicalIdSeek::markInvalidForColumnsToUnseek(DataChunk &chunk, vector<ExtentID> &target_eids, 
                            vector<idx_t> &mapping_idxs) const
{
    // Mark all columns invalid first
    for (auto columnIdx = 0; columnIdx < chunk.ColumnCount(); columnIdx++) {
        chunk.data[columnIdx].SetIsValid(false);
    }
    // Mark all outer columns valid
    for (auto columnIdx : outer_output_col_idxs) {
        if (columnIdx < chunk.ColumnCount()) {
            chunk.data[columnIdx].SetIsValid(true);
        }
    }
    // Mark seek columns valid
    for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
        auto mapping_idx = mapping_idxs[extentIdx];
        vector<idx_t> inner_output_col_idx;
        getOutputColIdxsForInner(extentIdx, mapping_idxs,
                                 inner_output_col_idx);
        for (auto columnIdx : inner_output_col_idx) {
            if (columnIdx < chunk.ColumnCount()) {
                chunk.data[columnIdx].SetIsValid(true);
            }
        }
    }
}

std::string PhysicalIdSeek::ParamsToString() const
{
    std::string result = "";
    result += JoinTypeToString(join_type) + ", ";
    result += "id_col_idx=" + std::to_string(id_col_idx) + ", ";
    result += "projection_mapping.size()=" +
              std::to_string(projection_mapping.size()) + ", ";
    result += "projection_mapping[0].size()=" +
              std::to_string(projection_mapping[0].size()) + ", ";
    result +=
        "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
    result += "inner_col_maps.size()=" + std::to_string(inner_col_maps.size());
    if (expressions.size() > 0 && expressions[0] != nullptr) {
        result += ", expression[0]=" + expressions[0]->ToString();
    }
    return result;
}

std::string PhysicalIdSeek::ToString() const
{
    return "IdSeek";
}

}  // namespace s62