
#include "common/typedef.hpp"

#include "common/types/rowcol_type.hpp"
#include "common/types/selection_vector.hpp"
#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "planner/joinside.hpp"

#include <string>
#include "icecream.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//

unique_ptr<OperatorState> PhysicalAdjIdxJoin::GetOperatorState(
    ExecutionContext &context) const
{
    return make_unique<AdjIdxJoinState>(this->join_type);
}

// helper functions
inline ExpandDirection adjListLogicalTypeToExpandDir(LogicalType &adjType)
{
    return adjType == LogicalType::FORWARD_ADJLIST ? ExpandDirection::OUTGOING
                                                   : ExpandDirection::INCOMING;
}

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

OperatorResultType PhysicalAdjIdxJoin::ProcessSemiAntiJoin(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    auto &state = (AdjIdxJoinState &)lstate;

    D_ASSERT(join_type == JoinType::SEMI || join_type == JoinType::ANTI);

    uint64_t *adj_start;
    uint64_t *adj_end;
    uint64_t src_vid;
    Vector &src_vid_column_vector =
        input.data[sid_col_idx];  // can be dictionaryvector
    size_t adjlist_size;

    for (; state.lhs_idx < input.size(); state.lhs_idx++) {
        uint64_t &src_vid =
            getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
        context.client->graph_storage_wrapper->getAdjListFromVid(
            *state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
            src_vid, adj_start, adj_end,
            adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]));
        // FIXME debug calculate size directly here
        int adj_size_debug = (adj_end - adj_start) / 2;
        const bool predicate_satisfied =
            (join_type == JoinType::SEMI)
                ? (!state.src_nullity[state.lhs_idx] &&
                   adj_size_debug > 0)  // semi join
                : (state.src_nullity[state.lhs_idx] ||
                   adj_size_debug == 0);  // anti join
        if (predicate_satisfied) {
            state.rhs_sel.set_index(state.output_idx++, state.lhs_idx);
        }
    }
    // use sel vector and filter lhs to chunk
    // chunk determined. now fill in lhs using slice operation
    for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
        chunk.data[colId].Slice(input.data[colId], state.rhs_sel,
                                state.output_idx);
    }
    D_ASSERT(state.output_idx <= STANDARD_VECTOR_SIZE);
    chunk.SetCardinality(state.output_idx);

    return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalAdjIdxJoin::GetJoinMatches(ExecutionContext &context,
                                        DataChunk &input,
                                        AdjIdxJoinState &state) const
{
    // check nullity for semi / anti join
    if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
        switch (input.data[sid_col_idx].GetVectorType()) {
            case VectorType::DICTIONARY_VECTOR: {
                const auto &sel_vector =
                    DictionaryVector::SelVector(input.data[sid_col_idx]);
                const auto &child =
                    DictionaryVector::Child(input.data[sid_col_idx]);
                for (idx_t i = 0; i < input.size(); i++) {
                    state.src_nullity[i] =
                        FlatVector::IsNull(child, sel_vector.get_index(i));
                }
                break;
            }
            case VectorType::CONSTANT_VECTOR: {
                auto is_null = ConstantVector::IsNull(input.data[sid_col_idx]);
                for (idx_t i = 0; i < input.size(); i++) {
                    state.src_nullity[i] = is_null;
                }
                break;
            }
            default: {
                for (idx_t i = 0; i < input.size(); i++) {
                    state.src_nullity[i] =
                        FlatVector::IsNull(input.data[sid_col_idx], i);
                }
                break;
            }
        }
    }

    // fill in adjacency col info
    if (state.adj_col_idxs.size() == 0) {
        context.client->graph_storage_wrapper->getAdjColIdxs(
            (idx_t)adjidx_obj_id, state.adj_col_idxs, state.adj_col_types);
        for (auto i = 0; i < state.adj_col_idxs.size(); i++) {
            D_ASSERT(state.adj_col_idxs[i] >= 0);
        }
    }
}

void PhysicalAdjIdxJoin::IterateSourceVidsAndFillRHSOutput(
    ExecutionContext &context, AdjIdxJoinState &state, DataChunk &input,
    DataChunk &chunk, uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
    uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
    ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const
{
    Vector &src_vid_column_vector = input.data[sid_col_idx];
    auto &validity = src_vid_column_vector.GetValidity();

    // todo cleaning these codes
    if (validity.AllValid()) {
        switch (src_vid_column_vector.GetVectorType()) {
            case VectorType::DICTIONARY_VECTOR: {
                auto src_vid_column_data =
                    (uint64_t *)src_vid_column_vector.GetData();
                auto &src_sel_vector =
                    DictionaryVector::SelVector(src_vid_column_vector);
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    uint64_t src_vid =
                        src_vid_column_data[src_sel_vector.get_index(
                            state.lhs_idx)];
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            case VectorType::FLAT_VECTOR: {
                auto src_vid_column_data =
                    (uint64_t *)src_vid_column_vector.GetData();
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    uint64_t src_vid = src_vid_column_data[state.lhs_idx];
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            case VectorType::CONSTANT_VECTOR: {
                uint64_t src_vid =
                    ((uint64_t *)ConstantVector::GetData<uintptr_t>(
                        src_vid_column_vector))[0];
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            case VectorType::ROW_VECTOR: {
                rowcol_t *rowcol_arr =
                    FlatVector::GetData<rowcol_t>(src_vid_column_vector);
                auto rowcol_idx = src_vid_column_vector.GetRowColIdx();
                char *row_ptr = src_vid_column_vector.GetRowMajorStore();
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    // calculate offset
                    auto base_offset = rowcol_arr[state.lhs_idx].offset;
                    PartialSchema *schema_ptr =
                        (PartialSchema *)rowcol_arr[state.lhs_idx].schema_ptr;
                    auto offset = schema_ptr->getIthColOffset(rowcol_idx);

                    // get vid
                    uint64_t src_vid;
                    memcpy(&src_vid, row_ptr + base_offset + offset,
                           sizeof(uint64_t));
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            default: {
                D_ASSERT(false);
            }
        }
    }
    else if (validity.CheckAllInValid()) {
        D_ASSERT(false);  // not implemented yet
    }
    else {
        switch (src_vid_column_vector.GetVectorType()) {
            case VectorType::DICTIONARY_VECTOR: {
                auto src_vid_column_data =
                    (uint64_t *)src_vid_column_vector.GetData();
                auto &src_sel_vector =
                    DictionaryVector::SelVector(src_vid_column_vector);
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    // uint64_t src_vid = src_vid_column_data[src_sel_vector.get_index(
                    //     state.lhs_idx)];
                    auto src_vid_val =
                        src_vid_column_vector.GetValue(state.lhs_idx);
                    if (src_vid_val.IsNull()) {
                        AdvanceToNextLHS(state, nullptr, nullptr,
                                         tgt_adj_column, eid_adj_column,
                                         tgt_validity_mask, eid_validity_mask);
                        continue;
                    }
                    uint64_t src_vid = src_vid_val.GetValue<uint64_t>();
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            case VectorType::FLAT_VECTOR: {
                auto src_vid_column_data =
                    (uint64_t *)src_vid_column_vector.GetData();
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    if (!validity.RowIsValid(state.lhs_idx)) {
                        AdvanceToNextLHS(state, nullptr, nullptr,
                                         tgt_adj_column, eid_adj_column,
                                         tgt_validity_mask, eid_validity_mask);
                        continue;
                    }
                    uint64_t src_vid = src_vid_column_data[state.lhs_idx];
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            case VectorType::CONSTANT_VECTOR: {
                // TODO we can optimize this case
                auto src_vid_val =
                    src_vid_column_vector.GetValue(state.lhs_idx);
                if (src_vid_val.IsNull()) {
                    AdvanceToNextLHS(state, nullptr, nullptr, tgt_adj_column,
                                     eid_adj_column, tgt_validity_mask,
                                     eid_validity_mask);
                    break;
                }
                // uint64_t src_vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(
                //     src_vid_column_vector))[0];
                uint64_t src_vid = src_vid_val.GetValue<uint64_t>();
                while (state.output_idx < STANDARD_VECTOR_SIZE &&
                       state.lhs_idx < input.size()) {
                    GetAdjListAndFillRHSOutput(
                        context, state, src_vid, src_adj_column, tgt_adj_column, eid_adj_column,
                        tgt_validity_mask, eid_validity_mask, cur_direction);
                }
                break;
            }
            default: {
                D_ASSERT(false);
            }
        }
    }
}

void PhysicalAdjIdxJoin::IterateSourceVidsAndFillRHSOutputInto(
    ExecutionContext &context, AdjIdxJoinState &state, DataChunk &input,
    DataChunk &chunk, uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
    uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
    ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const
{
    Vector &src_vid_column_vector = input.data[sid_col_idx];
    Vector &tgt_vid_column_vector = input.data[tgt_col_idx];

    // todo cleaning these codes
    if (src_vid_column_vector.GetVectorType() ==
        VectorType::DICTIONARY_VECTOR) {
        auto src_vid_column_data = (uint64_t *)src_vid_column_vector.GetData();
        auto &src_sel_vector =
            DictionaryVector::SelVector(src_vid_column_vector);
        if (tgt_vid_column_vector.GetVectorType() ==
            VectorType::DICTIONARY_VECTOR) {
            auto tgt_vid_column_data =
                (uint64_t *)tgt_vid_column_vector.GetData();
            auto tgt_sel_vector =
                DictionaryVector::SelVector(tgt_vid_column_vector);
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t src_vid = src_vid_column_data[src_sel_vector.get_index(
                    state.lhs_idx)];
                uint64_t tgt_vid = tgt_vid_column_data[tgt_sel_vector.get_index(
                    state.lhs_idx)];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else if (tgt_vid_column_vector.GetVectorType() ==
                 VectorType::FLAT_VECTOR) {
            auto tgt_vid_column_data =
                (uint64_t *)tgt_vid_column_vector.GetData();
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t src_vid = src_vid_column_data[src_sel_vector.get_index(
                    state.lhs_idx)];
                uint64_t tgt_vid = tgt_vid_column_data[state.lhs_idx];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else if (tgt_vid_column_vector.GetVectorType() ==
                 VectorType::CONSTANT_VECTOR) {
            uint64_t tgt_vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(
                tgt_vid_column_vector))[0];
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t src_vid = src_vid_column_data[src_sel_vector.get_index(
                    state.lhs_idx)];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else {
            D_ASSERT(false);
        }
    }
    else if (src_vid_column_vector.GetVectorType() == VectorType::FLAT_VECTOR) {
        auto src_vid_column_data = (uint64_t *)src_vid_column_vector.GetData();
        if (tgt_vid_column_vector.GetVectorType() ==
            VectorType::DICTIONARY_VECTOR) {
            auto tgt_vid_column_data =
                (uint64_t *)tgt_vid_column_vector.GetData();
            auto tgt_sel_vector =
                DictionaryVector::SelVector(tgt_vid_column_vector);
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t src_vid = src_vid_column_data[state.lhs_idx];
                uint64_t tgt_vid = tgt_vid_column_data[tgt_sel_vector.get_index(
                    state.lhs_idx)];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else if (tgt_vid_column_vector.GetVectorType() ==
                 VectorType::FLAT_VECTOR) {
            auto tgt_vid_column_data =
                (uint64_t *)tgt_vid_column_vector.GetData();
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t src_vid = src_vid_column_data[state.lhs_idx];
                uint64_t tgt_vid = tgt_vid_column_data[state.lhs_idx];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else if (tgt_vid_column_vector.GetVectorType() ==
                 VectorType::CONSTANT_VECTOR) {
            uint64_t tgt_vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(
                tgt_vid_column_vector))[0];
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t src_vid = src_vid_column_data[state.lhs_idx];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else {
            D_ASSERT(false);
        }
    }
    else if (src_vid_column_vector.GetVectorType() ==
             VectorType::CONSTANT_VECTOR) {
        uint64_t src_vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(
            src_vid_column_vector))[0];
        if (tgt_vid_column_vector.GetVectorType() ==
            VectorType::DICTIONARY_VECTOR) {
            auto tgt_vid_column_data =
                (uint64_t *)tgt_vid_column_vector.GetData();
            auto tgt_sel_vector =
                DictionaryVector::SelVector(tgt_vid_column_vector);
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t tgt_vid = tgt_vid_column_data[tgt_sel_vector.get_index(
                    state.lhs_idx)];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else if (tgt_vid_column_vector.GetVectorType() ==
                 VectorType::FLAT_VECTOR) {
            auto tgt_vid_column_data =
                (uint64_t *)tgt_vid_column_vector.GetData();
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                uint64_t tgt_vid = tgt_vid_column_data[state.lhs_idx];
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else if (tgt_vid_column_vector.GetVectorType() ==
                 VectorType::CONSTANT_VECTOR) {
            uint64_t tgt_vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(
                tgt_vid_column_vector))[0];
            while (state.output_idx < STANDARD_VECTOR_SIZE &&
                   state.lhs_idx < input.size()) {
                GetAdjListAndFillRHSOutputInto(
                    context, state, src_vid, tgt_vid, src_adj_column, tgt_adj_column,
                    eid_adj_column, tgt_validity_mask, eid_validity_mask,
                    cur_direction);
            }
        }
        else {
            D_ASSERT(false);
        }
    }
}

inline void PhysicalAdjIdxJoin::GetAdjListAndFillRHSOutput(
    ExecutionContext &context, AdjIdxJoinState &state, uint64_t src_vid,
    uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
    uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
    ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const
{
    uint64_t *adj_start, *adj_end;
    context.client->graph_storage_wrapper->getAdjListFromVid(
        *state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
        src_vid, adj_start, adj_end, cur_direction);

    // calculate size can be fetched
    if (state.rhs_idx == 0) {
        state.output_idx_before_fetch = state.output_idx;
    }
    int adj_size = (adj_end - adj_start) / 2;
    size_t num_rhs_left = adj_size - state.rhs_idx;
    size_t num_rhs_to_try_fetch =
        ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left)
            ? num_rhs_left
            : (STANDARD_VECTOR_SIZE - state.output_idx);

    if (adj_size == 0) {
        D_ASSERT(num_rhs_to_try_fetch == 0);
    }
    else {
        // fill sid
        if (load_sid) {
            for (size_t i = 0; i < num_rhs_to_try_fetch; i++) {
                src_adj_column[state.output_idx + i] = src_vid;
            }
        }

        // fill rhs (update output_idx and rhs_idx)
        fillFunc(state, adj_start, tgt_adj_column, eid_adj_column,
                 tgt_validity_mask, eid_validity_mask, num_rhs_to_try_fetch,
                 false);
    }

    // update lhs_idx and adj_idx for next iteration
    if (state.rhs_idx >= adj_size) {
        AdvanceToNextLHS(state, adj_start, adj_end, tgt_adj_column,
                         eid_adj_column, tgt_validity_mask, eid_validity_mask);
    }
}

inline void PhysicalAdjIdxJoin::GetAdjListAndFillRHSOutputInto(
    ExecutionContext &context, AdjIdxJoinState &state, uint64_t src_vid,
    uint64_t tgt_vid, uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
    uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
    ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const
{
    uint64_t *adj_start, *adj_end;
    context.client->graph_storage_wrapper->getAdjListFromVid(
        *state.adj_it, state.adj_col_idxs[state.adj_idx], state.prev_eid,
        src_vid, adj_start, adj_end, cur_direction);

    // calculate size can be fetched
    if (state.rhs_idx == 0) {
        state.output_idx_before_fetch = state.output_idx;
    }
    int adj_size = (adj_end - adj_start) / 2;
    size_t num_rhs_left = adj_size - state.rhs_idx;
    size_t num_rhs_to_try_fetch =
        ((STANDARD_VECTOR_SIZE - state.output_idx) > num_rhs_left)
            ? num_rhs_left
            : (STANDARD_VECTOR_SIZE - state.output_idx);

    if (adj_size == 0) {
        D_ASSERT(num_rhs_to_try_fetch == 0);
    }
    else {
        state.all_adjs_null = false;

        // fill sid
        if (load_sid) {
            for (size_t i = 0; i < num_rhs_to_try_fetch; i++) {
                src_adj_column[state.output_idx + i] = src_vid;
            }
        }

        // fill rhs (update output_idx and rhs_idx)
        fillFuncInto(state, adj_start, tgt_adj_column, eid_adj_column,
                     tgt_validity_mask, eid_validity_mask, tgt_vid,
                     num_rhs_to_try_fetch, false);
    }

    // update lhs_idx and adj_idx for next iteration
    if (state.rhs_idx >= adj_size) {
        // for this (lhs_idx, adj_idx), equi join is done
        if (state.adj_idx == state.adj_col_idxs.size() - 1) {
            if ((state.output_idx_before_fetch == state.output_idx) &&
                (join_type == JoinType::LEFT)) {
                // produce rhs (update output_idx and rhs_idx)
                fillFuncInto(state, adj_start, tgt_adj_column, eid_adj_column,
                             tgt_validity_mask, eid_validity_mask, tgt_vid, 1,
                             true);
            }
            state.all_adjs_null = true;
            state.lhs_idx++;
            state.adj_idx = 0;
        }
        else {
            state.adj_idx++;
        }
        state.rhs_idx = 0;
    }
}

inline void PhysicalAdjIdxJoin::AdvanceToNextLHS(
    AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *adj_end,
    uint64_t *&tgt_adj_column, uint64_t *&eid_adj_column,
    ValidityMask *tgt_validity_mask, ValidityMask *eid_validity_mask) const
{
    // for this (lhs_idx, adj_idx), equi join is done
    if (state.adj_idx == state.adj_col_idxs.size() - 1) {
        if ((state.output_idx_before_fetch == state.output_idx ||
             adj_start == nullptr) &&
            (join_type == JoinType::LEFT)) {
            // produce rhs (update output_idx and rhs_idx)
            fillFunc(state, adj_start, tgt_adj_column, eid_adj_column,
                     tgt_validity_mask, eid_validity_mask, 1, true);
        }
        state.lhs_idx++;
        state.adj_idx = 0;
    }
    else {
        state.adj_idx++;
    }
    state.rhs_idx = 0;
}

OperatorResultType PhysicalAdjIdxJoin::ProcessEquiJoin(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    auto &state = (AdjIdxJoinState &)lstate;
    uint64_t *src_adj_column = nullptr;
    uint64_t *tgt_adj_column = nullptr;
    uint64_t *eid_adj_column = nullptr;
    ValidityMask *tgt_validity_mask = nullptr;
    ValidityMask *eid_validity_mask = nullptr;
    ExpandDirection cur_direction;

    InitializeInfosForProcessing(
        state, input, chunk, src_adj_column, tgt_adj_column, eid_adj_column,
        tgt_validity_mask, eid_validity_mask, cur_direction);

    // iterate source vids
    IterateSourceVidsAndFillRHSOutput(
        context, state, input, chunk, src_adj_column, tgt_adj_column,
        eid_adj_column, tgt_validity_mask, eid_validity_mask, cur_direction);

    // chunk determined. now fill in lhs using slice operation
    FillLHSOutput(state, input, chunk);

    return CheckIterationState(state, input.size());
}

OperatorResultType PhysicalAdjIdxJoin::ProcessEquiJoinInto(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    auto &state = (AdjIdxJoinState &)lstate;
    uint64_t *src_adj_column = nullptr;
    uint64_t *tgt_adj_column = nullptr;
    uint64_t *eid_adj_column = nullptr;
    ValidityMask *tgt_validity_mask = nullptr;
    ValidityMask *eid_validity_mask = nullptr;
    ExpandDirection cur_direction;

    InitializeInfosForProcessing(
        state, input, chunk, src_adj_column, tgt_adj_column, eid_adj_column,
        tgt_validity_mask, eid_validity_mask, cur_direction);

    // iterate source vids and fill rhs output
    IterateSourceVidsAndFillRHSOutputInto(
        context, state, input, chunk, src_adj_column, tgt_adj_column,
        eid_adj_column, tgt_validity_mask, eid_validity_mask, cur_direction);

    // chunk determined. now fill in lhs using slice operation
    FillLHSOutput(state, input, chunk);

    return CheckIterationState(state, input.size());
}

OperatorResultType PhysicalAdjIdxJoin::ProcessLeftJoin(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    // currently, we process left join in GetAdjListAndFillRHSOutputInto
    return ProcessEquiJoin(context, input, chunk, lstate);
}

OperatorResultType PhysicalAdjIdxJoin::ProcessLeftJoinInto(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    // currently, we process left join in GetAdjListAndFillRHSOutputInto
    return ProcessEquiJoinInto(context, input, chunk, lstate);
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInput(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    auto &state = (AdjIdxJoinState &)lstate;

    if (!state.first_fetch) {
        InitializeAdjIdxJoin(state, context, input, chunk);
    }

    if (join_type == JoinType::INNER) {
        return ProcessEquiJoin(context, input, chunk, lstate);
    }
    else if (join_type == JoinType::LEFT) {
        return ProcessLeftJoin(context, input, chunk, lstate);
    }
    else if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
        // these joins can be processed in single call. explicitly process them and return
        return ProcessSemiAntiJoin(context, input, chunk, lstate);
    }
    else {
        throw InternalException("AdjIdxJoin type not supported yet.");
    }
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteNaiveInputInto(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    auto &state = (AdjIdxJoinState &)lstate;

    if (!state.first_fetch) {
        InitializeAdjIdxJoin(state, context, input, chunk);
    }

    if (join_type == JoinType::INNER) {
        return ProcessEquiJoinInto(context, input, chunk, lstate);
    }
    else if (join_type == JoinType::LEFT) {
        return ProcessLeftJoinInto(context, input, chunk, lstate);
    }
    else {
        throw InternalException("AdjIdxJoin type not supported yet.");
    }
}

void PhysicalAdjIdxJoin::InitializeAdjIdxJoin(AdjIdxJoinState &state,
                                              ExecutionContext &context,
                                              DataChunk &input,
                                              DataChunk &chunk) const
{
    // TODO do this once
    state.outer_col_map = move(outer_col_map);
    state.outer_col_maps = move(outer_col_maps);
    state.inner_col_map = move(inner_col_map);

    // Get join matches (sizes) for the LHS. Initialized one time per LHS
    GetJoinMatches(context, input, state);
    state.first_fetch = true;
}

void PhysicalAdjIdxJoin::InitializeInfosForProcessing(
    AdjIdxJoinState &state, DataChunk &input, DataChunk &chunk,
    uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
    uint64_t *&eid_adj_column, ValidityMask *&tgt_validity_mask,
    ValidityMask *&eid_validity_mask, ExpandDirection &cur_direction) const
{
    D_ASSERT(sid_col_idx < input.ColumnCount());

    D_ASSERT(!load_tid || (load_tid && tgtColIdx < chunk.ColumnCount()));
    if (load_eid) {
        D_ASSERT(edgeColIdx >= 0 && edgeColIdx < chunk.ColumnCount());
        // always flatvector[ID]. so ok to access directly
        eid_adj_column = (uint64_t *)chunk.data[edgeColIdx].GetData();
        eid_validity_mask = &FlatVector::Validity(chunk.data[edgeColIdx]);
    }
    if (load_sid) {
        D_ASSERT(srcColIdx >= 0 && srcColIdx < chunk.ColumnCount());
        src_adj_column = (uint64_t *)chunk.data[srcColIdx].GetData();
    }
    if (load_tid) {
        D_ASSERT(tgtColIdx >= 0 && tgtColIdx < chunk.ColumnCount());
        tgt_adj_column = (uint64_t *)chunk.data[tgtColIdx].GetData();
        tgt_validity_mask = &FlatVector::Validity(chunk.data[tgtColIdx]);
    }

    cur_direction =
        adjListLogicalTypeToExpandDir(state.adj_col_types[state.adj_idx]);
    // TODO we currently do not support iterate more than one edge types
    D_ASSERT(state.adj_col_idxs.size() == 1);
}

void PhysicalAdjIdxJoin::FillLHSOutput(AdjIdxJoinState &state, DataChunk &input,
                                       DataChunk &chunk) const
{
    idx_t schema_idx = input.GetSchemaIdx();
    schema_idx = 0;  // TODO 240117 tslee maybe we don't need
    D_ASSERT(schema_idx < state.outer_col_maps.size());
    D_ASSERT(input.ColumnCount() == state.outer_col_maps[schema_idx].size());
    for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
        if (state.outer_col_maps[schema_idx][colId] !=
            std::numeric_limits<uint32_t>::max()) {
            // when outer col map marked uint32_max, do not return
            D_ASSERT(state.outer_col_maps[schema_idx][colId] <
                     chunk.ColumnCount());
            chunk.data[state.outer_col_maps[schema_idx][colId]].Slice(
                input.data[colId], state.rhs_sel, state.output_idx);
        }
    }
    D_ASSERT(state.output_idx <= STANDARD_VECTOR_SIZE);
    chunk.SetCardinality(state.output_idx);
}

OperatorResultType PhysicalAdjIdxJoin::ExecuteRangedInput(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate) const
{
    D_ASSERT(false);
    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalAdjIdxJoin::Execute(ExecutionContext &context,
                                               DataChunk &input,
                                               DataChunk &chunk,
                                               OperatorState &lstate) const
{
    num_loops++;
    if (!is_adjidxjoin_into) {
        return ExecuteNaiveInput(context, input, chunk, lstate);
    }
    else {
        return ExecuteNaiveInputInto(context, input, chunk, lstate);
    }
}

OperatorResultType PhysicalAdjIdxJoin::CheckIterationState(
    AdjIdxJoinState &state, idx_t input_size) const
{
    if (state.lhs_idx >= input_size) {
        state.join_finished = true;
        state.resetForNewInput();
        return OperatorResultType::NEED_MORE_INPUT;
    }
    else {
        state.resetForMoreOutput();
        return OperatorResultType::HAVE_MORE_OUTPUT;
    }
}

std::string PhysicalAdjIdxJoin::ParamsToString() const
{
    std::string result = "";
    result += JoinTypeToString(join_type) + ", ";
    if (is_adjidxjoin_into)
        result += "INTO, ";
    result += "adjidx_obj_id=" + std::to_string(adjidx_obj_id) + ", ";
    result += "sid_col_idx=" + std::to_string(sid_col_idx) + ", ";
    result +=
        "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
    result +=
        "inner_col_map.size()=" + std::to_string(inner_col_map.size()) + ", ";
    result += "discard_tgt=" + std::to_string(discard_tgt) + ", ";
    return result;
}

std::string PhysicalAdjIdxJoin::ToString() const
{
    return "AdjIdxJoin";
}

}  // namespace duckdb
