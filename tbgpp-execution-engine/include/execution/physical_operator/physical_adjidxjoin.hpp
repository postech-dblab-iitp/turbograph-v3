#pragma once
#include "typedef.hpp"

#include "common/enums/join_type.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "extent/adjlist_iterator.hpp"
#include "planner/joinside.hpp"

#include <boost/timer/timer.hpp>
#include <unordered_set>

#include <cassert>
#include <tuple>

namespace duckdb {

class AdjIdxJoinState : public OperatorState {
   public:
    explicit AdjIdxJoinState(JoinType join_type) : join_type(join_type)
    {
        adj_it = new AdjacencyListIterator();
        rhs_sel.Initialize(STANDARD_VECTOR_SIZE);
        src_nullity.resize(STANDARD_VECTOR_SIZE);
        prev_eid = std::numeric_limits<ExtentID>::max();
        found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
        resetForNewInput();
    }
    //! Called when starting processing for new chunk
    inline void resetForNewInput()
    {
        resetForMoreOutput();
        first_fetch = false;
        join_finished = false;
        all_adjs_null = true;
        lhs_idx = 0;
        adj_idx = 0;
        rhs_idx = 0;
        // init vectors
        // adj_col_idxs.clear();
        // adj_col_types.clear();
        // std::fill(total_join_size.begin(), total_join_size.end(), 0);

        if (join_type != JoinType::INNER) {
            memset(found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
        }
    }
    inline void resetForMoreOutput() { output_idx = 0; }

   public:
    JoinType join_type;
    // operator data
    AdjacencyListIterator *adj_it;
    ExtentID prev_eid;

    // input -> output col mapping information
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> outer_col_maps;
    vector<uint32_t> inner_col_map;

    // join state - initialized per output
    idx_t output_idx;
    idx_t output_idx_before_fetch;

    // join state - initialized per new input, and updated while processing
    bool first_fetch;
    idx_t lhs_idx;
    idx_t adj_idx;
    idx_t rhs_idx;
    bool all_adjs_null;
    bool join_finished;  // equi join match finished

    // join metadata - initialized per new input
    vector<int> adj_col_idxs;           // indices
    vector<LogicalType> adj_col_types;  // forward_adj or backward_adj

    // join data - initialized per new input
    vector<bool> src_nullity;
    vector<vector<idx_t>>
        join_sizes;  // can be multiple when there are many adjlists per vertex
    vector<idx_t> total_join_size;  // sum of entries in join_sizes

    SelectionVector rhs_sel;  // used multiple times without initialization
    // whether or not the given tuple has found a match
    unique_ptr<bool[]> found_match;
};

class PhysicalAdjIdxJoin : public CypherPhysicalOperator {

   public:
    PhysicalAdjIdxJoin(Schema &sch, uint64_t adjidx_obj_id, JoinType join_type,
                       bool is_adjidxjoin_into, uint64_t sid_col_idx,
                       uint64_t tgt_col_idx, vector<uint32_t> &outer_col_map,
                       vector<uint32_t> &inner_col_map, AdjIdxIdIdxs &id_idxs)
        : CypherPhysicalOperator(PhysicalOperatorType::ADJ_IDX_JOIN, sch),
          adjidx_obj_id(adjidx_obj_id),
          join_type(join_type),
          is_adjidxjoin_into(is_adjidxjoin_into),
          sid_col_idx(sid_col_idx),
          tgt_col_idx(tgt_col_idx),
          inner_col_map(move(inner_col_map))
    {
        this->outer_col_maps.push_back(std::move(outer_col_map));

        auto edge_id_inner_map_idx = std::get<0>(id_idxs);
        auto src_id_inner_map_idx = std::get<1>(id_idxs);
        auto tgt_id_inner_map_idx = std::get<2>(id_idxs);

        load_eid = edge_id_inner_map_idx != -1;
        load_sid = src_id_inner_map_idx != -1;
        load_tid = tgt_id_inner_map_idx != -1;

        D_ASSERT((load_eid ? 1 : 0) + (load_sid ? 1 : 0) + (load_tid ? 1 : 0) ==
                 this->inner_col_map.size());

        if (load_eid) {
            edgeColIdx = this->inner_col_map[edge_id_inner_map_idx];
        }
        if (load_sid) {
            srcColIdx = this->inner_col_map[src_id_inner_map_idx];
        }
        if (load_tid) {
            tgtColIdx = this->inner_col_map[tgt_id_inner_map_idx];
            discard_tgt = tgtColIdx == std::numeric_limits<uint32_t>::max();
        }

        if (!is_adjidxjoin_into) {
            setFillFunc();
        }
        else {
            setFillFuncInto();
        }
    }

    ~PhysicalAdjIdxJoin() {}

   public:
    // common interface
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk,
                               OperatorState &state) const override;
    // locally used functions
    OperatorResultType ExecuteRangedInput(ExecutionContext &context,
                                          DataChunk &input, DataChunk &chunk,
                                          OperatorState &lstate) const;
    OperatorResultType ExecuteNaiveInput(ExecutionContext &context,
                                         DataChunk &input, DataChunk &chunk,
                                         OperatorState &lstate) const;
    OperatorResultType ExecuteNaiveInputInto(ExecutionContext &context,
                                             DataChunk &input, DataChunk &chunk,
                                             OperatorState &lstate) const;
    void GetJoinMatches(ExecutionContext &context, DataChunk &input,
                        AdjIdxJoinState &state) const;
    OperatorResultType ProcessSemiAntiJoin(ExecutionContext &context,
                                           DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate) const;
    OperatorResultType ProcessEquiJoin(ExecutionContext &context,
                                       DataChunk &input, DataChunk &chunk,
                                       OperatorState &lstate) const;
    OperatorResultType ProcessEquiJoinInto(ExecutionContext &context,
                                           DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate) const;
    OperatorResultType ProcessLeftJoin(ExecutionContext &context,
                                       DataChunk &input, DataChunk &chunk,
                                       OperatorState &lstate) const;
    OperatorResultType ProcessLeftJoinInto(ExecutionContext &context,
                                           DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate) const;
    void IterateSourceVidsAndFillRHSOutput(
        ExecutionContext &context, AdjIdxJoinState &state, DataChunk &input,
        DataChunk &chunk, uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
        uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
        ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const;
    void IterateSourceVidsAndFillRHSOutputInto(
        ExecutionContext &context, AdjIdxJoinState &state, DataChunk &input,
        DataChunk &chunk, uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
        uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
        ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const;
    inline void GetAdjListAndFillRHSOutput(
        ExecutionContext &context, AdjIdxJoinState &state, uint64_t src_vid,
        uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
        uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
        ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const;
    inline void GetAdjListAndFillRHSOutputInto(
        ExecutionContext &context, AdjIdxJoinState &state, uint64_t src_vid,
        uint64_t tgt_vid, uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
        uint64_t *&eid_adj_column, ValidityMask *tgt_validity_mask,
        ValidityMask *eid_validity_mask, ExpandDirection &cur_direction) const;
    inline void AdvanceToNextLHS(AdjIdxJoinState &state, uint64_t *adj_start,
                                 uint64_t *adj_end, uint64_t *&tgt_adj_column,
                                 uint64_t *&eid_adj_column,
                                 ValidityMask *tgt_validity_mask,
                                 ValidityMask *eid_validity_mask) const;
    void FillLHSOutput(AdjIdxJoinState &state, DataChunk &input,
                       DataChunk &chunk) const;
    void InitializeAdjIdxJoin(AdjIdxJoinState &state, ExecutionContext &context,
                              DataChunk &input, DataChunk &chunk) const;
    void InitializeInfosForProcessing(
        AdjIdxJoinState &state, DataChunk &input, DataChunk &chunk,
        uint64_t *&src_adj_column, uint64_t *&tgt_adj_column,
        uint64_t *&eid_adj_column, ValidityMask *&tgt_validity_mask,
        ValidityMask *&eid_validity_mask, ExpandDirection &cur_direction) const;
    OperatorResultType CheckIterationState(AdjIdxJoinState &state,
                                           idx_t input_size) const;

    std::string ParamsToString() const override;
    std::string ToString() const override;

    virtual size_t GetLoopCount() const override { return num_loops; }

    uint64_t adjidx_obj_id;  // 230303 current single adjidx object
    uint64_t sid_col_idx;    // source id column
    uint64_t tgt_col_idx;

    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> outer_col_maps;
    vector<uint32_t> inner_col_map;

    JoinType join_type;

    bool load_eid;
    bool load_sid;
    bool load_tid;

    idx_t edgeColIdx;
    idx_t srcColIdx;
    idx_t tgtColIdx;

    bool discard_tgt;
    bool is_adjidxjoin_into;
    std::function<void(AdjIdxJoinState &, uint64_t *, uint64_t *, uint64_t *,
                       ValidityMask *, ValidityMask *, size_t, bool)>
        fillFunc;
    std::function<void(AdjIdxJoinState &, uint64_t *, uint64_t *, uint64_t *,
                       ValidityMask *, ValidityMask *, uint64_t, size_t, bool)>
        fillFuncInto;

    uint64_t *inner_vec;

    mutable uint64_t num_loops = 0;

   private:
    void setFillFuncLoadEID()
    {
        // load eid & tgt
        if (discard_tgt) {
            // discard_tgt
            fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                              uint64_t *tgt_adj_column,
                              uint64_t *eid_adj_column,
                              ValidityMask *tgt_validity_mask,
                              ValidityMask *eid_validity_mask,
                              size_t num_rhs_to_try_fetch, bool fill_null) {
                D_ASSERT(!load_eid || (eid_adj_column != nullptr));
                if (unlikely(fill_null)) {
                    D_ASSERT(eid_validity_mask != nullptr);
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        eid_adj_column[state.output_idx] = 0;
                        eid_validity_mask->SetInvalid(state.output_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        eid_adj_column[state.output_idx] =
                            adj_start[state.rhs_idx * 2 + 1];
                        state.output_idx++;
                    }
                }
            };
        }
        else {
            // do not discard tgt
            fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                              uint64_t *tgt_adj_column,
                              uint64_t *eid_adj_column,
                              ValidityMask *tgt_validity_mask,
                              ValidityMask *eid_validity_mask,
                              size_t num_rhs_to_try_fetch, bool fill_null) {
                D_ASSERT(!load_eid || (eid_adj_column != nullptr));
                if (unlikely(fill_null)) {
                    D_ASSERT(tgt_validity_mask != nullptr);
                    D_ASSERT(eid_validity_mask != nullptr);
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        tgt_adj_column[state.output_idx] = 0;
                        eid_adj_column[state.output_idx] = 0;
                        tgt_validity_mask->SetInvalid(state.output_idx);
                        eid_validity_mask->SetInvalid(state.output_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        tgt_adj_column[state.output_idx] =
                            adj_start[state.rhs_idx * 2];
                        eid_adj_column[state.output_idx] =
                            adj_start[state.rhs_idx * 2 + 1];
                        state.output_idx++;
                    }
                }
            };
        }
    }

    void setFillFuncLoadTGTOnly()
    {
        // load tgt only
        if (discard_tgt) {
            fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                              uint64_t *tgt_adj_column,
                              uint64_t *eid_adj_column,
                              ValidityMask *tgt_validity_mask,
                              ValidityMask *eid_validity_mask,
                              size_t num_rhs_to_try_fetch, bool fill_null) {
                if (unlikely(fill_null)) {
                    D_ASSERT(false);
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        state.output_idx++;
                    }
                }
            };
        }
        else {
            fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                              uint64_t *tgt_adj_column,
                              uint64_t *eid_adj_column,
                              ValidityMask *tgt_validity_mask,
                              ValidityMask *eid_validity_mask,
                              size_t num_rhs_to_try_fetch, bool fill_null) {
                if (unlikely(fill_null)) {
                    D_ASSERT(tgt_validity_mask != nullptr);
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        tgt_adj_column[state.output_idx] = 0;
                        tgt_validity_mask->SetInvalid(state.output_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        tgt_adj_column[state.output_idx] =
                            adj_start[state.rhs_idx * 2];
                        state.output_idx++;
                    }
                }
            };
        }
    }

    void setFillFuncIntoLoadEID()
    {
        // load eid & tgt
        if (discard_tgt) {
            // discard_tgt
            fillFuncInto = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                                  uint64_t *tgt_adj_column,
                                  uint64_t *eid_adj_column,
                                  ValidityMask *tgt_validity_mask,
                                  ValidityMask *eid_validity_mask,
                                  uint64_t tgt_vid_into,
                                  size_t num_rhs_to_try_fetch, bool fill_null) {
                D_ASSERT(!load_eid || (eid_adj_column != nullptr));
                if (unlikely(fill_null)) {
                    D_ASSERT(eid_validity_mask != nullptr);
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        eid_adj_column[state.output_idx] = 0;
                        eid_validity_mask->SetInvalid(state.output_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        if (adj_start[state.rhs_idx * 2] == tgt_vid_into) {
                            state.rhs_sel.set_index(state.output_idx,
                                                    state.lhs_idx);
                            eid_adj_column[state.output_idx] =
                                adj_start[state.rhs_idx * 2 + 1];
                            state.output_idx++;
                        }
                    }
                }
            };
        }
        else {
            // do not discard tgt
            fillFuncInto = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                                  uint64_t *tgt_adj_column,
                                  uint64_t *eid_adj_column,
                                  ValidityMask *tgt_validity_mask,
                                  ValidityMask *eid_validity_mask,
                                  uint64_t tgt_vid_into,
                                  size_t num_rhs_to_try_fetch, bool fill_null) {
                D_ASSERT(!load_eid || (eid_adj_column != nullptr));
                if (unlikely(fill_null)) {
                    D_ASSERT(tgt_validity_mask != nullptr);
                    D_ASSERT(eid_validity_mask != nullptr);
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        tgt_adj_column[state.output_idx] = 0;
                        eid_adj_column[state.output_idx] = 0;
                        tgt_validity_mask->SetInvalid(state.output_idx);
                        eid_validity_mask->SetInvalid(state.output_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        if (adj_start[state.rhs_idx * 2] == tgt_vid_into) {
                            state.rhs_sel.set_index(state.output_idx,
                                                    state.lhs_idx);
                            tgt_adj_column[state.output_idx] =
                                adj_start[state.rhs_idx * 2];
                            eid_adj_column[state.output_idx] =
                                adj_start[state.rhs_idx * 2 + 1];
                            state.output_idx++;
                        }
                    }
                }
            };
        }
    }

    void setFillFuncIntoLoadTGTOnly()
    {
        // load tgt only
        if (discard_tgt) {
            fillFuncInto = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                                  uint64_t *tgt_adj_column,
                                  uint64_t *eid_adj_column,
                                  ValidityMask *tgt_validity_mask,
                                  ValidityMask *eid_validity_mask,
                                  uint64_t tgt_vid_into,
                                  size_t num_rhs_to_try_fetch, bool fill_null) {
                if (unlikely(fill_null)) {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        if (adj_start[state.rhs_idx * 2] == tgt_vid_into) {
                            state.rhs_sel.set_index(state.output_idx,
                                                    state.lhs_idx);
                            state.output_idx++;
                        }
                    }
                }
            };
        }
        else {
            fillFuncInto = [this](AdjIdxJoinState &state, uint64_t *adj_start,
                                  uint64_t *tgt_adj_column,
                                  uint64_t *eid_adj_column,
                                  ValidityMask *tgt_validity_mask,
                                  ValidityMask *eid_validity_mask,
                                  uint64_t tgt_vid_into,
                                  size_t num_rhs_to_try_fetch, bool fill_null) {
                if (unlikely(fill_null)) {
                    D_ASSERT(tgt_validity_mask != nullptr);
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        state.rhs_sel.set_index(state.output_idx,
                                                state.lhs_idx);
                        tgt_adj_column[state.output_idx] = 0;
                        tgt_validity_mask->SetInvalid(state.output_idx);
                        state.output_idx++;
                    }
                }
                else {
                    auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
                    for (; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
                        if (adj_start[state.rhs_idx * 2] == tgt_vid_into) {
                            state.rhs_sel.set_index(state.output_idx,
                                                    state.lhs_idx);
                            tgt_adj_column[state.output_idx] =
                                adj_start[state.rhs_idx * 2];
                            state.output_idx++;
                        }
                    }
                }
            };
        }
    }

    void setFillFunc()
    {
        if (load_eid) {
            setFillFuncLoadEID();
        }
        else {
            setFillFuncLoadTGTOnly();
        }
    }

    void setFillFuncInto()
    {
        if (load_eid) {
            setFillFuncIntoLoadEID();
        }
        else {
            setFillFuncIntoLoadTGTOnly();
        }
    }
};

}  // namespace duckdb