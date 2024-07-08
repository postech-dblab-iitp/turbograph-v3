#pragma once
#include "typedef.hpp"

#include "common/enums/join_type.hpp"
#include "common/types/value.hpp"
#include "execution/expression_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

typedef double NullRatio;
typedef double Skewness;

class PartialSchema;
class IdSeekState;
class PhysicalIdSeek : public CypherPhysicalOperator {
   private:
    enum class OutputFormat { GROUPING = 0, UNIONALL = 1, ROW = 2 };

   public:
    // Constructor without filter pushdown
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   bool force_output_union,
                   JoinType join_type = JoinType::INNER);

    // Constructor with filter pushdown
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   vector<vector<unique_ptr<Expression>>> &predicates,
                   vector<vector<idx_t>> &pred_col_idxs_per_schema,
                   bool force_output_union,
                   JoinType join_type = JoinType::INNER);
    ~PhysicalIdSeek()
    {
        for (auto &chunk : tmp_chunks) {
            chunk.reset();
        }
    }

   public:
    virtual void InitializeOutputChunks(
        std::vector<unique_ptr<DataChunk>> &output_chunks,
        Schema &output_schema, idx_t idx) override;
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk,
                               OperatorState &state) const override;
    OperatorResultType ExecuteInner(ExecutionContext &context, DataChunk &input,
                                    DataChunk &chunk,
                                    OperatorState &state) const;
    OperatorResultType ExecuteLeft(ExecutionContext &context, DataChunk &input,
                                   DataChunk &chunk,
                                   OperatorState &state) const;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               vector<unique_ptr<DataChunk>> &chunks,
                               OperatorState &state,
                               idx_t &output_chunk_idx) const override;
    OperatorResultType ExecuteInner(ExecutionContext &context, DataChunk &input,
                                    vector<unique_ptr<DataChunk>> &chunks,
                                    OperatorState &state,
                                    idx_t &output_chunk_idx) const;
    OperatorResultType ExecuteLeft(ExecutionContext &context, DataChunk &input,
                                   vector<unique_ptr<DataChunk>> &chunks,
                                   OperatorState &state,
                                   idx_t &output_chunk_idx) const;

    std::string ParamsToString() const override;
    std::string ToString() const override;

    // internal functions
    void initializeSeek(ExecutionContext &context, DataChunk &input,
                        vector<unique_ptr<DataChunk>> &chunks,
                        IdSeekState &state, idx_t nodeColIdx,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs,
                        vector<idx_t> &num_tuples_per_chunk) const;
    void initializeSeek(ExecutionContext &context, DataChunk &input,
                        DataChunk &chunk, IdSeekState &state, idx_t nodeColIdx,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs) const;
    void doSeekUnionAll(ExecutionContext &context, DataChunk &input,
                        DataChunk &chunk, OperatorState &lstate,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs, idx_t &output_size) const;
    void doSeekSchemaless(ExecutionContext &context, DataChunk &input,
                          DataChunk &chunk, OperatorState &lstate,
                          vector<ExtentID> &target_eids,
                          vector<vector<uint32_t>> &target_seqnos_per_extent,
                          vector<idx_t> &mapping_idxs, idx_t &output_idx) const;
    void doSeekGrouping(ExecutionContext &context, DataChunk &input,
                        vector<unique_ptr<DataChunk>> &chunks,
                        IdSeekState &state, idx_t nodeColIdx,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs,
                        vector<idx_t> &num_tuples_per_chunk) const;
    OperatorResultType referInputChunk(DataChunk &input, DataChunk &chunk,
                                       OperatorState &lstate,
                                       idx_t output_size) const;
    OperatorResultType referInputChunkLeft(DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate,
                                           idx_t output_idx) const;
    OperatorResultType referInputChunks(DataChunk &input,
                                        vector<unique_ptr<DataChunk>> &chunks,
                                        IdSeekState &state,
                                        vector<idx_t> &num_tuples_per_chunk,
                                        idx_t &output_chunk_idx) const;
    OperatorResultType referInputChunksLeft(
        DataChunk &input, vector<unique_ptr<DataChunk>> &chunks,
        IdSeekState &state, vector<idx_t> &num_tuples_per_chunk,
        idx_t &output_chunk_idx) const;

   private:
    OperatorResultType moveToNextOutputChunk(
        vector<unique_ptr<DataChunk>> &chunks, OperatorState &lstate,
        idx_t &output_chunk_idx) const;
    void markInvalidForUnseekedColumns(
        DataChunk &chunk, IdSeekState &state, vector<ExtentID> &target_eids,
        vector<vector<uint32_t>> &target_seqnos_per_extent,
        vector<idx_t> &mapping_idxs) const;
    void generatePartialSchemaInfos();
    void getOutputTypesForFilteredSeek(vector<LogicalType> &lhs_type,
                                       vector<LogicalType> &scan_type,
                                       vector<LogicalType> &out_type) const;
    void getOutputIdxsForFilteredSeek(idx_t chunk_idx,
                                      vector<idx_t> &output_col_idx) const;
    void getFilteredTargetSeqno(vector<idx_t> &seqno_to_eid_idx,
                                size_t num_extents, const sel_t *sel_idxs,
                                size_t count,
                                vector<vector<uint32_t>> &out_seqnos) const;
    void fillSeqnoToEIDIdx(vector<vector<uint32_t>> &target_seqnos_per_extent,
                           vector<idx_t> &seqno_to_eid_idx) const;
    void genNonPredColIdxs();
    void getReverseMappingIdxs(
        size_t num_chunks, idx_t base_chunk_idx, vector<idx_t> &mapping_idxs,
        vector<vector<idx_t>> &reverse_mapping_idxs) const;
    void remapSeqnoToEidIdx(vector<idx_t> &in_seqno_to_eid_idx,
                            const sel_t *sel_idxs, size_t sel_size,
                            vector<idx_t> &out_seqno_to_eid_idx) const;
    size_t calculateTotalNulls(
        DataChunk &chunk,
        vector<ExtentID> &target_eids,
        vector<vector<uint32_t>> &target_seqnos_per_extent,
        vector<idx_t> &mapping_idxs) const;
    OutputFormat determineFormatByCostModel(
        bool sort_order_enforced, size_t total_nulls) const;
    void getOutputColIdxsForOuter(vector<idx_t> &output_col_idx) const;
    void getOutputColIdxsForInner(idx_t extentIdx, vector<idx_t> &mapping_idxs,
                                  vector<idx_t> &output_col_idx) const;
    void getColMapWithoutID(const vector<uint32_t> &col_map,
                            vector<LogicalType> &types, idx_t &out_id_col_idx,
                            vector<uint32_t> &out_col_map) const;
    void fillOutSizePerSchema(vector<ExtentID> &target_eids,
                             vector<vector<uint32_t>> &target_seqnos_per_extent,
                             vector<idx_t> &mapping_idxs) const;
    void getUnionScanTypes();
    void buildExpressionExecutors(vector<vector<unique_ptr<Expression>>> &predicates);

    // parameters
    uint64_t id_col_idx;
    mutable vector<uint64_t> oids;
    mutable vector<LogicalType> union_scan_type;
    mutable vector<vector<LogicalType>> scan_types;  // types scan
    mutable vector<vector<uint64_t>> projection_mapping;
    mutable vector<vector<uint64_t>> scan_projection_mapping;

    mutable vector<idx_t> num_tuples_per_schema;
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<uint32_t> union_inner_col_map_wo_id;
    vector<PartialSchema> partial_schemas;
    mutable vector<ExtentID> target_eids;
    bool force_output_union = true;
    idx_t num_outer_schemas = 1;
    idx_t num_inner_schemas = 1;
    idx_t num_total_schemas = 1;
    idx_t out_id_col_idx;

    // filter processing
    bool do_filter_pushdown;
    vector<unique_ptr<Expression>> expressions;
    mutable vector<ExpressionExecutor> executors;
    vector<unique_ptr<DataChunk>> tmp_chunks;
    mutable vector<bool> is_tmp_chunk_initialized_per_schema;
    mutable vector<vector<idx_t>> pred_col_idxs_per_schema;
    mutable vector<vector<idx_t>> non_pred_col_idxs_per_schema;

    JoinType join_type;
};

}  // namespace duckdb