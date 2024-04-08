#pragma once
#include "typedef.hpp"

#include "execution/expression_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "common/types/value.hpp"

namespace duckdb {

class PartialSchema;
class IdSeekState;
class PhysicalIdSeek : public CypherPhysicalOperator {

   public:
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   JoinType join_type = JoinType::INNER);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   bool is_output_union_schema,
                   JoinType join_type = JoinType::INNER);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<unique_ptr<Expression>> predicates,
                   JoinType join_type = JoinType::INNER);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<duckdb::LogicalType> scan_type,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   int64_t filterKeyIndex, duckdb::Value filterValue,
                   JoinType join_type = JoinType::INNER);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   vector<unique_ptr<Expression>> &predicates,
                   vector<vector<idx_t>> &pred_col_idxs_per_schema,
                   bool is_output_union_schema,
                   JoinType join_type = JoinType::INNER);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   vector<vector<unique_ptr<Expression>>> &predicates,
                   vector<vector<idx_t>> &pred_col_idxs_per_schema,
                   bool is_output_union_schema,
                   JoinType join_type = JoinType::INNER);
    ~PhysicalIdSeek() {
        for (auto &chunk : tmp_chunks) {
            chunk.reset();
        }
    }

   public:
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
    virtual void InitializeOutputChunks(
        std::vector<unique_ptr<DataChunk>> &output_chunks,
        Schema &output_schema, idx_t idx) override;

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
    void doSeekUnionAll(ExecutionContext &context, DataChunk &input,
                        DataChunk &chunk, OperatorState &lstate,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs, idx_t &output_idx) const;
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
    void referInputChunk(DataChunk &input, DataChunk &chunk,
                         OperatorState &lstate, idx_t output_idx) const;
    void referInputChunkLeft(DataChunk &input, DataChunk &chunk,
                         OperatorState &lstate, idx_t output_idx) const;
    OperatorResultType referInputChunks(DataChunk &input,
                          vector<unique_ptr<DataChunk>> &chunks,
                          IdSeekState &state,
                          vector<idx_t> &num_tuples_per_chunk,
                          idx_t &output_chunk_idx) const;
    OperatorResultType referInputChunksLeft(DataChunk &input,
                          vector<unique_ptr<DataChunk>> &chunks,
                          IdSeekState &state,
                          vector<idx_t> &num_tuples_per_chunk,
                          idx_t &output_chunk_idx) const;
    void generatePartialSchemaInfos();
    void getOutputTypesForFilteredSeek(vector<LogicalType>& lhs_type, vector<LogicalType>& scan_type,  vector<LogicalType> &out_type) const;
    void getOutputIdxsForFilteredSeek(idx_t chunk_idx, vector<idx_t>& output_col_idx) const;
    void getFilteredTargetSeqno(vector<idx_t>& seqno_to_eid_idx, size_t num_extents, const sel_t* sel_idxs, size_t count, vector<vector<uint32_t>>& out_seqnos) const;
    void fillSeqnoToEIDIdx(vector<vector<uint32_t>>& target_seqnos_per_extent, vector<idx_t>& seqno_to_eid_idx) const;
    void genNonPredColIdxs();
    void getReverseMappingIdxs(size_t num_chunks, idx_t base_chunk_idx, vector<idx_t>& mapping_idxs, vector<vector<idx_t>>& reverse_mapping_idxs) const;
    void remapSeqnoToEidIdx(vector<idx_t>& in_seqno_to_eid_idx, const sel_t* sel_idxs, size_t sel_size, vector<idx_t>& out_seqno_to_eid_idx) const;

    // parameters
    uint64_t id_col_idx;
    mutable vector<uint64_t> oids;
    mutable vector<vector<uint64_t>> projection_mapping;

    // used to initialize output chunks.
    mutable vector<LogicalType> target_types;

    mutable vector<LogicalType> scan_type;           // types scan
    mutable vector<vector<LogicalType>> scan_types;  // types scan
    // projection mapping for scan from the storage
    mutable vector<vector<uint64_t>> scan_projection_mapping;
    
    // filter processing
    bool do_filter_pushdown;
    bool has_unpushdowned_expressions = false;
    // mutable bool is_tmp_chunk_initialized = false;
    mutable vector<bool> is_tmp_chunk_initialized_per_schema;
    // when negative, no filter pushdown
    mutable int64_t filter_pushdown_key_idx;
    // do not use when filter_pushdown_key_idx < 0
    mutable Value filter_pushdown_value;

    vector<vector<uint32_t>> outer_col_maps;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<PartialSchema> partial_schemas;
    bool is_output_union_schema = true;
    idx_t num_total_schemas = 1;

    mutable vector<vector<idx_t>> pred_col_idxs_per_schema;
    mutable vector<vector<idx_t>> non_pred_col_idxs_per_schema;
    vector<unique_ptr<DataChunk>> tmp_chunks;
    vector<unique_ptr<Expression>> expressions;
    mutable vector<ExpressionExecutor> executors;

    mutable vector<ExtentID> target_eids;

    JoinType join_type;
};

}  // namespace duckdb