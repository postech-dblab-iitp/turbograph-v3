#pragma once
#include "typedef.hpp"

#include "execution/expression_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class PhysicalIdSeek : public CypherPhysicalOperator {

   public:
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<uint32_t> &inner_col_map);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<vector<uint32_t>> &outer_col_maps,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<uint32_t> &inner_col_map,
                   vector<unique_ptr<Expression>> predicates);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<uint32_t> &inner_col_map,
                   vector<duckdb::LogicalType> scan_type,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   int64_t filterKeyIndex, duckdb::Value filterValue);
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<uint32_t> &inner_col_map,
                   vector<duckdb::LogicalType> scan_type,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<unique_ptr<Expression>> predicates);
    ~PhysicalIdSeek() {}

   public:
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk,
                               OperatorState &state) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               vector<unique_ptr<DataChunk>> &chunks,
                               OperatorState &state,
                               idx_t &output_chunk_idx) const override;

    std::string ParamsToString() const override;
    std::string ToString() const override;

    // internal functions
    void doSeekUnionAll(ExecutionContext &context, DataChunk &input,
                        DataChunk &chunk, OperatorState &lstate,
                        vector<ExtentID> &target_eids,
                        vector<vector<idx_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs, idx_t &output_idx) const;
    void doSeekSchemaless(ExecutionContext &context, DataChunk &input,
                          DataChunk &chunk, OperatorState &lstate,
                          vector<ExtentID> &target_eids,
                          vector<vector<idx_t>> &target_seqnos_per_extent,
                          vector<idx_t> &mapping_idxs, idx_t &output_idx) const;
    void referInputChunk(DataChunk &input, DataChunk &chunk,
                         OperatorState &lstate, idx_t output_idx) const;

    // parameters
    uint64_t id_col_idx;
    mutable vector<uint64_t> oids;
    mutable vector<vector<uint64_t>> projection_mapping;
    mutable unordered_map<idx_t, idx_t> ps_oid_to_projection_mapping;

    // used to initialize output chunks.
    mutable vector<LogicalType> target_types;

    mutable vector<LogicalType> scan_type;           // types scan
    mutable vector<vector<LogicalType>> scan_types;  // types scan
    // projection mapping for scan from the storage
    mutable vector<vector<uint64_t>> scan_projection_mapping;
    mutable vector<vector<uint64_t>> tmp_chunk_mapping;

    // filter pushdown
    bool do_filter_pushdown;
    bool has_expression = false;
    mutable bool is_tmp_chunk_initialized = false;
    // when negative, no filter pushdown
    mutable int64_t filter_pushdown_key_idx;
    // do not use when filter_pushdown_key_idx < 0
    mutable Value filter_pushdown_value;

    vector<uint32_t> outer_col_map;
    vector<uint32_t> inner_col_map;
    vector<vector<uint32_t>> outer_col_maps;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;

    unique_ptr<Expression> expression;
    mutable DataChunk tmp_chunk;
    mutable ExpressionExecutor executor;
};

}  // namespace duckdb