#ifndef PHYSICAL_ALLSHORTESTPATH_H
#define PHYSICAL_ALLSHORTESTPATH_H

#include "typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "planner/expression.hpp"
#include "storage/extent/adjlist_iterator.hpp"

namespace duckdb {

class PhysicalAllShortestPathJoin : public CypherPhysicalOperator {

public:
    PhysicalAllShortestPathJoin(Schema &sch, 
                                uint64_t adjidx_obj_id_fwd,
                                uint64_t adjidx_obj_id_bwd,
                                vector<uint32_t> &input_col_map,
                                duckdb::idx_t output_idx,
                                duckdb::idx_t src_id_idx, duckdb::idx_t dst_id_idx,
                                uint64_t lower_bound, uint64_t upper_bound)
        : CypherPhysicalOperator(PhysicalOperatorType::ALL_SHORTEST_PATH, sch),
          adjidx_obj_id_fwd(adjidx_obj_id_fwd),
          adjidx_obj_id_bwd(adjidx_obj_id_bwd),
          input_col_map(input_col_map),
          output_idx(output_idx),
          src_id_idx(src_id_idx),
          dst_id_idx(dst_id_idx),
          lower_bound(lower_bound),
          upper_bound(upper_bound) {}

    ~PhysicalAllShortestPathJoin() {}

public:
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk,
                               OperatorState &state) const override;

    std::string ParamsToString() const override;
    std::string ToString() const override;

private:
    /* common params */
    vector<uint32_t> input_col_map;
    duckdb::idx_t output_idx;

    uint64_t adjidx_obj_id_fwd;
    uint64_t adjidx_obj_id_bwd;

    duckdb::idx_t src_id_idx;
    duckdb::idx_t dst_id_idx;

    uint64_t lower_bound;
    uint64_t upper_bound;
};

} // namespace duckdb

#endif
