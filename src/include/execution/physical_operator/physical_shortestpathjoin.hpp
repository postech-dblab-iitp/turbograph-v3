#ifndef PHYSICAL_SHORTESTPATH_H
#define PHYSICAL_SHORTESTPATH_H

#include "common/typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "planner/expression.hpp"
#include "storage/extent/adjlist_iterator.hpp"

namespace duckdb {

class PhysicalShortestPathJoin: public CypherPhysicalOperator {

public:
	PhysicalShortestPathJoin(Schema &sch, 
						uint64_t adjidx_obj_id,
                       vector<uint32_t> &input_col_map,
					   duckdb::idx_t output_idx,
					   duckdb::idx_t src_id_idx, duckdb::idx_t dst_id_idx,
					   uint64_t lower_bound, uint64_t upper_bound)
					   : CypherPhysicalOperator(PhysicalOperatorType::SHORTEST_PATH, sch),
					   adjidx_obj_id(adjidx_obj_id),
					   input_col_map(input_col_map),
					   output_idx(output_idx),
					   src_id_idx(src_id_idx),
					   dst_id_idx(dst_id_idx),
					   lower_bound(lower_bound),
					   upper_bound(upper_bound) {}


	PhysicalShortestPathJoin(Schema &sch, 
						uint64_t adjidx_obj_id_fwd,
						uint64_t adjidx_obj_id_bwd,
                       vector<uint32_t> &input_col_map,
					   duckdb::idx_t output_idx,
					   duckdb::idx_t src_id_idx, duckdb::idx_t dst_id_idx,
					   uint64_t lower_bound, uint64_t upper_bound)
					   : CypherPhysicalOperator(PhysicalOperatorType::SHORTEST_PATH, sch),
					   adjidx_obj_id_fwd(adjidx_obj_id_fwd),
					   adjidx_obj_id_bwd(adjidx_obj_id_bwd),
					   input_col_map(input_col_map),
					   output_idx(output_idx),
					   src_id_idx(src_id_idx),
					   dst_id_idx(dst_id_idx),
					   lower_bound(lower_bound),
					   upper_bound(upper_bound) {}
	
    ~PhysicalShortestPathJoin() {}
	
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

	uint64_t adjidx_obj_id;
	uint64_t adjidx_obj_id_fwd;
	uint64_t adjidx_obj_id_bwd;

	duckdb::idx_t src_id_idx;
	duckdb::idx_t dst_id_idx;

	uint64_t lower_bound;
	uint64_t upper_bound;
};

} // namespace duckdb

#endif