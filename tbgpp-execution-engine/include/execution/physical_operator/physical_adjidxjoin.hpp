#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

#include <boost/timer/timer.hpp>
#include <unordered_set>

#include <cassert>

namespace duckdb {

class PhysicalAdjIdxJoin: public CypherPhysicalOperator {

public:
	//230303 TODO need to change this....
		// adjidx_obj_id => multiple objects
	PhysicalAdjIdxJoin(CypherSchema& sch, uint64_t adjidx_obj_id, JoinType join_type, uint64_t sid_col_idx, bool load_eid,
					   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map) 
		: CypherPhysicalOperator(sch), adjidx_obj_id(adjidx_obj_id), join_type(join_type), sid_col_idx(sid_col_idx), load_eid(load_eid),
			enumerate(true), remaining_conditions(move(vector<JoinCondition>())), outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map))
		{ 
			if(load_eid) {
				D_ASSERT(this->inner_col_map.size() == 2);	// inner = (eid, tid)
			} else {
				D_ASSERT(this->inner_col_map.size() == 1);	// inner = (tid)
			}
		}

	~PhysicalAdjIdxJoin() {}

public:
	
	// common interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;
	// locally used functions
	OperatorResultType ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	OperatorResultType ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const;
	void ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate, bool is_left_join) const;
	void ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	uint64_t adjidx_obj_id;	// 230303 current single adjidx object
	uint64_t sid_col_idx;	// source id column

	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;
	
	JoinType join_type;

	// remaining join conditions
	vector<JoinCondition> remaining_conditions;

	bool load_eid;
	bool enumerate;

private:
	// inline void fillOutputChunk(AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null) const {
	// 	// set sel vector on lhs	// TODO apply filter predicates
	// 	auto tmp_output_idx = state.output_idx;	// do not alter output_idx here
	// 	for( ; tmp_output_idx < state.output_idx + num_rhs_to_try_fetch ; tmp_output_idx++ ) {
	// 		state.rhs_sel.set_index(tmp_output_idx, state.lhs_idx);
	// 	}

	// 	// produce rhs (update output_idx and rhs_idx)	// TODO apply predicate : use other than for statement
	// 	auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
	// 	if (!fill_null) {
	// 		for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
	// 			tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
	// 			if (load_eid) {
	// 				D_ASSERT(eid_adj_column != nullptr);
	// 				eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
	// 			}
	// 			state.output_idx++;
	// 		}
	// 		D_ASSERT(tmp_output_idx == state.output_idx);
	// 	} else {
	// 		for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
	// 			tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
	// 			if (load_eid) {
	// 				D_ASSERT(eid_adj_column != nullptr);
	// 				eid_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
	// 			}
	// 			state.output_idx++;
	// 		}
	// 		D_ASSERT(tmp_output_idx == state.output_idx);
	// 	}
	// }
};

}