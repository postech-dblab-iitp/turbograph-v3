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
	void ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
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
};

}