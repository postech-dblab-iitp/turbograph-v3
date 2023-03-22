#ifndef PHYSICAL_VARLEN_ADJIDXJOIN_H
#define PHYSICAL_VARLEN_ADJIDXJOIN_H

#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

namespace duckdb {

class PhysicalVarlenAdjIdxJoin: public CypherPhysicalOperator {

public:
    PhysicalVarlenAdjIdxJoin() {}
    ~PhysicalVarlenAdjIdxJoin() {}

	PhysicalVarlenAdjIdxJoin(CypherSchema& sch, uint64_t adjidx_obj_id, JoinType join_type, uint64_t sid_col_idx, bool load_eid,
					   uint64_t min_length, uint64_t max_length, vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map) 
		: CypherPhysicalOperator(sch), adjidx_obj_id(adjidx_obj_id), join_type(join_type), sid_col_idx(sid_col_idx), load_eid(load_eid), min_length(min_length), max_length(max_length),
			/*enumerate(true), remaining_conditions(move(vector<JoinCondition>())),*/ outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map))
		{ }

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
	void ProcessVarlenEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
    uint64_t VarlengthExpand_internal(ExecutionContext& context, uint64_t src_vid, DataChunk &chunk, OperatorState &lstate, int64_t remaining_output) const;
	void addNewPathToOutput(uint64_t *tgt_adj_column, uint64_t *eid_adj_column, uint64_t output_idx, vector<uint64_t> &current_path, uint64_t new_edge_id) const;
	bool falsePositiveCheck(vector<uint64_t> &current_path, uint64_t new_edge_id) const;

	uint64_t min_length;
	uint64_t max_length;

	// from adjidxjoin
	uint64_t adjidx_obj_id;	// 230303 current single adjidx object
	uint64_t sid_col_idx;	// source id column

	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;
	
	JoinType join_type;

	bool load_eid;
};

} // namespace duckdb

#endif