#ifndef PHYSICAL_SHORTESTPATH_H
#define PHYSICAL_SHORTESTPATH_H

#include "typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

namespace duckdb {

class PhysicalShortestPath: public CypherPhysicalOperator {

public:
    ~PhysicalShortestPath() {}

	PhysicalShortestPath(
						/* common params */
						Schema& sch, vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map,
						/* parameters for varlengthexpand */
						uint64_t adjidx_obj_id, JoinType join_type, uint64_t sid_col_idx, bool load_eid, 
						uint64_t min_length, uint64_t max_length,
						/* parameters for IdSeek */
						uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping, 
						std::vector<duckdb::LogicalType> scan_type, vector<vector<uint64_t>> scan_projection_mapping,
						/* parameters for Filtering */
						vector<unique_ptr<Expression>> predicates
					   ) 
		: CypherPhysicalOperator(PhysicalOperatorType::SHORTEST_PATH, sch), 
			/* common params */
			outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)),
			/* parameters for varlengthexpand */
			adjidx_obj_id(adjidx_obj_id), join_type(join_type), sid_col_idx(sid_col_idx), load_eid(load_eid),
			min_length(min_length), max_length(max_length),
			/* parameters for IdSeek */
			id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),scan_projection_mapping(scan_projection_mapping)
		{ 
			this->scan_types.push_back(std::move(scan_type));
			
			/* parameters for Filtering */
			D_ASSERT(predicates.size() > 0);
			if (predicates.size() > 1) {
				auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
				for (auto &expr : predicates) {
					conjunction->children.push_back(move(expr));
				}
				expression = move(conjunction);
			} else {
				expression = move(predicates[0]);
			}
		}
	
public:
	bool IsSink() const override { return true; }

public:
	bool IsSource() const override { return true; }

//     // common interface
// 	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
// 	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;
// 	// locally used functions
// 	OperatorResultType ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
// 	OperatorResultType ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
// 	void GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const;
// 	void ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
// 	void ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
// 	void ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
// 	void ProcessVarlenEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;

// 	std::string ParamsToString() const override;
// 	std::string ToString() const override;

// private:
//     uint64_t VarlengthExpand_internal(ExecutionContext& context, uint64_t src_vid, DataChunk &chunk, OperatorState &lstate, int64_t remaining_output) const;
// 	void addNewPathToOutput(uint64_t *tgt_adj_column, uint64_t *eid_adj_column, uint64_t output_idx, vector<uint64_t> &current_path, uint64_t new_edge_id) const;
// 	bool falsePositiveCheck(vector<uint64_t> &current_path, uint64_t new_edge_id) const;

public:
	/* common params */
	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;

	/* parameters for varlengthexpand */
	uint64_t adjidx_obj_id;	// current single adjidx object
	uint64_t sid_col_idx;	// source id column	
	JoinType join_type;
	bool load_eid;
	uint64_t min_length;
	uint64_t max_length;

	/* parameters for IdSeek */
	uint64_t id_col_idx;
	mutable vector<uint64_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;
	mutable vector<vector<LogicalType>> scan_types;
	mutable vector<vector<uint64_t>> scan_projection_mapping;

	/* parameters for Filtering */
	unique_ptr<Expression> expression;
};

} // namespace duckdb

#endif