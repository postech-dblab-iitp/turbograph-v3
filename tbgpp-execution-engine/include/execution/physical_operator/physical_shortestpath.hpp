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
			id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping), scan_type(scan_type), scan_projection_mapping(scan_projection_mapping)
		{ 
			/* parameters for IdSeek */
			for (int col_idx = 0; col_idx < this->inner_col_map.size(); col_idx++) {
				target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
			}

			D_ASSERT(oids.size() == projection_mapping.size());
			for (auto i = 0; i < oids.size(); i++) {
				ps_oid_to_projection_mapping.insert({oids[i], i});
			}

			// May need to implement tmp_chunk_mapping?
			
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

			executor.AddExpression(*expression);
		}
	
public:
	// SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const override;
	// unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	// void Combine(ExecutionContext& context, LocalSinkState& lstate) const override;
	bool IsSink() const override { return true; }

public:
	// void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const;
	// unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
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

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
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
	mutable vector<LogicalType> scan_type;
	mutable vector<vector<uint64_t>> scan_projection_mapping;
	mutable vector<LogicalType> target_types;
	mutable unordered_map<idx_t, idx_t> ps_oid_to_projection_mapping;

	/* parameters for Filtering */
	unique_ptr<Expression> expression;
	mutable ExpressionExecutor executor;
};

} // namespace duckdb

#endif