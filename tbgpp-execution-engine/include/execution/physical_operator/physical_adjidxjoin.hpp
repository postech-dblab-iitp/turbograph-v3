#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"
#include "extent/adjlist_iterator.hpp"

#include <boost/timer/timer.hpp>
#include <unordered_set>

#include <cassert>

namespace duckdb {

class AdjIdxJoinState : public OperatorState {
public:
	explicit AdjIdxJoinState() {
		resetForNewInput();
		adj_it = new AdjacencyListIterator();
		rhs_sel.Initialize(STANDARD_VECTOR_SIZE);
		src_nullity.resize(STANDARD_VECTOR_SIZE);
		join_sizes.resize(STANDARD_VECTOR_SIZE);
		total_join_size.resize(STANDARD_VECTOR_SIZE);
	}
	//! Called when starting processing for new chunk
	inline void resetForNewInput() {
		resetForMoreOutput();
		first_fetch = false;
		join_finished = false;
		all_adjs_null = true;
		lhs_idx = 0;
		adj_idx = 0;
		rhs_idx = 0;
		// init vectors
		// adj_col_idxs.clear();
		// adj_col_types.clear();
		std::fill(total_join_size.begin(), total_join_size.end(), 0);
	}
	inline void resetForMoreOutput() {
		output_idx = 0;
	}
public:
	// operator data
	AdjacencyListIterator *adj_it;
	// initialize rest of operator members
	idx_t srcColIdx;
	idx_t edgeColIdx;
	idx_t tgtColIdx;

	// input -> output col mapping information
	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;
	
	// join state - initialized per output
	idx_t output_idx;

	// join state - initialized per new input, and updated while processing
	bool first_fetch;
	idx_t lhs_idx;
	idx_t adj_idx;
	idx_t rhs_idx;
	bool all_adjs_null;
	bool join_finished;	// equi join match finished
	
	// join metadata - initialized per new input
	vector<int> adj_col_idxs;					// indices
	vector<LogicalType> adj_col_types;			// forward_adj or backward_adj

	// join data - initialized per new input
	vector<bool> src_nullity;
	vector<vector<idx_t>> join_sizes;	// can be multiple when there are many adjlists per vertex
	vector<idx_t> total_join_size;		// sum of entries in join_sizes

	SelectionVector rhs_sel;	// used multiple times without initialization
};

class PhysicalAdjIdxJoin: public CypherPhysicalOperator {

public:
	//230303 TODO need to change this....
		// adjidx_obj_id => multiple objects
	PhysicalAdjIdxJoin(Schema& sch, uint64_t adjidx_obj_id, JoinType join_type, uint64_t sid_col_idx, bool load_eid,
					   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, bool load_eid_temporarily = false)
		: CypherPhysicalOperator(PhysicalOperatorType::ADJ_IDX_JOIN, sch), adjidx_obj_id(adjidx_obj_id), join_type(join_type), sid_col_idx(sid_col_idx), load_eid(load_eid),
			enumerate(true), remaining_conditions(move(vector<JoinCondition>())), outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)),
			load_eid_temporarily(load_eid_temporarily)
	{
		discard_tgt = discard_edge = false;
		if (load_eid) {
			D_ASSERT(this->inner_col_map.size() == 2);	// inner = (tid, eid)
			discard_tgt = (this->inner_col_map[0] == std::numeric_limits<uint32_t>::max());
			discard_edge = (this->inner_col_map[1] == std::numeric_limits<uint32_t>::max());
		} else {
			D_ASSERT(this->inner_col_map.size() == 1);	// inner = (tid)
			discard_tgt = (this->inner_col_map[0] == std::numeric_limits<uint32_t>::max());
			discard_edge = true;
		}
		if (load_eid_temporarily) { D_ASSERT(load_eid); }
		
		setFillFunc();
	}

	PhysicalAdjIdxJoin(Schema& sch, uint64_t adjidx_obj_id, JoinType join_type, uint64_t sid_col_idx, bool load_eid,
					   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, bool do_filter_pushdown,
					   uint32_t outer_pos, uint32_t inner_pos, bool load_eid_temporarily = false)
		: CypherPhysicalOperator(PhysicalOperatorType::ADJ_IDX_JOIN, sch), adjidx_obj_id(adjidx_obj_id), join_type(join_type), sid_col_idx(sid_col_idx), load_eid(load_eid),
			enumerate(true), remaining_conditions(move(vector<JoinCondition>())), outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)),
			do_filter_pushdown(do_filter_pushdown), outer_pos(outer_pos), inner_pos(inner_pos), load_eid_temporarily(load_eid_temporarily)
	{
		discard_tgt = discard_edge = false;
		if (load_eid) {
			D_ASSERT(this->inner_col_map.size() == 2);	// inner = (tid, eid)
			discard_tgt = (this->inner_col_map[0] == std::numeric_limits<uint32_t>::max());
			discard_edge = (this->inner_col_map[1] == std::numeric_limits<uint32_t>::max());
		} else {
			D_ASSERT(this->inner_col_map.size() == 1);	// inner = (tid)
			discard_tgt = (this->inner_col_map[0] == std::numeric_limits<uint32_t>::max());
			discard_edge = true;
		}
		if (load_eid_temporarily) { D_ASSERT(load_eid); }

		setFillFunc();
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
	bool load_eid_temporarily;
	bool enumerate;
	bool discard_tgt;
	bool discard_edge;
	std::function<void(AdjIdxJoinState &, uint64_t *, uint64_t *, uint64_t *, size_t, bool, Vector &)> fillFunc;

	// filter predicate pushdown // TODO we currently consider equality predicate with tgt
	bool do_filter_pushdown = false;
	uint32_t outer_pos;
	uint32_t inner_pos;
	uint64_t *inner_vec;

private:
	void setFillFuncLoadEID() {
		// load eid & tgt
		if (discard_tgt) {
			if (discard_edge) {
				// discard_tgt && discard_edge
				if (do_filter_pushdown) {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								state.output_idx++;
							}
						}
					};
				} else {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								state.output_idx++;
							}
						}
					};
				}
			} else {
				// discard_tgt
				if (do_filter_pushdown) {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						D_ASSERT(!load_eid || (eid_adj_column != nullptr));
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								eid_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
								state.output_idx++;
							}
						}
					};
				} else {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						D_ASSERT(!load_eid || (eid_adj_column != nullptr));
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								eid_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
								state.output_idx++;
							}
						}
					};
				}
			}
		} else {
			// do not discard tgt
			if (discard_edge) {
				// discard_edge
				if (do_filter_pushdown) {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
								state.output_idx++;
							}
						}
					};
				} else {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
								state.output_idx++;
							}
						}
					};
				}
			} else {
				if (do_filter_pushdown) {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						D_ASSERT(!load_eid || (eid_adj_column != nullptr));
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								eid_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
								eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
								state.output_idx++;
							}
						}
					};
				} else {
					fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
						D_ASSERT(!load_eid || (eid_adj_column != nullptr));
						if (fill_null) {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								eid_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
								state.output_idx++;
							}
						} else {
							auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
							for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
								state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
								tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
								eid_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2 + 1];
								state.output_idx++;
							}
						}
					};
				}
			}
		}
	}

	void setFillFuncLoadTGTOnly() {
		// load tgt only
		if (discard_tgt) {
			if (do_filter_pushdown) {
				fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
					if (fill_null) {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							state.output_idx++;
						}
					} else {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							fprintf(stdout, "Compare %ld != %ld\n", outer_vec.GetValue(state.lhs_idx).GetValue<uint64_t>(), adj_start[state.rhs_idx * 2]);
							if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							state.output_idx++;
						}
					}
				};
			} else {
				fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
					if (fill_null) {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							state.output_idx++;
						}
					} else {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							state.output_idx++;
						}
					}
				};
			}
		} else {
			if (do_filter_pushdown) {
				fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
					if (fill_null) {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
							state.output_idx++;
						}
					} else {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							//fprintf(stdout, "Compare %ld != %ld\n", outer_vec.GetValue(state.lhs_idx).GetValue<uint64_t>(), adj_start[state.rhs_idx * 2]);
							if (outer_vec.GetValue(state.lhs_idx) != adj_start[state.rhs_idx * 2]) continue;
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
							state.output_idx++;
						}
					}
				};
			} else {
				fillFunc = [this](AdjIdxJoinState &state, uint64_t *adj_start, uint64_t *tgt_adj_column, 
										uint64_t *eid_adj_column, size_t num_rhs_to_try_fetch, bool fill_null, Vector &outer_vec) {
					if (fill_null) {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							tgt_adj_column[state.output_idx] = std::numeric_limits<uint64_t>::max();
							state.output_idx++;
						}
					} else {
						auto tmp_rhs_idx_end = state.rhs_idx + num_rhs_to_try_fetch;
						for( ; state.rhs_idx < tmp_rhs_idx_end; state.rhs_idx++) {
							state.rhs_sel.set_index(state.output_idx, state.lhs_idx);
							tgt_adj_column[state.output_idx] = adj_start[state.rhs_idx * 2];
							state.output_idx++;
						}
					}
				};
			}
		}
	}

	void setFillFunc() {
		if (load_eid) {
			setFillFuncLoadEID();
		} else {
			setFillFuncLoadTGTOnly();
		}
	}
};

}