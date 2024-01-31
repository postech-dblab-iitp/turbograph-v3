#include "execution/physical_operator/physical_hash_join.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "main/client_context.hpp"
//#include "main/query_profiler.hpp"
#include "parallel/thread_context.hpp"
#include "storage/buffer_manager.hpp"
#include "storage/storage_manager.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"	

namespace duckdb {

/**
 * Build executor should have right expressions on join condition
 * In schemaless situation, right expressions can have different BoundReferenceExpression (Note that they have different schema)
 * Therefore, according to their schema, build_executor should have initialized differently, which is crazy.
*/
PhysicalHashJoin::PhysicalHashJoin(Schema sch, 
									vector<JoinCondition> cond,
									JoinType join_type,
                                   vector<uint32_t> &output_left_projection_map,	// s62 style projection map
                                   vector<uint32_t> &output_right_projection_map,	// s62 style projection map
								   vector<LogicalType> &right_build_types,
								   vector<idx_t> &right_build_map	// duckdb style build map - what build types
								   )
    : PhysicalComparisonJoin(sch, PhysicalOperatorType::HASH_JOIN, move(cond), join_type),
		build_types(right_build_types), right_projection_map(right_build_map),
		output_left_projection_map(output_left_projection_map), output_right_projection_map(output_right_projection_map)
	 {

	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}
	
	D_ASSERT(build_types.size() == right_projection_map.size());
	if(join_type == JoinType::ANTI || join_type == JoinType::SEMI) {
		D_ASSERT(build_types.size() == 0);
	}

	D_ASSERT(delim_types.size() == 0);

	// TODO build inverse projection map
	
}


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinLocalState : public LocalSinkState {
public:
	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;

	//! The HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! Whether or not the hash table has been finalized
	bool finalized = false;
};


// unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	
// }

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_unique<HashJoinLocalState>();
	if (!right_projection_map.empty()) {
		state->build_chunk.Initialize(build_types);
	}
	for (auto &cond : conditions) {
		std::cout << "left :" << cond.left->ToString() << std::endl;
		std::cout << "right :" << cond.right->ToString() << std::endl;
		state->build_executor.AddExpression(*cond.right);
	}
	state->join_keys.Initialize(condition_types);

	// globals
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(*(context.client->db.get())), conditions, build_types, join_type);
	// if (!delim_types.empty() && join_type == JoinType::MARK) {
	// 	// correlated MARK join
	//	// DELTED
	// }
	// for perfect hash join
	// state->perfect_join_executor =
	//     make_unique<PerfectHashJoinExecutor>(*this, *state->hash_table, perfect_join_statistics);

	return move(state);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &state) const {
	auto &sink = (HashJoinLocalState &)state;
	auto &lstate = (HashJoinLocalState &)state;
	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.build_executor.Execute(input, lstate.join_keys);
	// TODO: add statement to check for possible per
	// build the HT
 	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
		sink.hash_table->Build(lstate.join_keys, input);
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext& context, LocalSinkState& lstate) const {
	auto &state = (HashJoinLocalState &)lstate;
	// auto &client_profiler = QueryProfiler::Get(context.client);
	// context.thread.profiler.Flush(this, &state.build_executor, "build_executor", 1);
	// client_profiler.Flush(context.thread.profiler);

	// finalize contexts
	auto &sink = (HashJoinLocalState &)lstate;	
	// check for possible perfect hash table
	// auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
	// if (use_perfect_hash) {
	// 	D_ASSERT(sink.hash_table->equality_types.size() == 1);
	// 	auto key_type = sink.hash_table->equality_types[0];
	// 	use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
	// }
	// In case of a large build side or duplicates, use regular hash join
	//if (!use_perfect_hash) {
		//sink.perfect_join_executor.reset();
		sink.hash_table->Finalize();
	//}
	sink.finalized = true;
	// if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
	// 	return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	// }
	// return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
// SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
//                                             GlobalSinkState &gstate) const {
// 	auto &sink = (HashJoinGlobalState &)gstate;	
// 	// check for possible perfect hash table
// 	// auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
// 	// if (use_perfect_hash) {
// 	// 	D_ASSERT(sink.hash_table->equality_types.size() == 1);
// 	// 	auto key_type = sink.hash_table->equality_types[0];
// 	// 	use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
// 	// }
// 	// In case of a large build side or duplicates, use regular hash join
// 	//if (!use_perfect_hash) {
// 		//sink.perfect_join_executor.reset();
// 		sink.hash_table->Finalize();
// 	//}
// 	sink.finalized = true;
// 	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
// 		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
// 	}
// 	return SinkFinalizeType::READY;
// }

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalHashJoinState : public OperatorState {
public:
	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		// context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto state = make_unique<PhysicalHashJoinState>();
	// auto &sink = (HashJoinLocalState &)*sink_state;
	// if (sink.perfect_join_executor) {
	// 	state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	// } else {
		state->join_keys.Initialize(condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
	//}
	return move(state);
}

OperatorResultType PhysicalHashJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             OperatorState &state_p, LocalSinkState &sink_state) const {
	auto &state = (PhysicalHashJoinState &)state_p;
	auto &sink = (HashJoinLocalState &)sink_state;
	D_ASSERT(sink.finalized);

	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return OperatorResultType::FINISHED;
	}
	// if (sink.perfect_join_executor) {
	// 	return sink.perfect_join_executor->ProbePerfectHashTable(context, input, chunk, *state.perfect_hash_join_state);
	// }

	if (state.scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state.scan_structure->Next(state.join_keys, input, chunk);
		if (chunk.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// probe the HT
	if (sink.hash_table->Count() == 0) {
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, input, chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// resolve the join keys for the left chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);

	// perform the actual probe
	state.scan_structure = sink.hash_table->Probe(state.join_keys);
	state.scan_structure->Next(state.join_keys, input, chunk);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
// class HashJoinScanState : public GlobalSourceState {
// public:
// 	explicit HashJoinScanState(const PhysicalHashJoin &op) : op(op) {
// 	}

// 	const PhysicalHashJoin &op;
// 	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
// 	JoinHTScanState ht_scan_state;

// 	idx_t MaxThreads() override {
// 		auto &sink = (HashJoinGlobalState &)*op.sink_state;
// 		return sink.hash_table->Count() / (STANDARD_VECTOR_SIZE * 10);
// 	}
// };

// unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
// 	return make_unique<HashJoinScanState>(*this);
// }

// void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
//                                LocalSourceState &lstate) const {
// 	D_ASSERT(IsRightOuterJoin(join_type));
// 	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
// 	auto &sink = (HashJoinGlobalState &)*sink_state;
// 	auto &state = (HashJoinScanState &)gstate;
// 	sink.hash_table->ScanFullOuter(chunk, state.ht_scan_state);
// }

std::string PhysicalHashJoin::ParamsToString() const {
	std::string result = "";
	result += "output_left_projection_map.size()=" + std::to_string(output_left_projection_map.size()) + ", ";
	result += "output_right_projection_map.size()=" + std::to_string(output_right_projection_map.size()) + ", ";
	result += "right_projection_map.size()=" + std::to_string(right_projection_map.size()) + ", ";
	result += "condition_types.size()=" + std::to_string(condition_types.size()) + ", ";
	result += "build_types.size()=" + std::to_string(build_types.size()) + ", ";
	return result;
}

std::string PhysicalHashJoin::ToString() const {
	return "HashJoin";
}

} // namespace duckdb
