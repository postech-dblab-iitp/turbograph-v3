//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/radix_partitioned_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/partitionable_hashtable.hpp"
#include "parser/group_by_node.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {
class BufferManager;
class Executor;
class PhysicalHashAggregate;
class Pipeline;
class Task;

class RadixPartitionedHashTable {
public:
	RadixPartitionedHashTable(GroupingSet &grouping_set, const PhysicalHashAggregate &op);

	GroupingSet &grouping_set;
	vector<idx_t> null_groups;
	const PhysicalHashAggregate &op;

	vector<LogicalType> group_types;
	//! how many groups can we have in the operator before we switch to radix partitioning
	idx_t radix_limit;

	//! The GROUPING values that belong to this hash table
	vector<Value> grouping_values;

public:
	//! Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;

	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate, DataChunk &input,
	          DataChunk &aggregate_input_chunk) const;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const;
	bool Finalize(ClientContext &context, GlobalSinkState &gstate_p) const;

	void ScheduleTasks(Executor &executor, const shared_ptr<Event> &event, GlobalSinkState &state,
	                   vector<unique_ptr<Task>> &tasks) const;

	//! Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState() const;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSinkState &sink_state,
	             GlobalSourceState &gstate_p, const vector<uint64_t> &output_projection_mapping) const;

	static void SetMultiScan(GlobalSinkState &state);
	bool ForceSingleHT(GlobalSinkState &state) const;
};

} // namespace duckdb
