//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"
// #include "storage/data_table.hpp"
#include "parser/group_by_node.hpp"
#include "execution/radix_partitioned_hashtable.hpp"

#include "planner/expression/bound_aggregate_expression.hpp"


namespace duckdb {

class ClientContext;
class BufferManager;

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public CypherPhysicalOperator {
public:
 PhysicalHashAggregate(Schema &sch, vector<uint64_t> &output_projection_mapping,
                       vector<unique_ptr<Expression>> expressions,
                       vector<uint32_t> &grouping_key_idxs_p);
 PhysicalHashAggregate(Schema &sch, vector<uint64_t> &output_projection_mapping,
                       vector<unique_ptr<Expression>> expressions,
                       vector<unique_ptr<Expression>> groups,
                       vector<uint32_t> &grouping_key_idxs_p);
 PhysicalHashAggregate(Schema &sch, vector<uint64_t> &output_projection_mapping,
                       vector<unique_ptr<Expression>> expressions,
                       vector<unique_ptr<Expression>> groups,
                       vector<GroupingSet> grouping_sets,
                       vector<vector<idx_t>> grouping_functions,
                       vector<uint32_t> &grouping_key_idxs_p);
 ~PhysicalHashAggregate(){};

public:
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const override;
	void Combine(ExecutionContext &context, LocalSinkState &lstate) const override;
	bool IsSink() const override { return true; }
	DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;

	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const override;
	bool IsSource() const override { return true; }
	bool IsSourceDataRemaining(LocalSourceState &lstate, LocalSinkState &sink_state) const override;

	string ParamsToString() const override;
	std::string ToString() const override;
	//! Toggle multi-scan capability on a hash table, which prevents the scan of the aggregate from being destructive
	//! If this is not toggled the GetData method will destroy the hash table as it is scanning it
	// static void SetMultiScan(GlobalSinkState &state);

	virtual size_t GetLoopCount() const override { return num_loops; }

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The grouping sets
	vector<GroupingSet> grouping_sets;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! The set of GROUPING functions
	vector<vector<idx_t>> grouping_functions;
	//! Whether or not all aggregates are combinable
	bool all_combinable;

	vector<uint32_t> grouping_key_idxs;

	//! Whether or not any aggregation is DISTINCT
	bool any_distinct;

	//! The group types
	vector<LogicalType> group_types;
	//! The payload types
	vector<LogicalType> payload_types;
	//! The aggregate return types
	vector<LogicalType> aggregate_return_types;

	//! The radix partitioned hash tables (one per grouping set)
	vector<RadixPartitionedHashTable> radix_tables;

	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;

	unordered_map<Expression *, size_t> filter_indexes;

	vector<uint64_t> output_projection_mapping;

	mutable uint64_t num_loops = 0;
};

} // namespace duckdb
