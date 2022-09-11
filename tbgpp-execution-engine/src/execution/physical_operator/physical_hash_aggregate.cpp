#include "execution/physical_operator/physical_hash_aggregate.hpp"

#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/aggregate_hashtable.hpp"
#include "execution/partitionable_hashtable.hpp"
#include "main/client_context.hpp"
#include "parallel/pipeline.hpp"
#include "parallel/task_scheduler.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "parallel/event.hpp"
#include "common/atomic.hpp"

#include "execution/execution_context.hpp"

#include "planner/expression/bound_reference_expression.hpp"

#include "execution/physical_operator/physical_operator.hpp"

#include "icecream.hpp"

namespace duckdb {

PhysicalHashAggregate::PhysicalHashAggregate(CypherSchema& sch, vector<unique_ptr<Expression>> expressions)
	: PhysicalHashAggregate(sch, move(expressions), {}) {
}
PhysicalHashAggregate::PhysicalHashAggregate(CypherSchema& sch, vector<unique_ptr<Expression>> expressions,
						vector<unique_ptr<Expression>> groups_p)
	: PhysicalHashAggregate(sch, move(expressions), move(groups_p), {}, {}) {
}
PhysicalHashAggregate::PhysicalHashAggregate(CypherSchema& sch, vector<unique_ptr<Expression>> expressions,
						vector<unique_ptr<Expression>> groups_p,
						vector<GroupingSet> grouping_sets_p,
						vector<vector<idx_t>> grouping_functions_p)
	: CypherPhysicalOperator(sch), groups(move(groups_p)),
      grouping_sets(move(grouping_sets_p)), grouping_functions(move(grouping_functions_p)), all_combinable(true),	
      any_distinct(false) {

	// TODO no support for custom grouping sets and grouping functions
	D_ASSERT(grouping_sets.size() == 0 );
	D_ASSERT(grouping_functions.size() == 0 );
IC();
	// get a list of all aggregates to be computed
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_types.size(); i++) {
			set.insert(i);
		}
		grouping_sets.push_back(move(set));
	}
IC();
	vector<LogicalType> payload_types_filters;
	for (auto &expr : expressions) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		if (aggr.distinct) {
			any_distinct = true;
		}

		aggregate_return_types.push_back(aggr.return_type);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			payload_types_filters.push_back(aggr.filter->return_type);
		}
		if (!aggr.function.combine) {
			all_combinable = false;
		}
		aggregates.push_back(move(expr));
	}

	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
IC();
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}

	for (auto &grouping_set : grouping_sets) {
		radix_tables.emplace_back(grouping_set, *this);
	}
IC();
}


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// class HashAggregateGlobalState : public GlobalSinkState {
// public:
// 	HashAggregateGlobalState(const PhysicalHashAggregate &op, ClientContext &context) {
// 		radix_states.reserve(op.radix_tables.size());
// 		for (auto &rt : op.radix_tables) {
// 			radix_states.push_back(rt.GetGlobalSinkState(context));
// 		}
// 	}

// 	vector<unique_ptr<GlobalSinkState>> radix_states;
// };

class HashAggregateLocalSinkState : public LocalSinkState {
public:
	HashAggregateLocalSinkState(const PhysicalHashAggregate &op, ExecutionContext &context) {
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}

		local_radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			local_radix_states.push_back(rt.GetLocalSinkState(context));
		}

		// initialize global states too
		global_radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			global_radix_states.push_back(rt.GetGlobalSinkState(*(context.client)));
		}
	}

	DataChunk aggregate_input_chunk;

	vector<unique_ptr<LocalSinkState>> local_radix_states;
	vector<unique_ptr<GlobalSinkState>> global_radix_states;
};

// void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
// 	auto &gstate = (HashAggregateGlobalState &)state;
// 	for (auto &radix_state : gstate.radix_states) {
// 		RadixPartitionedHashTable::SetMultiScan(*radix_state);
// 	}
// }

// unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
// 	return make_unique<HashAggregateGlobalState>(*this, context);
// }

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalSinkState>(*this, context);
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const {
	auto &llstate = (HashAggregateLocalSinkState &)lstate;
	// auto &gstate = (HashAggregateGlobalState &)state;

	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(input.size());
	aggregate_input_chunk.Verify();

	// for (idx_t i = 0; i < radix_tables.size(); i++) {
	// 	radix_tables[i].Sink(context, *gstate.radix_states[i], *llstate.radix_states[i], input, aggregate_input_chunk);
	// }
// IC(radix_tables.size());
// IC(llstate.global_radix_states.size());
// IC(llstate.local_radix_states.size());
	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Sink(context, *llstate.global_radix_states[i], *llstate.local_radix_states[i], input, aggregate_input_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}


class HashAggregateFinalizeEvent : public Event {
public:
	HashAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateLocalSinkState &lstate_p, ClientContext& context)
	    : Event(*(context.executor)), context(context), op(op_p), lstate(lstate_p) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateLocalSinkState &lstate;
	ClientContext& context;
	// Pipeline *pipeline;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			op.radix_tables[i].ScheduleTasks(*(context.executor), shared_from_this(), *lstate.global_radix_states[i], tasks);
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

void PhysicalHashAggregate::Combine(ExecutionContext &context, LocalSinkState &lstate) const {
	
	//
	// combine content
	//
	// auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalSinkState &)lstate;

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Combine(context, *llstate.global_radix_states[i], *llstate.local_radix_states[i]);
	}

	//
	// finalize content
	//
	// TODO new pipelinefinishevent (which is already being executed at this point)

	bool any_partitioned = false;
	for (idx_t i = 0; i < llstate.global_radix_states.size(); i++) {
		bool is_partitioned = radix_tables[i].Finalize(*(context.client), *llstate.global_radix_states[i]);
		if (is_partitioned) {
			any_partitioned = true;
		}
	}
	if (any_partitioned) {
		D_ASSERT(false && "JHKO this logic encuntererd. plz add to execute child event when this encountered");
		//auto new_event = make_shared<HashAggregateFinalizeEvent>(*this, llstate, *(context.client));
		// make child event for pipelinefinish event
		//event.InsertEvent(move(new_event));
	}
	// finishevent
	// TODO start event
	// here
	// new_event.start?

}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit HashAggregateLocalSourceState(const PhysicalHashAggregate &op) : scan_index(0) {
		IC( ListType::GetChildType(op.types[1]).ToString() );
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSourceState());
		}
		IC();
	}

	idx_t scan_index;
	vector<unique_ptr<GlobalSourceState>> radix_states;
};


unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context) const {
IC();
	return make_unique<HashAggregateLocalSourceState>(*this);
}
void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const {
	
	auto &sstate = (HashAggregateLocalSinkState &)sink_state;
	auto &state = (HashAggregateLocalSourceState &)lstate;

	while (state.scan_index < state.radix_states.size()) {
		radix_tables[state.scan_index].GetData(context, chunk, *sstate.global_radix_states[state.scan_index],
		                                       *state.radix_states[state.scan_index]);
		if (chunk.size() != 0) {
			return;
		}

		state.scan_index++;
	}

}

string PhysicalHashAggregate::ParamsToString() const {
	return "hashagg-params";
}
std::string PhysicalHashAggregate::ToString() const {

	return "HashAggregate";
}

}