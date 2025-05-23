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

PhysicalHashAggregate::PhysicalHashAggregate(
    Schema &sch, vector<uint64_t> &output_projection_mapping,
    vector<unique_ptr<Expression>> expressions,
    vector<uint32_t> &grouping_key_idxs_p)
    : PhysicalHashAggregate(sch, output_projection_mapping, move(expressions),
                            {}, grouping_key_idxs_p)
{}
PhysicalHashAggregate::PhysicalHashAggregate(
    Schema &sch, vector<uint64_t> &output_projection_mapping,
    vector<unique_ptr<Expression>> expressions,
    vector<unique_ptr<Expression>> groups_p,
    vector<uint32_t> &grouping_key_idxs_p)
    : PhysicalHashAggregate(sch, output_projection_mapping, move(expressions),
                            move(groups_p), {}, {}, grouping_key_idxs_p)
{}
PhysicalHashAggregate::PhysicalHashAggregate(
    Schema &sch, vector<uint64_t> &output_projection_mapping,
    vector<unique_ptr<Expression>> expressions,
    vector<unique_ptr<Expression>> groups_p,
    vector<GroupingSet> grouping_sets_p,
    vector<vector<idx_t>> grouping_functions_p,
    vector<uint32_t> &grouping_key_idxs_p)
    : CypherPhysicalOperator(PhysicalOperatorType::HASH_AGGREGATE, sch),
      groups(move(groups_p)),
      grouping_sets(move(grouping_sets_p)),
      grouping_functions(move(grouping_functions_p)),
      all_combinable(true),
      any_distinct(false),
      output_projection_mapping(output_projection_mapping),
      grouping_key_idxs(move(grouping_key_idxs_p))
{
    // TODO no support for custom grouping sets and grouping functions
    D_ASSERT(grouping_sets.size() == 0);
    D_ASSERT(grouping_functions.size() == 0);

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
            }
            else {
                ++aggregate_input_idx;
            }
        }
    }

    for (auto &grouping_set : grouping_sets) {
        radix_tables.emplace_back(grouping_set, *this);
    }
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

    for (idx_t i = 0; i < radix_tables.size(); i++) {
        radix_tables[i].Sink(context, *llstate.global_radix_states[i],
                             *llstate.local_radix_states[i], input,
                             aggregate_input_chunk);
    }

    num_loops++;

	return SinkResultType::NEED_MORE_INPUT;
}

DataChunk &PhysicalHashAggregate::GetLastSinkedData(LocalSinkState &lstate) const {
	auto &llstate = (HashAggregateLocalSinkState &)lstate;
	return llstate.aggregate_input_chunk;
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
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSourceState());
		}
	}

	idx_t scan_index;
	vector<unique_ptr<GlobalSourceState>> radix_states;
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalSourceState>(*this);
}

void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const {
	auto &sstate = (HashAggregateLocalSinkState &)sink_state;
	auto &state = (HashAggregateLocalSourceState &)lstate;

	/* We assume after aggregation, schema is unified */
	chunk.SetSchemaIdx(0);
	while (state.scan_index < state.radix_states.size()) {
		auto prev_chunk_card = chunk.size();
		radix_tables[state.scan_index].GetData(context, chunk, *sstate.global_radix_states[state.scan_index],
		                                       *state.radix_states[state.scan_index], output_projection_mapping);
		auto new_chunk_card = chunk.size() - prev_chunk_card;
		if (new_chunk_card != 0) {
			return;
		}

		state.scan_index++;
	}

}

bool PhysicalHashAggregate::IsSourceDataRemaining(LocalSourceState &lstate, LocalSinkState &sink_state) const {
	auto &state = (HashAggregateLocalSourceState &)lstate;
	return state.scan_index < state.radix_states.size();
}

string PhysicalHashAggregate::ParamsToString() const {
	string params = "hashagg-params / groups: ";
	for (auto &group : groups) {
		params += group->ToString() + ", ";
	}
	params += " / aggregates: ";
	for (auto &aggr : aggregates) {
		params += aggr->ToString() + ", ";
	}
	return params;
}
std::string PhysicalHashAggregate::ToString() const {
	return "HashAggregate";
}

}