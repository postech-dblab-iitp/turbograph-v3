#include "typedef.hpp"

#include "execution/physical_operator/physical_top_n_sort.hpp"
#include "execution/top_n_sort.hpp"

#include "execution/expression_executor.hpp"

#include <string>

#include "common/allocator.hpp"

#include "icecream.hpp"

namespace duckdb {


PhysicalTopNSort::PhysicalTopNSort(CypherSchema& sch, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset)
	: CypherPhysicalOperator(sch), orders(move(orders)), limit(limit), offset(offset)
{ }

PhysicalTopNSort::~PhysicalTopNSort() { };

class TopNSortSinkState : public LocalSinkState {
public:
	explicit TopNSortSinkState(ClientContext &context, const vector<LogicalType> &payload_types,
	             const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	TopNHeap heap;
};

unique_ptr<LocalSinkState> PhysicalTopNSort::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<TopNSortSinkState>(*(context.client), types, orders, limit, offset);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalTopNSort::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const {
	// append to the local sink state
icecream::ic.enable();
	auto &sink = (TopNSortSinkState &)lstate;
	sink.heap.Sink(input);
	sink.heap.Reduce();
icecream::ic.disable();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalTopNSort::Combine(ExecutionContext &context, LocalSinkState &lstate_p) const {
IC();
	// auto &gstate = (TopNGlobalState &)state;
	auto &lstate = (TopNSortSinkState &)lstate_p;
	// directly call finalize for heap
	lstate.heap.Finalize();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class TopNSortSourceState : public LocalSourceState {
public:
	TopNScanState state;
	bool initialized = false;
};

unique_ptr<LocalSourceState> PhysicalTopNSort::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<TopNSortSourceState>();
}

void PhysicalTopNSort::GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const{
icecream::ic.disable();
	if (limit == 0) {
	return;
	}
	auto &state = (TopNSortSourceState &)lstate;
	auto &gstate = (TopNSortSinkState &)sink_state;

	if (!state.initialized) {
		gstate.heap.InitializeScan(state.state, true);
		state.initialized = true;
	}
	gstate.heap.Scan(state.state, chunk);
icecream::ic.enable();
}

std::string PhysicalTopNSort::ParamsToString() const {
	return "topnsort-param";
}

std::string PhysicalTopNSort::ToString() const {
	return "TopNSort";
}


}