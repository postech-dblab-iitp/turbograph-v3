#include "execution/physical_operator/physical_node_scan.hpp"
//#include "common/types/chunk_collection.hpp"

#include "storage/graph_store.hpp"
#include "extent/extent_iterator.hpp"

#include "planner/expression.hpp"

#include <cassert>

namespace duckdb {
class NodeScanState : public LocalSourceState {
public:
	explicit NodeScanState() {
		iter_inited = true;
		// TODO remove
		null_adjopt = LoadAdjListOption::NONE;
	}
public:
	bool iter_inited;
	ExtentIterator *ext_it;
	// TODO use for vectorized processing
	DataChunk extent_cache;
	// TODO remove after updating storage API	
	std::vector<LabelSet> null_els;
	LoadAdjListOption null_adjopt;
};

PhysicalNodeScan::PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk):
		CypherPhysicalOperator(sch), labels(labels), propertyKeys(pk), filter_pushdown_expression(nullptr) { }
		
PhysicalNodeScan::PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk, vector<unique_ptr<Expression>> storage_predicates):
	CypherPhysicalOperator(sch), labels(labels), propertyKeys(pk) {

	// TODO pushdwn predicates
	// convert into CNF

	filter_pushdown_expression = nullptr;

}
PhysicalNodeScan::~PhysicalNodeScan() {}

unique_ptr<LocalSourceState> PhysicalNodeScan::GetLocalSourceState() const {
	return make_unique<NodeScanState>();
}

void PhysicalNodeScan::GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;

	// If first time here, call doScan and get iterator from iTbgppGraphStore
	if (state.iter_inited) {
		state.iter_inited = false;
		auto initializeAPIResult =
			context.client->graph_store->InitializeScan(state.ext_it, labels, state.null_els, state.null_adjopt, propertyKeys, schema.getTypes());
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK); 
	}
	D_ASSERT(state.ext_it != nullptr);

	// TODO need to split chunk in units of EXEC_ENGINE_VECTOR_SIZE
	// TODO pass filter pushdown and bypasses
	auto scanAPIResult =
		context.client->graph_store->doScan(state.ext_it, chunk, labels, state.null_els, state.null_adjopt, propertyKeys, schema.getTypes());
	// auto scanAPIResult =
	// 	itbgpp_graph->doScan(state.ext_it, chunk, labels, state.null_els, state.null_els, propertyKeys, schema.getTypes(), filterKey, filterValue);
	// GetData() should return empty chunk to indicate scan is finished.
}

std::string PhysicalNodeScan::ParamsToString() const {
	return "nodescan-params";
}

std::string PhysicalNodeScan::ToString() const {
	return "NodeScan";
}
}