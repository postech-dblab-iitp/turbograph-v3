#include "execution/physical_operator/physical_node_scan.hpp"
//#include "common/types/chunk_collection.hpp"

#include "storage/graph_store.hpp"
#include "extent/extent_iterator.hpp"

#include "planner/expression.hpp"
#include "icecream.hpp"

#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"

#include <cassert>

namespace duckdb {
class NodeScanState : public LocalSourceState {
public:
	explicit NodeScanState() {
		iter_inited = false;
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
		CypherPhysicalOperator(sch), labels(labels), propertyKeys(pk), filter_pushdown_key("") { }
		
PhysicalNodeScan::PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk, string filter_pushdown_key, Value filter_pushdown_value):
	CypherPhysicalOperator(sch), labels(labels), propertyKeys(pk), filter_pushdown_key(filter_pushdown_key), filter_pushdown_value(filter_pushdown_value) { }

PhysicalNodeScan::~PhysicalNodeScan() {}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalNodeScan::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<NodeScanState>();
}

void PhysicalNodeScan::GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const {
	auto &state = (NodeScanState &)lstate;
// icecream::ic.enable();
	// If first time here, call doScan and get iterator from iTbgppGraphStore
	if (!state.iter_inited) {
		state.iter_inited = true;
		// select properties to access
		PropertyKeys access_property_keys = propertyKeys;
		vector<LogicalType> access_schema = schema.getTypes();
		if( filter_pushdown_key.compare("") != 0 ) {
			// check to add filterkey to access
			for( auto& key: propertyKeys ) {
				if( key.compare(filter_pushdown_key) == 0 ) { break; }
			}
			// now add pushdown key to access keys
			access_property_keys.push_back( filter_pushdown_key );
			access_schema.push_back( filter_pushdown_value.type() );
		}

		auto initializeAPIResult =
			context.client->graph_store->InitializeScan(state.ext_it, labels, state.null_els, state.null_adjopt, access_property_keys, access_schema );
		D_ASSERT(initializeAPIResult == StoreAPIResult::OK); 

	}
	D_ASSERT(state.ext_it != nullptr);

	// TODO need to split chunk in units of EXEC_ENGINE_VECTOR_SIZE
	if( filter_pushdown_key.compare("") == 0 ) {
		context.client->graph_store->doScan(state.ext_it, chunk, labels, state.null_els, state.null_adjopt, propertyKeys, schema.getTypes());
	} else {
		context.client->graph_store->doScan(state.ext_it, chunk, labels, state.null_els, state.null_adjopt, propertyKeys, schema.getTypes(), filter_pushdown_key, filter_pushdown_value);
	}
//IC();
	// GetData() should return empty chunk to indicate scan is finished.
icecream::ic.disable();
}

std::string PhysicalNodeScan::ParamsToString() const {
	return "nodescan-params";
}

std::string PhysicalNodeScan::ToString() const {
	return "NodeScan";
}
}