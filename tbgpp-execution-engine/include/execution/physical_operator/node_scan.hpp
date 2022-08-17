#pragma once
#include "typedef.hpp"

#include "storage/graph_store.hpp"
#include "main/client_context.hpp"


#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include <vector>
#include <cassert>

namespace duckdb {

class NodeScan: public CypherPhysicalOperator {

public:
	NodeScan(CypherSchema& sch, ClientContext &ctxt, LabelSet l, std::vector<LabelSet> els, LoadAdjListOption ljo,  PropertyKeys pk):
		CypherPhysicalOperator(sch), context(ctxt), labels(l), edgeLabelSet(els), loadAdjOpt(ljo), propertyKeys(pk), filterKey("")  {
		
		assert( edgeLabelSet.size() <= 1 && "wrong nodescan interface implementation leads to limitation in ELS size");
	}
	NodeScan(CypherSchema& sch, ClientContext &ctxt, LabelSet l, std::vector<LabelSet> els, LoadAdjListOption ljo,  PropertyKeys pk, std::string fkey, duckdb::Value fval ):
		CypherPhysicalOperator(sch), context(ctxt), labels(l), edgeLabelSet(els), loadAdjOpt(ljo), propertyKeys(pk), filterKey(fkey), filterValue(fval)  {
		
		assert( edgeLabelSet.size() <= 1 && "wrong nodescan interface implementation leads to limitation in ELS size");
	}
	~NodeScan() { }

public:
	
	void GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState() const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	LabelSet labels;
	std::vector<LabelSet> edgeLabelSet; // TODO name is wrong.... needs to be set of set, 
	LoadAdjListOption loadAdjOpt;
	PropertyKeys propertyKeys;
	std::vector<duckdb::LogicalType> scanSchema;
	ClientContext &context;

	std::string filterKey;
	duckdb::Value filterValue;

};	

}