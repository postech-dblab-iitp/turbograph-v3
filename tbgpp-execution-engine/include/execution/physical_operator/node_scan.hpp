#include "typedef.hpp"

#include "storage/graph_store.hpp"


#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include <vector>

using namespace duckdb;

class NodeScan: public CypherPhysicalOperator {

public:
	NodeScan(CypherSchema& sch, LabelSet l, std::vector<LabelSet> els, LoadAdjListOption ljo,  PropertyKeys pk):
		CypherPhysicalOperator(sch), labels(l), edgeLabelSet(els), loadAdjOpt(ljo), propertyKeys(pk)  {

	}
	~NodeScan() { }

public:
	
	void GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState() const override;

	std::string ParamsToString() const override;

	LabelSet labels;
	std::vector<LabelSet> edgeLabelSet;
	LoadAdjListOption loadAdjOpt;
	PropertyKeys propertyKeys;
	std::vector<duckdb::LogicalType> scanSchema;

};	