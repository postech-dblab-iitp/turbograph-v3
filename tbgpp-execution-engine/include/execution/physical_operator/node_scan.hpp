#include "typedef.hpp"

#include "storage/graph_store.hpp"


#include "execution/physical_operator/cypher_physical_operator.hpp"
#include <vector>

using namespace duckdb;

class NodeScan: public CypherPhysicalOperator {

public:
	NodeScan(CypherSchema& sch): CypherPhysicalOperator(sch) {
		// TODO needs to be set by parameter
		// labels
		labels.insert("Person");
		// adj option
		loadAdjOpt = LoadAdjListOption::OUTGOING;
		// edge labelset
		auto e1 = LabelSet();
		e1.insert("KNOWS");
		edgeLabelSet.push_back(e1);
		// property keys
		// foopp.push_back("url");
		// foopp.push_back("name");
		// foopp.push_back("id");

		// scan schema
		scanSchema.push_back(duckdb::LogicalType::UBIGINT);
		scanSchema.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT));

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