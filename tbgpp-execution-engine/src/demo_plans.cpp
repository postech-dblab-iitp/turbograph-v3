#include "demo_plans.hpp"

#include "main/database.hpp"
//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/node_scan.hpp"
#include "execution/physical_operator/physical_dummy_operator.hpp"
#include "execution/physical_operator/produce_results.hpp"
#include "execution/physical_operator/expand.hpp"

#include "storage/graph_store.hpp"

#include "typedef.hpp"
#include <vector>

#include <iostream>

namespace duckdb {
QueryPlanSuite::QueryPlanSuite(GraphStore* graphstore): graphstore(graphstore) {  }


std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1() {
	/*
		MATCH (n:Organisation) RETURN n.name, n.id, n.url;
		+----------------------------------------------------------------------------------------------+
		| n                                                                                            |
		+----------------------------------------------------------------------------------------------+
		| (:Company:Organisation {name: "Kam_Air", id: 0, url: "http://dbpedia.org/resource/Kam_Air"}) |
		+----------------------------------------------------------------------------------------------+
		should output 7955 tuples
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "url", duckdb::LogicalType::VARCHAR);
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSet;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Organisation");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("url");
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, scan_labels, scan_edegLabelSet, scan_loadAdjOpt, scan_propertyKeys));
		//operators
		
		// sink
	ops.push_back(new ProduceResults(schema));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test2() {
	/*
		MATCH (p:Person)-[:LIKES]->(m:Comment) RETURN p.id;
		
		+--------------------+
		| p.id          |
		+--------------------+
		should output 624 tuples // TODO tuples
	*/
	
	// SCAN
		// schema
	CypherSchema schema1;
	schema1.addNode("p", LoadAdjListOption::OUTGOING);
	schema1.addPropertyIntoNode("p", "id", duckdb::LogicalType::BIGINT);
		// parameters
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");
	auto e1 = LabelSet();
	e1.insert("LIKES");
	scan_edegLabelSets.push_back(e1);
	scan_loadAdjOpt = LoadAdjListOption::OUTGOING;
	scan_propertyKeys.push_back("id");
	
	// Expand
		// schema
	CypherSchema schema2 = schema1;
	schema2.addNode("m", LoadAdjListOption::NONE);
		// params
	LabelSet tgt_labels;
	tgt_labels.insert("Comment");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	ops.push_back(new NodeScan(schema1, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys));
	ops.push_back(new Expand(schema2, "p", e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new ProduceResults(schema2));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test3() {
	/*
		MATCH (p:Person)-[:LIKES]->(m:Post) RETURN p.firstName, p.lastName, m.browserUsedid;
		
		+------------------------------------------+
		| p.firstName | p.lastName | m.browserUsed |
		+------------------------------------------+
		| "Jose"      | "Alonso"   | "Firefox"     |
		+------------------------------------------+
		should output 759 tuples // TODO tuples
	*/
	
	// SCAN
		// schema
	CypherSchema schema1;
	schema1.addNode("p", LoadAdjListOption::OUTGOING);
	schema1.addPropertyIntoNode("p", "firstName", duckdb::LogicalType::VARCHAR);
	schema1.addPropertyIntoNode("p", "lastName", duckdb::LogicalType::VARCHAR);
		// parameters
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");
	auto e1 = LabelSet();
	e1.insert("LIKES");
	scan_edegLabelSets.push_back(e1);
	scan_loadAdjOpt = LoadAdjListOption::OUTGOING;
	scan_propertyKeys.push_back("firstName");
	scan_propertyKeys.push_back("lastName");
	
	// Expand
		// schema
	CypherSchema schema2 = schema1;
	schema2.addNode("m", LoadAdjListOption::NONE);
	schema2.addPropertyIntoNode("m", "browserUsed", duckdb::LogicalType::VARCHAR);
		// params
	LabelSet tgt_labels;
	tgt_labels.insert("Post");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;
	tgt_propertyKeys.push_back("browserUsed");

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	ops.push_back(new NodeScan(schema1, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys));
	ops.push_back(new Expand(schema2, "p", e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new ProduceResults(schema2));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;

}
}