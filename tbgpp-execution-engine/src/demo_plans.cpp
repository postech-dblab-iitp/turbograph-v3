#include "demo_plans.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/executor.hpp"
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


QueryPlanSuite::QueryPlanSuite(GraphStore* graphstore): graphstore(graphstore) {  }


std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1() {
	/*
		MATCH (n:Organisation) RETURN n;
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
	schema.addPropertyIntoNode("n", "url", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::BIGINT);
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSet;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Organisation");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	scan_propertyKeys.push_back("url");
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, scan_labels, scan_edegLabelSet, scan_loadAdjOpt, scan_propertyKeys));
		//operators
		
		// sink
	ops.push_back(new ProduceResults());
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test2() {
	/*
		MATCH (p:Person)-[:LIKES]->(m) RETURN p.id, id(m);
		// person likes comment and post
		
		+--------------------+
		| p.id          | id(m) |
		+--------------------+
		should output 1383 tuples // TODO tuples
	*/
	
	// SCAN
		// schema
	CypherSchema schema1;
	schema1.addNode("n", LoadAdjListOption::OUTGOING);
	schema1.addPropertyIntoNode("n", "id", duckdb::LogicalType::BIGINT);
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
	schema2.addColumn("id(m)", duckdb::LogicalType::UBIGINT); // TODO fixme
		// params
	LabelSet tgt_labels;
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt;
	PropertyKeys tgt_propertyKeys;

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	ops.push_back(new NodeScan(schema1, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys));
	ops.push_back(new Expand(schema2, "p", e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new ProduceResults());
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

