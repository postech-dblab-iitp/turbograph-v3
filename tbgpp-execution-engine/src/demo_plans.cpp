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

#include "storage/graph_store.hpp"

#include "typedef.hpp"
#include <vector>

#include <iostream>


QueryPlanSuite::QueryPlanSuite(GraphStore* graphstore): graphstore(graphstore) {  }


std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1() {
	/*
		MATCH (n:Organisation) RETURN n;
		
		should output 7955 tuples
	*/
	// scan schema
	//auto schema = new CypherSchema();
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::OUTGOING);
	schema.addPropertyIntoNode("n", "url", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::BIGINT);
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSet;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Organisation");
	auto e1 = LabelSet();
	e1.insert("IS_LOCATED_IN");
	scan_edegLabelSet.push_back(e1);
	scan_loadAdjOpt = LoadAdjListOption::OUTGOING;
	scan_propertyKeys.push_back("url");
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	ops.push_back(new NodeScan(schema, scan_labels, scan_edegLabelSet, scan_loadAdjOpt, scan_propertyKeys));
	ops.push_back(new ProduceResults());
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

