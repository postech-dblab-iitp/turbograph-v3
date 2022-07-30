#include "demo_plans.hpp"

#include "main/database.hpp"
#include "main/client_context.hpp"
//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/node_scan.hpp"
#include "execution/physical_operator/physical_dummy_operator.hpp"
#include "execution/physical_operator/produce_results.hpp"
#include "execution/physical_operator/expand.hpp"
#include "execution/physical_operator/simple_filter.hpp"
#include "execution/physical_operator/simple_projection.hpp"

#include "storage/graph_store.hpp"

#include "typedef.hpp"
#include <vector>

#include <iostream>

namespace duckdb {
QueryPlanSuite::QueryPlanSuite(GraphStore* graphstore, ClientContext &context): graphstore(graphstore), context(context) {  }


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
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSet, scan_loadAdjOpt, scan_propertyKeys));
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
	scan_loadAdjOpt = LoadAdjListOption::NONE;
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
	ops.push_back(new NodeScan(schema1, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys));
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
	ops.push_back(new NodeScan(schema1, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys));
	ops.push_back(new Expand(schema2, "p", e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new ProduceResults(schema2));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBCShort1() {

	/*
	MATCH (n:Person {id: 4398046511333 })-[:IS_LOCATED_IN]->(p:City)
		RETURN
			n.firstName AS firstName,
			n.lastName AS lastName,
			n.birthday AS birthday,
			n.locationIP AS locationIP,
			n.browserUsed AS browserUsed,
			p.id AS cityId,
			n.gender AS gender,
			n.creationDate AS creationDate

	+------------------------------------------------------------------------------------------------------------+
	| firstName | lastName    | birthday     | locationIP      | browserUsed | cityId | gender   | creationDate  |
	+------------------------------------------------------------------------------------------------------------+
	| "Rafael"  | "Fernández" | 334540800000 | "31.24.152.190" | "Chrome"    | 1345   | "female" | 1275959471971 |
	+------------------------------------------------------------------------------------------------------------+
	1 rows
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::OUTGOING);
	schema.addPropertyIntoNode("n", "birthday", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "firstName", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "lastName", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "gender", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "browserUsed", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "locationIP", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "creationDate", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::BIGINT);
	
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");
	scan_loadAdjOpt = LoadAdjListOption::OUTGOING;
	auto e1 = LabelSet();
	e1.insert("IS_LOCATED_IN");
	scan_edegLabelSets.push_back(e1);
	scan_propertyKeys.push_back("birthday");
	scan_propertyKeys.push_back("firstName");
	scan_propertyKeys.push_back("lastName");
	scan_propertyKeys.push_back("gender");
	scan_propertyKeys.push_back("browserUsed");
	scan_propertyKeys.push_back("locationIP");
	scan_propertyKeys.push_back("creationDate");
	scan_propertyKeys.push_back("id");
	
	// Filter
	CypherSchema filter_schema = schema;
	int filter_colnum = 9; // id
	auto filter_value = duckdb::Value::BIGINT(4398046511333);

	// Expand
	CypherSchema expandschema = filter_schema;
	expandschema.addNode("p", LoadAdjListOption::NONE);
	expandschema.addPropertyIntoNode("p", "id", duckdb::LogicalType::BIGINT);
		// params
	LabelSet tgt_labels;
	tgt_labels.insert("City");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;
	tgt_propertyKeys.push_back("id");
		// 0 . / . 1  /  2-9   / 10  / 11
		// nid / nadj / nttr-8 / pid / pattr-1
	
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("birthday", duckdb::LogicalType::BIGINT);
	project_schema.addColumn("locationIP", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("browserUsed", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("cityId", duckdb::LogicalType::BIGINT);
	project_schema.addColumn("gender", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);

	std::vector<int> project_ordering({3, 4, 2, 7, 6, 11, 5, 8});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys));
		//operators
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new Expand(expandschema, "n", e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new SimpleProjection(project_schema, project_ordering));
		// sink
	ops.push_back(new ProduceResults(project_schema));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
	

}



}