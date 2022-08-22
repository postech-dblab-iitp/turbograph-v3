

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
#include "execution/physical_operator/naive_expand.hpp"
#include "execution/physical_operator/simple_filter.hpp"
#include "execution/physical_operator/simple_projection.hpp"
#include "execution/physical_operator/edge_fetch.hpp"
//#include "execution/physical_operator/projection.hpp"
#include "execution/physical_operator/limit.hpp"

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
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
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

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1_1() {
	/*
		MATCH (n:Organisation) RETURN n.url;
		+----------------------------------------------------------------------------------------------+
		| n                                                                                            |
		+----------------------------------------------------------------------------------------------+
		| (:Company:Organisation {url: "http://dbpedia.org/resource/Kam_Air"}) |
		+----------------------------------------------------------------------------------------------+
		should output 7955 tuples
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
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
	// projections
	CypherSchema project_schema;
	project_schema.addColumn("url", duckdb::LogicalType::VARCHAR);
	std::vector<int> project_ordering({2,});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSet, scan_loadAdjOpt, scan_propertyKeys));
		//operators
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

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1_2() {
	/*
		MATCH (n:Organisation) WHERE n.id = 5 RETURN n.id;
		+----------------------------------------------------------------------------------------------+
		| n                                                                                            |
		+----------------------------------------------------------------------------------------------+
		| (:Company:Organisation {url: "http://dbpedia.org/resource/Kam_Air"}) |
		+----------------------------------------------------------------------------------------------+
		should output 7955 tuples
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
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
	// filter
	CypherSchema filter_schema = schema;
	int filter_colnum = 1; // id
	auto filter_value = duckdb::Value::UBIGINT(5);

	// projections
	CypherSchema project_schema;
	project_schema.addColumn("url", duckdb::LogicalType::VARCHAR);
	std::vector<int> project_ordering({2,});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSet, scan_loadAdjOpt, scan_propertyKeys));
		//operators
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
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

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test1_3() {
	/*
		MATCH (n:Organisation) LIMIT 5;
		should output 5 tuples
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
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
	ops.push_back(new Limit(schema, 5));
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
	schema1.addPropertyIntoNode("p", "id", duckdb::LogicalType::UBIGINT);
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


std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test5() {
	/*
		MATCH (n:Comment) RETURN n.name, n.id, n.url;
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "creationDate", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "locationIP", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "content", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSet;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Comment");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	scan_propertyKeys.push_back("creationDate");
	scan_propertyKeys.push_back("locationIP");
	scan_propertyKeys.push_back("content");
	scan_propertyKeys.push_back("id");
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


/* 
 * 
 * LDBC Plan Implementations
 * 
 */

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
	X rows
	*/
	// scan schema
	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "birthday", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "firstName", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "lastName", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "gender", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "browserUsed", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "locationIP", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "creationDate", duckdb::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	auto e1 = LabelSet();
	e1.insert("IS_LOCATED_IN");
	//scan_edegLabelSets.push_back(e1);
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
	int filter_colnum = 8; // id
		//sf1, 10, 100
	auto filter_value = duckdb::Value::UBIGINT(14);

	// Expand
	CypherSchema expandschema = filter_schema;
	expandschema.addNode("p", LoadAdjListOption::NONE);
	expandschema.addPropertyIntoNode("p", "id", duckdb::LogicalType::UBIGINT);
		// params
	LabelSet tgt_labels;
	tgt_labels.insert("Place");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;
	tgt_propertyKeys.push_back("id");
		// 0 . / . 1  /  1-8   / 9  / 10
		// nid / nadj / nttr-8 / pid / pattr-1
	
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("birthday", duckdb::LogicalType::BIGINT);
	project_schema.addColumn("locationIP", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("browserUsed", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("cityId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("gender", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);

	std::vector<int> project_ordering({2, 3, 1, 6, 5, 10, 4, 7});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys, "id", filter_value));
		//operators
	// FIXME add me again!
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new NaiveExpand(expandschema, "n", scan_labels, e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
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


std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBCShort3() {
	
	/*
	MATCH (n:Person {id: $personId })-[r:KNOWS]-(friend)
	RETURN
		friend.id AS personId,
		friend.firstName AS firstName,
		friend.lastName AS lastName,
		r.creationDate AS friendshipCreationDate
	ORDER BY
		friendshipCreationDate DESC,
		toInteger(personId) ASC
	*/
// TO
	/*
	MATCH (n:Person {id: $personId })-[r:KNOWS]->(friend:Person)
	RETURN
		friend.id AS personId,
		friend.firstName AS firstName,
		friend.lastName AS lastName,
		r.creationDate AS friendshipCreationDate
	*/

	CypherSchema schema;
	schema.addNode("n", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);

	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("id");
	scan_labels.insert("Person");
	scan_loadAdjOpt = LoadAdjListOption::NONE;

	// filter
	CypherSchema filter_schema = schema;
	int filter_colnum = 1; // id
		//sf1, 10, 100
	auto filter_value = duckdb::Value::UBIGINT(14);
		// TODO change value

	// expand
	auto expandschema = schema;
	expandschema.addEdge("r");
	expandschema.addNode("friend", LoadAdjListOption::NONE);
	expandschema.addPropertyIntoNode("friend", "id", duckdb::LogicalType::UBIGINT);
	expandschema.addPropertyIntoNode("friend", "firstName", duckdb::LogicalType::VARCHAR);
	expandschema.addPropertyIntoNode("friend", "lastName", duckdb::LogicalType::VARCHAR);
	auto e1 = LabelSet();
	e1.insert("KNOWS");
	LabelSet tgt_labels;
	tgt_labels.insert("Person");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;
	tgt_propertyKeys.push_back("id");
	tgt_propertyKeys.push_back("firstName");
	tgt_propertyKeys.push_back("lastName");

	// fetchedge
	auto efSchema = expandschema;
	efSchema.addPropertyIntoEdge("r", "creationDate", duckdb::LogicalType::BIGINT);
	PropertyKeys edge_pks;
	edge_pks.push_back("creationDate");

	// projection
	CypherSchema project_schema;
	project_schema.addColumn("personId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("friendshipCreationDate", duckdb::LogicalType::BIGINT);

	std::vector<int> project_ordering({ 5, 6, 7, 3 });
	
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys, "id", filter_value));
		//operators
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new NaiveExpand(expandschema, "n", scan_labels, e1, ExpandDirection::OUTGOING, "r", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new EdgeFetch(efSchema, "r", e1, edge_pks));
	ops.push_back(new SimpleProjection(project_schema, project_ordering) );
	ops.push_back(new ProduceResults(project_schema) );

	// wrap pipeline into vector
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);

	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;

}



std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBCShort4() {

	/*
	MATCH (n:Message {id: 4398046511333 })
		RETURN
			m.creationDate AS messageCreationDate,
			coalesce(m.content, m.imageFile) as messageContent
	X rows
	*/

	// TODO replacing expression as just simply loading two columns 
	/*
	MATCH (n:Message {id: 4398046511333 })
		RETURN
			m.creationDate AS messageCreationDate,
			m.content as messageContent,
			m.imageFile as messageFile
	X rows
	*/

	// run two pipelines sequentially.
	auto p1 = ldbc_s4_comment();
	auto p2 = ldbc_s4_post();
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(p1);
	result.push_back(p2);
	return result;

}

CypherPipelineExecutor* QueryPlanSuite::ldbc_s4_comment() {
	// Pipeline 1
	// scan schema
	CypherSchema schema;
	schema.addNode("m", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("m", "id", duckdb::LogicalType::UBIGINT);
	schema.addPropertyIntoNode("m", "content", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("m", "creationDate", duckdb::LogicalType::BIGINT);
	
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Comment");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	auto e1 = LabelSet();
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("content");
	scan_propertyKeys.push_back("creationDate");

	CypherSchema filter_schema = schema;
	int filter_colnum = 1; // id
		//sf1, 10, 100
	//auto filter_value = duckdb::Value::UBIGINT(57459); // 1
	//auto filter_value = duckdb::Value::BIGINT(58929); // 10 
	auto filter_value = duckdb::Value::BIGINT(19560); // 100
	// TODO change
	
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("content", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);

	std::vector<int> project_ordering({ 2, 3 });

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys, "id", filter_value));
		//operators
	// FIXME add me again!
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new SimpleProjection(project_schema, project_ordering));
		// sink
	ops.push_back(new ProduceResults(project_schema));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	return pipeexec1;
}

CypherPipelineExecutor* QueryPlanSuite::ldbc_s4_post() {
	// Pipeline 1
	// scan schema
	CypherSchema schema;
	schema.addNode("m", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("m", "id", duckdb::LogicalType::UBIGINT);
	schema.addPropertyIntoNode("m", "content", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("m", "imageFile", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("m", "creationDate", duckdb::LogicalType::BIGINT);

	
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Post");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	auto e1 = LabelSet();
	scan_propertyKeys.push_back("id");
	scan_propertyKeys.push_back("content");
	scan_propertyKeys.push_back("imageFile");
	scan_propertyKeys.push_back("creationDate");

	CypherSchema filter_schema = schema;
	int filter_colnum = 1; // id
		//sf1, 10, 100
	auto filter_value = duckdb::Value::UBIGINT(0); // 1 10 100 s ;; NO ID
	
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("content", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("imageFile", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);

	std::vector<int> project_ordering({ 2, 3, 4});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys, "id", filter_value));
		//operators
	// FIXME add me again!
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new SimpleProjection(project_schema, project_ordering));
		// sink
	ops.push_back(new ProduceResults(project_schema));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	return pipeexec1;
}


std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBCShort5() {

	/*
	MATCH (m:Message {id:  $messageId })-[:HAS_CREATOR]->(p:Person)
	RETURN
		p.id AS personId,
		p.firstName AS firstName,
		p.lastName AS lastName
	*/

	// run two pipelines sequentially.
	auto p1 = ldbc_s5_comment();
	auto p2 = ldbc_s5_post();

	std::vector<CypherPipelineExecutor*> result;
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* QueryPlanSuite::ldbc_s5_comment() {

	CypherSchema schema;
	schema.addNode("m", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("m", "id", duckdb::LogicalType::UBIGINT);
	
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Comment");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	auto e1 = LabelSet();
	e1.insert("C_HAS_CREATOR");
	//scan_edegLabelSets.push_back(e1);
	scan_propertyKeys.push_back("id");
	
	// Filter
	CypherSchema filter_schema = schema;
	int filter_colnum = 1; // id
		//sf1
	//auto filter_value = duckdb::Value::UBIGINT(57459);
		// sf10
	//auto filter_value = duckdb::Value::BIGINT(58929);
		// sf100
	auto filter_value = duckdb::Value::BIGINT(19560); // 100
	// 
	
	

	// Expand
	CypherSchema expandschema = filter_schema;
	expandschema.addNode("p", LoadAdjListOption::NONE);
	expandschema.addPropertyIntoNode("p", "id", duckdb::LogicalType::UBIGINT);
	expandschema.addPropertyIntoNode("p", "firstName", duckdb::LogicalType::VARCHAR);
	expandschema.addPropertyIntoNode("p", "lastName", duckdb::LogicalType::VARCHAR);
		// params
	LabelSet tgt_labels;
	tgt_labels.insert("Person");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;
	tgt_propertyKeys.push_back("id");
	tgt_propertyKeys.push_back("firstName");
	tgt_propertyKeys.push_back("lastName");
		// 0 . / . 1  /  1-8   / 9  / 10
		// nid / nadj / nttr-8 / pid / pattr-1
	
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("personId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);

	std::vector<int> project_ordering({ 3,4,5});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys, "id", filter_value));
		//operators
	// FIXME add me again!
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new NaiveExpand(expandschema, "m", scan_labels, e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new SimpleProjection(project_schema, project_ordering));
		// sink
	ops.push_back(new ProduceResults(project_schema));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	
	// wrap pipeline into vector
	return pipeexec1;

}


CypherPipelineExecutor* QueryPlanSuite::ldbc_s5_post() {

	CypherSchema schema;
	schema.addNode("m", LoadAdjListOption::NONE);
	schema.addPropertyIntoNode("m", "id", duckdb::LogicalType::UBIGINT);
	
	// scan params
	LabelSet scan_labels;
	std::vector<LabelSet> scan_edegLabelSets;
	LoadAdjListOption scan_loadAdjOpt;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Post");
	scan_loadAdjOpt = LoadAdjListOption::NONE;
	auto e1 = LabelSet();
	e1.insert("P_HAS_CREATOR");
	//scan_edegLabelSets.push_back(e1);
	scan_propertyKeys.push_back("id");
	
	// Filter
	CypherSchema filter_schema = schema;
	int filter_colnum = 1; // id
		//sf1
	//auto filter_value = duckdb::Value::BIGINT(57459);
		// sf10
	//auto filter_value = duckdb::Value::BIGINT(58929);
	auto filter_value = duckdb::Value::UBIGINT(0);	

	// Expand
	CypherSchema expandschema = filter_schema;
	expandschema.addNode("p", LoadAdjListOption::NONE);
	expandschema.addPropertyIntoNode("p", "id", duckdb::LogicalType::UBIGINT);
	expandschema.addPropertyIntoNode("p", "firstName", duckdb::LogicalType::VARCHAR);
	expandschema.addPropertyIntoNode("p", "lastName", duckdb::LogicalType::VARCHAR);
		// params
	LabelSet tgt_labels;
	tgt_labels.insert("Person");
	std::vector<LabelSet> tgt_edgeLabelSets;
	LoadAdjListOption tgt_loadAdjOpt = LoadAdjListOption::NONE;
	PropertyKeys tgt_propertyKeys;
	tgt_propertyKeys.push_back("id");
	tgt_propertyKeys.push_back("firstName");
	tgt_propertyKeys.push_back("lastName");
		// 0 . / . 1  /  1-8   / 9  / 10
		// nid / nadj / nttr-8 / pid / pattr-1
	
	// Project
	CypherSchema project_schema;
	project_schema.addColumn("personId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);

	std::vector<int> project_ordering({ 3,4,5});

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new NodeScan(schema, context, scan_labels, scan_edegLabelSets, scan_loadAdjOpt, scan_propertyKeys, "id", filter_value));
		//operators
	// FIXME add me again!
	ops.push_back(new SimpleFilter(filter_schema, filter_colnum, filter_value));
	ops.push_back(new NaiveExpand(expandschema, "m", scan_labels, e1, ExpandDirection::OUTGOING, "", tgt_labels, tgt_edgeLabelSets, tgt_loadAdjOpt, tgt_propertyKeys));
	ops.push_back(new SimpleProjection(project_schema, project_ordering));
		// sink
	ops.push_back(new ProduceResults(project_schema));
	auto pipe1 = new CypherPipeline(ops);
	auto pipeexec1 = new CypherPipelineExecutor(pipe1, graphstore);
	return pipeexec1;

}

}