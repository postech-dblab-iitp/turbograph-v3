// #include "plans/query_plan_suite.hpp"

// namespace duckdb {

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS1() {

// 	/*
// 	MATCH (n:Person {id: 4398046511333 })-[:IS_LOCATED_IN]->(p:City)
// 		RETURN
// 			n.firstName AS firstName,
// 			n.lastName AS lastName,
// 			n.birthday AS birthday,
// 			n.locationIP AS locationIP,
// 			n.browserUsed AS browserUsed,
// 			p.id AS cityId,
// 			n.gender AS gender,
// 			n.creationDate AS creationDate

// 	+------------------------------------------------------------------------------------------------------------+
// 	| firstName | lastName    | birthday     | locationIP      | browserUsed | cityId | gender   | creationDate  |
// 	+------------------------------------------------------------------------------------------------------------+
// 	| "Rafael"  | "Fernández" | 334540800000 | "31.24.152.190" | "Chrome"    | 1345   | "female" | 1275959471971 |
// 	+------------------------------------------------------------------------------------------------------------+
// 	X rows
// 	*/
// 	// scan schema
// 	CypherSchema schema;
// 	schema.addNode("n");
// 	schema.addPropertyIntoNode("n", "birthday", duckdb::LogicalType::BIGINT);
// 	schema.addPropertyIntoNode("n", "firstName", duckdb::LogicalType::VARCHAR);
// 	schema.addPropertyIntoNode("n", "lastName", duckdb::LogicalType::VARCHAR);
// 	schema.addPropertyIntoNode("n", "gender", duckdb::LogicalType::VARCHAR);
// 	schema.addPropertyIntoNode("n", "browserUsed", duckdb::LogicalType::VARCHAR);
// 	schema.addPropertyIntoNode("n", "locationIP", duckdb::LogicalType::VARCHAR);
// 	schema.addPropertyIntoNode("n", "creationDate", duckdb::LogicalType::BIGINT);
	
// 	// scan params
// 	LabelSet scan_labels;
// 	PropertyKeys scan_propertyKeys;
// 	scan_labels.insert("Person");
// 	scan_propertyKeys.push_back("birthday");
// 	scan_propertyKeys.push_back("firstName");
// 	scan_propertyKeys.push_back("lastName");
// 	scan_propertyKeys.push_back("gender");
// 	scan_propertyKeys.push_back("browserUsed");
// 	scan_propertyKeys.push_back("locationIP");
// 	scan_propertyKeys.push_back("creationDate");
	
// 	// Filter
// 	{
// 		// 14

// 	}

// 	// Expand
// 	CypherSchema expandschema = schema;
// 	expandschema.addNode("p");
// 	LabelSet exp_edgelabelset;
// 	LabelSet exp_tgtlabelset;

	
// 	// FetchId
// 	CypherSchema schema3 = expandschema;
// 	schema3.addPropertyIntoNode("p", "id", duckdb::LogicalType::UBIGINT );
	
// 	// Project
// 	// CypherSchema project_schema;
// 	// project_schema.addColumn("firstName", duckdb::LogicalType::VARCHAR);
// 	// project_schema.addColumn("lastName", duckdb::LogicalType::VARCHAR);
// 	// project_schema.addColumn("birthday", duckdb::LogicalType::BIGINT);
// 	// project_schema.addColumn("locationIP", duckdb::LogicalType::VARCHAR);
// 	// project_schema.addColumn("browserUsed", duckdb::LogicalType::VARCHAR);
// 	// project_schema.addColumn("cityId", duckdb::LogicalType::UBIGINT);
// 	// project_schema.addColumn("gender", duckdb::LogicalType::VARCHAR);
// 	// project_schema.addColumn("creationDate", duckdb::LogicalType::BIGINT);
// 	// std::vector<int> project_ordering({2, 3, 1, 6, 5, 10, 4, 7});

// 	// pipe 1
// 	std::vector<CypherPhysicalOperator *> ops;
// 		// source
// 	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys));
// 		//operators
// 	// FIXME add me again!
// 	// ops.push_back(new PhysicalFilter(filter_schema, filter_colnum, filter_value));
// 	ops.push_back(new PhysicalAdjIdxJoin(expandschema, "n", scan_labels, exp_edgelabelset, ExpandDirection::OUTGOING, exp_tgtlabelset, JoinType::INNER, false, true ));
// 	// ops.push_back(new SimpleProjection(project_schema, project_ordering));
// 		// sink
// 	ops.push_back(new PhysicalProduceResults(expandschema));
// 	auto pipe1 = new CypherPipeline(ops);
// 	auto ctx1 = new ExecutionContext(&context);
// 	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
// 	// wrap pipeline into vector
// 	std::vector<CypherPipelineExecutor*> result;
// 	result.push_back(pipeexec1);
// 	return result;

// }

// }