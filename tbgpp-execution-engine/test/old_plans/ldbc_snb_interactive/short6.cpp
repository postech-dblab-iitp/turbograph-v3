#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS6() {

	// scan message
	CypherSchema sch1;
	sch1.addNode("m");

	// Filter
	duckdb::Value filter_val; // message key
	if(LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(2199029886840); }
	if(LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(58929); }
	if(LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(19560); }

	// m -> p
	CypherSchema sch2;
	sch2.addNode("p");

	// p <- f
	CypherSchema sch3;
	sch3.addNode("f");

	// f -> mod
	CypherSchema sch4;
	sch4.addNode("mod");
	
	// Fetch f (_m _p _f _mod)
	CypherSchema sch5 = sch4;
	sch5.addPropertyIntoNode("f", "id", duckdb::LogicalType::UBIGINT );
	sch5.addPropertyIntoNode("f", "title", duckdb::LogicalType::VARCHAR );
	PropertyKeys f_keys;
	f_keys.push_back("id");
	f_keys.push_back("title");

	// fetch mod (_m _p _f f.id f.title _mod)
	CypherSchema sch6 = sch5;
	sch6.addPropertyIntoNode("mod", "id", duckdb::LogicalType::UBIGINT );
	sch6.addPropertyIntoNode("mod", "firstName", duckdb::LogicalType::VARCHAR );
	sch6.addPropertyIntoNode("mod", "lastName", duckdb::LogicalType::VARCHAR );
	PropertyKeys m_keys;
	m_keys.push_back("id");
	m_keys.push_back("firstName");
	m_keys.push_back("lastName");

	// Project (_m _p _f f.id f.title _mod mod.id mod.fn mod.ln)

		// out : 
	CypherSchema project_schema;
	project_schema.addColumn("forumId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("forumTitle", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("moderatorId", duckdb::LogicalType::UBIGINT);
	project_schema.addColumn("moderatorFirstName", duckdb::LogicalType::VARCHAR);
	project_schema.addColumn("moderatorLastName", duckdb::LogicalType::VARCHAR);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 3)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 6)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 7)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 8)) );
	}
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new PhysicalNodeScan(sch1, LabelSet("Comment"), vector<string>(), "id", filter_val));
		//operators
	ops.push_back(new PhysicalAdjIdxJoin(sch2, "m", LabelSet("Comment"), LabelSet("REPLY_OF"), ExpandDirection::OUTGOING, LabelSet("Post"), JoinType::INNER, false, true));
	ops.push_back(new PhysicalAdjIdxJoin(sch3, "p", LabelSet("Post"), LabelSet("CONTAINER_OF"), ExpandDirection::INCOMING, LabelSet("Forum"), JoinType::INNER, false, true));
	ops.push_back(new PhysicalAdjIdxJoin(sch4, "f", LabelSet("Forum"), LabelSet("HAS_MODERATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalNodeIdSeek(sch5, "f", LabelSet("Forum"), f_keys));
	ops.push_back( new PhysicalNodeIdSeek(sch6, "p", LabelSet("Person"), m_keys));
	ops.push_back(new PhysicalProjection(project_schema, move(proj_exprs)));
		// sink
	ops.push_back(new PhysicalProduceResults(project_schema));
	
	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
	
}

}