#include "plans/query_plan_suite.hpp"

namespace duckdb {

CypherPipelineExecutor* is5_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS5() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = is5_pipe1(*this);
	result.push_back(p1);
	return result;

}

CypherPipelineExecutor* is5_pipe1(QueryPlanSuite& suite) {

// scan message
	CypherSchema sch1;
	sch1.addNode("m");
	unique_ptr<Expression> filter_expr1;
	// Filter
	duckdb::Value filter_val; // person key
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(57459); }
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(58929); }
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(19560); }

// expand
	CypherSchema sch2 = sch1;
	sch2.addNode("p");

// fetch (in : _m, _p)
	CypherSchema sch3 = sch2;
	sch3.addPropertyIntoNode("p", "id", LogicalType::UBIGINT);
	sch3.addPropertyIntoNode("p", "firstName", LogicalType::VARCHAR);
	sch3.addPropertyIntoNode("p", "lastName", LogicalType::VARCHAR);
	PropertyKeys p_keys;
	p_keys.push_back("id");
	p_keys.push_back("firstName");
	p_keys.push_back("lastName");

// project (in: _m, _p, p.id, p.fn, p.ln)
	CypherSchema sch5;
	sch5.addColumn("personId", LogicalType::UBIGINT);
	sch5.addColumn("firstName", LogicalType::VARCHAR);
	sch5.addColumn("lastName", LogicalType::VARCHAR);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2) ) );	// fr.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3) ) );	// fr.fn
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4) ) );	// fr.ln
	}

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Comment"), vector<string>(), "id", filter_val));	// comment only
	//ops
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "m", LabelSet("Comment"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalNodeIdSeek(sch3, "p", LabelSet("Person"), p_keys));
	ops.push_back( new PhysicalProjection(sch5, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalProduceResults(sch5));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

}