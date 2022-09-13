#include "plans/query_plan_suite.hpp"

namespace duckdb {

CypherPipelineExecutor* is2_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* is2_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS2() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = is2_pipe1(*this);
	auto p2 = is2_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* is2_pipe1(QueryPlanSuite& suite) {

// scan person
	CypherSchema sch1;
	sch1.addNode("UNNAMED1");

// FIXME
	duckdb::Value filter_val;
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(57459); }
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(58929); }
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(19560); }

// expand
	CypherSchema sch2 = sch1;
	sch2.addNode("message");

// fetch properties
	CypherSchema sch3 = sch2;
	sch3.addPropertyIntoNode("message", "id", LogicalType::UBIGINT);
	sch3.addPropertyIntoNode("message", "creationDate", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("message", "content", duckdb::LogicalType::VARCHAR);
	PropertyKeys exp_pkeys;
	exp_pkeys.push_back("id");
	exp_pkeys.push_back("creationDate");
	exp_pkeys.push_back("content");

// project
	CypherSchema sch4;
	sch4.addNode("message");
	sch4.addColumn("messageId", LogicalType::UBIGINT);
	sch4.addColumn("messageCreationDate", LogicalType::BIGINT);
	sch4.addColumn("messageContent", LogicalType::VARCHAR);
	// 0 1 2 3 4
	// _ _ i c c => _ i c c
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::ID, 1) ) );		// _mid
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2) ) );	// m.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3) ) );	// m.cDate
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4) ) );	// m.content
	}

// topn (in : _mid, m.id, m.date)
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 2);
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1);
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), PropertyKeys(), "id", filter_val ) );
	// ops
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "UNNAMED1", LabelSet("Person"), LabelSet("HAS_CREATOR"), ExpandDirection::INCOMING, LabelSet("Comment"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalNodeIdSeek(sch3, "message", LabelSet("Comment"), exp_pkeys));
	ops.push_back( new PhysicalProjection(sch4, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalTopNSort(sch4, move(orders), (idx_t)10, (idx_t)0));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

CypherPipelineExecutor* is2_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// get source

// varlenexpand
	CypherSchema sch1;
	sch1.addNode("message");
	sch1.addColumn("messageId", LogicalType::UBIGINT);
	sch1.addColumn("messageCreationDate", LogicalType::BIGINT);
	sch1.addColumn("messageContent", LogicalType::VARCHAR);
	sch1.addNode("post");

// expand
	CypherSchema sch2 = sch1;
	sch2.addNode("person");

// nodeidseek (in : _m, mid, mcd, mc, _post, _per)
	CypherSchema sch3 = sch2;
	sch3.addPropertyIntoNode("post", "id", LogicalType::UBIGINT);
	PropertyKeys seek1_prop;
	seek1_prop.push_back("id");

// nodeidseek (in : _m, mid, mcd, mc, _post, post.id, _per )
	CypherSchema sch3_1 = sch3;
	sch3_1.addPropertyIntoNode("person", "id", LogicalType::UBIGINT);
	sch3_1.addPropertyIntoNode("person", "firstName", LogicalType::VARCHAR);
	sch3_1.addPropertyIntoNode("person", "lastName", LogicalType::VARCHAR);
	PropertyKeys seek2_prop;
	seek2_prop.push_back("id");
	seek2_prop.push_back("firstName");
	seek2_prop.push_back("lastName");
	
// project (in ; _m., mid, mcd, mc, _post, post.id, _per, per.id, per.fn, per.ln )
	CypherSchema sch4;
	sch4.addColumn("messageId", LogicalType::UBIGINT);
	sch4.addColumn("messageContent", LogicalType::VARCHAR);
	sch4.addColumn("messageCreationDate", LogicalType::BIGINT);
	sch4.addColumn("postId", LogicalType::UBIGINT);
	sch4.addColumn("personId", LogicalType::UBIGINT);
	sch4.addColumn("personFirstName", LogicalType::VARCHAR);
	sch4.addColumn("personLastName", LogicalType::VARCHAR);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1)) );	// mi
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3)) );	// content
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 2)) );	// cdate
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 5)) );	// postid
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 7)) );	// personid
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 8)) );	// fn
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 9)) );	// ln
	}

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// ops
// FIXME here
	//ops.push_back( new Physical)	// TODO here
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "post", LabelSet("Post"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true) );
// FIXME here ; do i need to filter by target nodes? // custom filter?
	// TODO change plan : 
	ops.push_back( new PhysicalNodeIdSeek(sch3, "post", LabelSet("Post"), seek1_prop));
	ops.push_back( new PhysicalNodeIdSeek(sch3_1, "person", LabelSet("Person"), seek2_prop));
	ops.push_back( new PhysicalProjection(sch4, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalProduceResults(sch4));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

}