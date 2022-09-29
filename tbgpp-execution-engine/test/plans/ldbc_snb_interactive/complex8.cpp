#include "plans/query_plan_suite.hpp"


namespace duckdb {

CypherPipelineExecutor* ic8_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* ic8_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC8() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = ic8_pipe1(*this);
	auto p2 = ic8_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* ic8_pipe1(QueryPlanSuite& suite) {

	// scan person
	CypherSchema sch1;
	sch1.addNode("start");
	duckdb::Value filter_val; // person key
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(24189255818757); }	// demo
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(14); }
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(14); }

	// start <- m
	CypherSchema sch2 = sch1;
	sch2.addNode("m");

	// m<-comment
	CypherSchema sch3 = sch2;
	sch3.addNode("comment");

	// comment->person
	CypherSchema sch4 = sch3;
	sch4.addNode("person");

	// attach person (_start, _m, _comment, _person)
	CypherSchema sch5 = sch4;
	sch5.addPropertyIntoNode("person", "id", LogicalType::UBIGINT);
	sch5.addPropertyIntoNode("person", "firstName", LogicalType::VARCHAR);
	sch5.addPropertyIntoNode("person", "lastName", LogicalType::VARCHAR);
	PropertyKeys person_keys;
	person_keys.push_back("id");
	person_keys.push_back("firstName");
	person_keys.push_back("lastName");

	// attach comment (_start, _m, _comment, _person, person.id, person.firstname, person.lastname)
	CypherSchema sch6 = sch5;
	sch6.addPropertyIntoNode("comment", "creationDate", LogicalType::BIGINT);
	sch6.addPropertyIntoNode("comment", "id", LogicalType::UBIGINT);
	sch6.addPropertyIntoNode("comment", "content", LogicalType::VARCHAR);
	PropertyKeys comment_keys;
	comment_keys.push_back("creationDate");
	comment_keys.push_back("id");
	comment_keys.push_back("content");

	// projection (_s, _m, _c, c.cd, c.id, c.con, _p, p.id, p.fn, p.ln)
	CypherSchema sch7;
	sch7.addColumn("personId", LogicalType::UBIGINT);
	sch7.addColumn("personfirstName", LogicalType::VARCHAR);
	sch7.addColumn("personlastName", LogicalType::VARCHAR);
	sch7.addColumn("commentcreationDate", LogicalType::BIGINT);
	sch7.addColumn("commentId", LogicalType::UBIGINT);
	sch7.addColumn("commentContent", LogicalType::VARCHAR);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 7) ) );
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 8) ) );	
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 9) ) );	
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3) ) );	
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 4) ) );	
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5) ) );	
	}

	// topn
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3);	// ccd desc
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 4);	// cid asc
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));	
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "start", LabelSet("Person"), LabelSet("HAS_CREATOR"), ExpandDirection::INCOMING, LabelSet("Comment"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch3, "m", LabelSet("Comment"), LabelSet("REPLY_OF_COMMENT"), ExpandDirection::INCOMING, LabelSet("Comment"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch4, "comment", LabelSet("Comment"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalNodeIdSeek(sch5, "person", LabelSet("Person"), person_keys));
	ops.push_back( new PhysicalNodeIdSeek(sch6, "comment", LabelSet("Comment"), comment_keys));
	ops.push_back( new PhysicalProjection(sch7, move(proj_exprs)));
	ops.push_back( new PhysicalTopNSort(sch7, move(orders), (idx_t)20, (idx_t)0) );

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;

}

CypherPipelineExecutor* ic8_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	CypherSchema sch7;
	sch7.addColumn("personId", LogicalType::UBIGINT);
	sch7.addColumn("personfirstName", LogicalType::VARCHAR);
	sch7.addColumn("personlastName", LogicalType::VARCHAR);
	sch7.addColumn("commentcreationDate", LogicalType::BIGINT);
	sch7.addColumn("commentId", LogicalType::UBIGINT);
	sch7.addColumn("commentContent", LogicalType::VARCHAR);

	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// ops
	// sink
	ops.push_back( new PhysicalProduceResults(sch7) );

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}

}