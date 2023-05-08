#include "plans/query_plan_suite.hpp"



namespace duckdb {

CypherPipelineExecutor* is7_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* is7_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS7() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = is7_pipe1(*this);
	auto p2 = is7_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;

}

CypherPipelineExecutor* is7_pipe1(QueryPlanSuite& suite) {

// scan message
	Schema sch1;
	sch1.addNode("m");
	duckdb::Value filter_val; // person key
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(2199029886840); } // verified
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(3); }	// TODO fixme
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(0); }	// TODO fixme

// expand  m->c
	Schema sch2 = sch1;
	sch2.addNode("c");

// expand c->p
	Schema sch3 = sch2;
	sch3.addNode("p");

// optwxpand m->a (in : _m, _c, _p )
	Schema sch4 = sch3;
	sch4.addNode("a");

// optexpand a-r->p' (in ; _m _c _p _a)
	Schema sch5 = sch4;
	sch5.addEdge("r");
	sch5.addNode("p1");

// filter p == null or p = p1 (in : _m _c _p _a _r _p1 )
	vector<unique_ptr<Expression>> into_predicates;
	{
		auto test_null_val = Value(LogicalType::UBIGINT);	// is this right way to define null?
		D_ASSERT( test_null_val.IsNull() );
		auto filter_expr1_lhs = make_unique<BoundComparisonExpression>(
				ExpressionType::COMPARE_EQUAL,
				std::move( make_unique<BoundReferenceExpression>(LogicalType::ID, 2) ),	// p
				std::move( make_unique<BoundConstantExpression>( Value(LogicalType::ID)) )	// null
		);
		auto filter_expr1_rhs = make_unique<BoundComparisonExpression>(
				ExpressionType::COMPARE_EQUAL,
				std::move( make_unique<BoundReferenceExpression>(LogicalType::ID, 2) ),	// p
				std::move( make_unique<BoundReferenceExpression>(LogicalType::ID, 5) )	// p1
		);
		// or
		auto filter_expr1 = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(filter_expr1_lhs), move(filter_expr1_rhs));
		into_predicates.push_back(std::move(filter_expr1));
	}

// fetch c (in: _m _c _p _a _r _p1 )
	Schema sch6 = sch5;
	sch6.addPropertyIntoNode("c", "content", LogicalType::VARCHAR);
	sch6.addPropertyIntoNode("c", "creationDate", LogicalType::BIGINT);
	PropertyKeys c_keys;
	c_keys.push_back( "content" );
	c_keys.push_back( "creationDate" );

// fetch p (in: _m _c c.c c.cd _p _a _r _p1)
	Schema sch7 = sch6;
	sch7.addPropertyIntoNode("p", "id", LogicalType::UBIGINT);
	sch7.addPropertyIntoNode("p", "firstName", LogicalType::VARCHAR);
	sch7.addPropertyIntoNode("p", "lastName", LogicalType::VARCHAR);
	PropertyKeys p_keys;
	p_keys.push_back( "id" );
	p_keys.push_back( "firstName" );
	p_keys.push_back( "lastName" );
	

//  (in: _m _c c.c c.cd _p p.id p.fn p.ln _a _r _p1)
//  (out: c.c c.cd p.id p.fn p.ln bool )
	Schema sch8;
	sch8.addColumn("commentContent", duckdb::LogicalType::VARCHAR);
	sch8.addColumn("commentCreationDate", duckdb::LogicalType::BIGINT);
	sch8.addColumn("replyAuthorId", duckdb::LogicalType::UBIGINT);
	sch8.addColumn("replyAuthorFirstName", duckdb::LogicalType::VARCHAR);
	sch8.addColumn("replyAuthorLastName", duckdb::LogicalType::VARCHAR);
	sch8.addColumn("replyAuthorKnowsOriginalMessageAuthor", duckdb::LogicalType::BOOLEAN);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 2)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 5)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 7)) );

		auto e_when_lhs = make_unique<BoundReferenceExpression>(LogicalType::ID, 9);	// r
		auto e_when_rhs = make_unique<BoundConstantExpression>( Value(LogicalType::ID) );	// null
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(e_when_lhs), move(e_when_rhs));
		auto e_then = make_unique<BoundConstantExpression>( Value::BOOLEAN(false) );
		auto e_else = make_unique<BoundConstantExpression>(  Value::BOOLEAN(true) );
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		proj_exprs.push_back( move(e) );
	}

// order by
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1);	// ccd
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);	// replyauthorid
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));


	// Note that this is a left deep plan. This plan type reduces #pipeline(3->2) but larger intermediate result size.
	// anothor possible is bushy plan (i.e. LeftHJ ( m-c-p, m-a-r-p )

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Post"), vector<string>(), "id", filter_val));	// post only
	//ops
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "m", LabelSet("Post"), LabelSet("REPLY_OF"), ExpandDirection::INCOMING, LabelSet("Comment"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch3, "c", LabelSet("Comment"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch4, "m", LabelSet("Comment"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::LEFT, false, true));	// optional m->a
	ops.push_back( new PhysicalAdjIdxJoin(sch5, "a", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::LEFT, true, true)); // optional a-r->p1 // loadedge true
	ops.push_back( new PhysicalFilter(sch5, move(into_predicates)));
	ops.push_back( new PhysicalNodeIdSeek(sch6, "c", LabelSet("Comment"), c_keys));
	ops.push_back( new PhysicalNodeIdSeek(sch7, "p", LabelSet("Person"), p_keys));
	ops.push_back( new PhysicalProjection(sch8, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalTopNSort(sch8, move(orders), (idx_t) 1000, (idx_t)0)); // order

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;	
}

CypherPipelineExecutor* is7_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// order by
	Schema sch8;
	sch8.addColumn("commentContent", duckdb::LogicalType::VARCHAR);
	sch8.addColumn("commentCreationDate", duckdb::LogicalType::BIGINT);
	sch8.addColumn("replyAuthorId", duckdb::LogicalType::UBIGINT);
	sch8.addColumn("replyAuthorFirstName", duckdb::LogicalType::VARCHAR);
	sch8.addColumn("replyAuthorLastName", duckdb::LogicalType::VARCHAR);
	sch8.addColumn("replyAuthorKnowsOriginalMessageAuthor", duckdb::LogicalType::BOOLEAN);

	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// ops
	// sink
	ops.push_back( new PhysicalProduceResults(sch8) );

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;

}



}