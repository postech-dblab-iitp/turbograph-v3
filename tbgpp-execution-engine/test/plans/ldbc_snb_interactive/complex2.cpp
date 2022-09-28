#include "plans/query_plan_suite.hpp"


namespace duckdb {

CypherPipelineExecutor* ic2_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* ic2_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC2() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = ic2_pipe1(*this);
	auto p2 = ic2_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* ic2_pipe1(QueryPlanSuite& suite) {

// scan person
	CypherSchema sch1;
	sch1.addNode("n");
	duckdb::Value filter_val; // person key
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(14); }
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(14); }
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(14); }

// p->friend
	CypherSchema sch2 = sch1;
	sch2.addNode("friend");

// friend<-message (in : _n, _friend)
	CypherSchema sch3 = sch2;
	sch3.addNode("message"); // post

// attach message ( _n _friend _message)
	CypherSchema sch4 = sch3;
	sch4.addPropertyIntoNode("message", "id", LogicalType::UBIGINT);
	sch4.addPropertyIntoNode("message", "content", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("message", "creationDate", LogicalType::BIGINT);
	PropertyKeys m_keys;
	m_keys.push_back("id");
	m_keys.push_back("content");
	m_keys.push_back("creationDate");

// filter  (_n _f _m m.id m.c m.cd)
	vector<unique_ptr<Expression>> filter_exprs;
	{
		unique_ptr<Expression> filter_expr1;
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, 
							make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 5),
							make_unique<BoundConstantExpression>(Value::BIGINT( 1354060800000 ))
						);
		filter_exprs.push_back(move(filter_expr1));
	}

// attach friend
	CypherSchema sch5 = sch4;
	sch5.addPropertyIntoNode("friend", "id", LogicalType::UBIGINT);
	sch5.addPropertyIntoNode("friend", "firstName", LogicalType::VARCHAR);
	sch5.addPropertyIntoNode("friend", "lastName", LogicalType::VARCHAR);
	PropertyKeys f_keys;
	f_keys.push_back("id");
	f_keys.push_back("firstName");
	f_keys.push_back("lastName");

// projection (_n _f f.id f.fn f.ln _m m.id m.c m.cd)
	CypherSchema sch6;
	sch6.addColumn("personId", LogicalType::UBIGINT);
	sch6.addColumn("firstName", LogicalType::VARCHAR);
	sch6.addColumn("lastName", LogicalType::VARCHAR);
	sch6.addColumn("postId", LogicalType::UBIGINT);
	sch6.addColumn("content", LogicalType::VARCHAR);
	sch6.addColumn("creationDate", LogicalType::BIGINT);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2) ) );	// fr.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3) ) );	// fr.fn
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4) ) );	// fr.ln
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 6) ) );	// mid
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 7) ) );	// mc
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 8) ) );	// mcd
	}

// order	
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 5);
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 3);
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
	//ops
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "n", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch3, "friend", LabelSet("Person"), LabelSet("POST_HAS_CREATOR"), ExpandDirection::INCOMING, LabelSet("Post"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalNodeIdSeek(sch4, "message", LabelSet("Post"), m_keys));
	ops.push_back( new PhysicalFilter(sch4, move(filter_exprs)));
	ops.push_back( new PhysicalNodeIdSeek(sch5, "friend", LabelSet("Person"), f_keys));
	ops.push_back( new PhysicalProjection(sch6, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalTopNSort(sch6, move(orders), (idx_t) 20, (idx_t)0)); // 

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;

}

CypherPipelineExecutor* ic2_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// order
	CypherSchema sch6;
	sch6.addColumn("personId", LogicalType::UBIGINT);
	sch6.addColumn("firstName", LogicalType::VARCHAR);
	sch6.addColumn("lastName", LogicalType::VARCHAR);
	sch6.addColumn("postId", LogicalType::BIGINT);
	sch6.addColumn("content", LogicalType::VARCHAR);
	sch6.addColumn("creationdate", LogicalType::BIGINT);

	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// ops
	// sink
	ops.push_back( new PhysicalProduceResults(sch6) );

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;

}



}