#include "plans/query_plan_suite.hpp"

namespace duckdb {

CypherPipelineExecutor* is3_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* is3_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS3() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = is3_pipe1(*this);
	auto p2 = is3_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;

}

CypherPipelineExecutor* is3_pipe1(QueryPlanSuite& suite) {

// scan person 
	CypherSchema sch1;
	sch1.addNode("n");
	duckdb::Value filter_val; // person key
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(35184372099695); }	// verified for samsung demo
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(14); }
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(14); }

// expand (in ; _n)
	CypherSchema sch2 = sch1;
	sch2.addEdge("r");
	sch2.addNode("friend");
	
// fetch edge r ( in : _n, _r, _friend)
	CypherSchema sch3 = sch2;
	sch3.addPropertyIntoEdge("r", "creationDate", LogicalType::BIGINT);
	PropertyKeys r_keys;
	r_keys.push_back("creationDate");

// fetch friend (in : _n, _r, r.creationdAte, _friend)
	CypherSchema sch4 = sch3;
	sch4.addPropertyIntoNode("friend", "id", LogicalType::UBIGINT);
	sch4.addPropertyIntoNode("friend", "firstName", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("friend", "lastName", LogicalType::VARCHAR);
	PropertyKeys friend_keys;
	friend_keys.push_back("id");
	friend_keys.push_back("firstName");
	friend_keys.push_back("lastName");

// project  (in : _n, _r, r.creationdAte, _friend, fr.id, fr.fn, fr.ln)
	CypherSchema sch5;
	sch5.addColumn("personId", LogicalType::UBIGINT);
	sch5.addColumn("firstName", LogicalType::VARCHAR);
	sch5.addColumn("lastName", LogicalType::VARCHAR);
	sch5.addColumn("friendshipCreationDate", LogicalType::BIGINT);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 4) ) );	// fr.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5) ) );	// fr.fn
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6) ) );	// fr.ln
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 2) ) );	// r.cd
	}

// orderby (in : pid, fn, ln, fcd)
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3);
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 0);
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
	//ops
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "n", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::INCOMING, LabelSet("Person"), JoinType::INNER, true, true));
	ops.push_back( new PhysicalEdgeIdSeek(sch3, "r", LabelSet("KNOWS"), r_keys));
	ops.push_back( new PhysicalNodeIdSeek(sch4, "friend", LabelSet("Person"), friend_keys));
	ops.push_back( new PhysicalProjection(sch5, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalTopNSort(sch5, move(orders), (idx_t) 1000, (idx_t)0)); // 

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


CypherPipelineExecutor* is3_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) { 

	CypherSchema sch1;
	sch1.addColumn("personId", LogicalType::UBIGINT);
	sch1.addColumn("firstName", LogicalType::VARCHAR);
	sch1.addColumn("lastName", LogicalType::VARCHAR);
	sch1.addColumn("friendshipCreationDate", LogicalType::BIGINT);

	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// ops
	// sink
	ops.push_back( new PhysicalProduceResults(sch1) );

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;

}

}