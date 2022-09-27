#include "plans/query_plan_suite.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "function/scalar/string_functions.hpp"

#include "icecream.hpp"
namespace duckdb {

CypherPipelineExecutor* q13_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* q13_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
CypherPipelineExecutor* q13_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
CypherPipelineExecutor* q13_pipe4(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::TPCH_Q13() {
icecream::ic.disable();
	std::vector<CypherPipelineExecutor*> result;
IC();
	auto p1 = q13_pipe1(*this);
	IC();
	auto p2 = q13_pipe2(*this, p1);
	IC();
	auto p3 = q13_pipe3(*this, p2);
	IC();
	auto p4 = q13_pipe4(*this, p3);
	result.push_back(p1);
	result.push_back(p2);
	result.push_back(p3);
	result.push_back(p4);
	IC();
icecream::ic.disable();
	return result;

}

CypherPipelineExecutor* q13_pipe1(QueryPlanSuite& suite) {
	
	// scan o with O_COMMENT
	CypherSchema sch1;
	sch1.addNode("o");
	sch1.addPropertyIntoNode("o", "O_COMMENT", LogicalType::VARCHAR);

	// optional match (_o, o.c)
	CypherSchema sch2 = sch1;
	sch2.addNode("c");

	// aggregate (_o, o.c, _c)
	CypherSchema sch3;
	sch3.addNode("c");
	sch3.addColumn("c_count", LogicalType::BIGINT);
	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	// 1 keys (groups)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // _c
	// 1 agg expression
	vector<unique_ptr<Expression>> agg_expr_1_child;
	auto agg_expr_func = CountStarFun::GetFunction();
	agg_exprs.push_back(
		make_unique<BoundAggregateExpression>(agg_expr_func, move(agg_expr_1_child), nullptr, nullptr, false )
	); 

// pipes
	std::vector<CypherPhysicalOperator *> ops;

	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("ORDERS"), PropertyKeys({"O_COMMENT"}) ) );
	// ops.push_back( new PhysicalFilter(sch1) );	// FIXME filter on like
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "o", LabelSet("ORDERS"), LabelSet("MADE_BY"), ExpandDirection::OUTGOING, LabelSet("CUSTOMER"), JoinType::INNER, false, true) );	// FIXME need outer join
	ops.push_back( new PhysicalHashAggregate(sch3, move(agg_exprs), move(agg_groups)) );

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

CypherPipelineExecutor* q13_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe ) {

	// aggregate2 (c_id, c_count) -> (c_count, count(cid))
	CypherSchema sch4;
	sch4.addColumn("c_count", LogicalType::BIGINT);
	sch4.addColumn("custdist", LogicalType::BIGINT );

	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	// 1 keys (groups)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1) ); // c_count
	// 1 agg expression
	vector<unique_ptr<Expression>> agg_expr_1_child;
	auto agg_expr_func = CountStarFun::GetFunction();
	agg_exprs.push_back(
		make_unique<BoundAggregateExpression>(agg_expr_func, move(agg_expr_1_child), nullptr, nullptr, false )
	); 

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	// sink
	ops.push_back( new PhysicalHashAggregate(sch4, move(agg_exprs), move(agg_groups)) );

// pipes, add child
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}


CypherPipelineExecutor* q13_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	// orderby (c_count, custdist)
	CypherSchema sch4;
	sch4.addColumn("c_count", LogicalType::BIGINT);
	sch4.addColumn("custdist", LogicalType::BIGINT );

	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1);	// custdist
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 0);	// c_count
	BoundOrderByNode order2(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));


	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	// sink
	ops.push_back( new PhysicalTopNSort(sch4, move(orders), (idx_t)1000, (idx_t)0) );

// pipes, add child
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}

CypherPipelineExecutor* q13_pipe4(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	// produce!
	CypherSchema sch4;
	sch4.addColumn("c_count", LogicalType::INTEGER);
	sch4.addColumn("custdist", LogicalType::INTEGER );
	
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	// sink
	ops.push_back( new PhysicalProduceResults(sch4));

// pipes, add child
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;


}


}