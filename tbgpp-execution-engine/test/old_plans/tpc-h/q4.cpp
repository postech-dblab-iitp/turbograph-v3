#include "plans/query_plan_suite.hpp"
#include "function/aggregate/distributive_functions.hpp"

namespace duckdb {

CypherPipelineExecutor* q4_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* q4_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
CypherPipelineExecutor* q4_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::TPCH_Q4() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = q4_pipe1(*this);
	auto p2 = q4_pipe2(*this, p1);
	auto p3 = q4_pipe3(*this, p2);
	result.push_back(p1);
	result.push_back(p2);
	result.push_back(p3);
	return result;
}

CypherPipelineExecutor* q4_pipe1(QueryPlanSuite& suite) {

	// scan ORDERS;
	Schema sch1;
	sch1.addNode("o");
	sch1.addPropertyIntoNode("o", "O_ORDERDATE", LogicalType::DATE);
	sch1.addPropertyIntoNode("o", "O_ORDERPRIORITY", LogicalType::VARCHAR);
	PropertyKeys o_keys({"O_ORDERDATE", "O_ORDERPRIORITY"});

	// filter (_o, o.od, o.p)
	vector<unique_ptr<Expression>> filter_exprs;
	Value start; Value end;
	if( suite.TPCH_SF==1 ) { 
		start = Value::DATE(1994,1,1);
		end = Value::DATE(1994,4,1);
	} else if( suite.TPCH_SF==10 ) {
		start = Value::DATE(1995,8,1);
		end = Value::DATE(1995,11,1);
	}
	{
// FIXME change predicate when SF changes
		// https://www.timeanddate.com/date/durationresult.html
		auto filter_expr1 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 1) ),	// orderdate
			move( make_unique<BoundConstantExpression>(start)) 
		);
		auto filter_expr2 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_LESSTHAN,
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 1) ),
			move( make_unique<BoundConstantExpression>(end))
		);
		filter_exprs.push_back(move(filter_expr1));
		filter_exprs.push_back(move(filter_expr2));
	}

	// adjidxjoin (_o, o.od, o.p) o->l
	Schema sch2 = sch1;
	sch2.addNode("l");

	// fetch (_o, o.od, o.p, _l)
	Schema sch3 = sch2;
	sch3.addPropertyIntoNode("l", "L_COMMITDATE", LogicalType::DATE);
	sch3.addPropertyIntoNode("l", "L_RECEIPTDATE", LogicalType::DATE);

	// filter (_o o.od o.p _l l.cd, l.rd)
	vector<unique_ptr<Expression>> filter_exprs_2;
	{
		auto filter_expr1 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_LESSTHAN,	// commitdate < receiptdate
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 4) ),		// commitdate
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 5) )			// receiptdate
		);
		filter_exprs_2.push_back(move(filter_expr1));
	}

	// aggregate ( _o, o.od, o.op _l, l.cd, l.rd,)
	Schema sch4;
	sch4.addColumn("O_ORDERPRIORITY", LogicalType::VARCHAR);
	sch4.addColumn("ORDER_COUNT", LogicalType::BIGINT );

	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	// 1 keys (groups)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 2) ); // o.orderpriority
	// 1 agg expression
	auto agg_expr_func = CountFun::GetFunction();
	vector<unique_ptr<Expression>> agg_expr_1_child;
	agg_expr_1_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) );	// 0
	agg_exprs.push_back(
		make_unique<BoundAggregateExpression>(agg_expr_func, move(agg_expr_1_child), nullptr, nullptr, true )	// select count( distinct _o ) group by priorty
	); // count( distinct orders._id )


// // pipes
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("ORDERS"), o_keys) );
	ops.push_back( new PhysicalFilter(sch1, move(filter_exprs)) );
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "o", LabelSet("ORDERS"), LabelSet("IS_PART_OF_BACKWARD"), ExpandDirection::OUTGOING, LabelSet("LINEITEM"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalNodeIdSeek(sch3, "l", LabelSet("LINEITEM"), PropertyKeys({"L_COMMITDATE", "L_RECEIPTDATE"})));
	ops.push_back( new PhysicalFilter(sch3, move(filter_exprs_2)));
	ops.push_back( new PhysicalHashAggregate(sch4, move(agg_exprs), move(agg_groups)));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

CypherPipelineExecutor* q4_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	Schema sch4;
	sch4.addColumn("O_ORDERPRIORITY", LogicalType::VARCHAR);
	sch4.addColumn("ORDER_COUNT", LogicalType::BIGINT );

	// sort
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 0);	// orderpriroity
	BoundOrderByNode order1(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));

// pipes
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	// sink
	ops.push_back( new PhysicalTopNSort(sch4, move(orders), (idx_t)100, (idx_t)0));

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;

}

CypherPipelineExecutor* q4_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	// produce
	Schema sch4;
	sch4.addColumn("O_ORDERPRIORITY", LogicalType::VARCHAR);
	sch4.addColumn("ORDER_COUNT", LogicalType::BIGINT );

// pipes
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	// sink
	ops.push_back( new PhysicalProduceResults(sch4) );


	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}

}