// #include "plans/query_plan_suite.hpp"
// #include "function/aggregate/distributive_functions.hpp"

// namespace duckdb {

// CypherPipelineExecutor* q4_pipe1(QueryPlanSuite& suite);
// CypherPipelineExecutor* q4_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
// CypherPipelineExecutor* q4_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::TPCH_Q4() {

// 	std::vector<CypherPipelineExecutor*> result;
// 	auto p1 = q4_pipe1(*this);
// 	auto p2 = q4_pipe2(*this, p1);
// 	auto p3 = q4_pipe3(*this, p2);
// 	result.push_back(p1);
// 	result.push_back(p2);
// 	result.push_back(p3);
// 	return result;
// }

// CypherPipelineExecutor* q4_pipe1(QueryPlanSuite& suite) {

// 	// scan LINEITEM;
// 	CypherSchema sch1;
// 	sch1.addNode("l");
// 	sch1.addPropertyIntoNode("l", "L_COMMITDATE", LogicalType::DATE);
// 	sch1.addPropertyIntoNode("l", "L_RECEIPTDATE", LogicalType::DATE);

// 	// filter (_l, l.lc, l.lr)
// 	vector<unique_ptr<Expression>> filter_exprs;
// 	{
// 		auto filter_expr1 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_LESSTHAN,	// commitdate < receiptdate
// 			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 1) ),		// commitdate
// 			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 2) )			// receiptdate
// 		);
// 		filter_exprs.push_back(move(filter_expr1));
// 	}
// 	// expand (_l, l.lc, l.lr)
// 	CypherSchema sch2 = sch1;
// 	sch2.addNode("o");

// 	// seek (_l, l.lc, l.lr, _o)
// 	CypherSchema sch3 = sch2;
// 	sch3.addPropertyIntoNode("o", "O_ORDERDATE", LogicalType::DATE);
// 	sch3.addPropertyIntoNode("o", "O_ORDERPRIORITY", LogicalType::VARCHAR);
// 	PropertyKeys o_keys({"O_ORDERDATE", "O_ORDERPRIORITY"});

// 	// filter (_l, l.lc, l.lr, _o, o.od, o.op)
// 	vector<unique_ptr<Expression>> filter_exprs_2;
// 	{
// // FIXME change predicate when SF changes
// 		// https://www.timeanddate.com/date/durationresult.html
// 		auto filter_expr1 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_GREATERTHANOREQUALTO,
// 			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 4) ),	// orderdate
// 			move( make_unique<BoundConstantExpression>(Value::DATE(1994,1,1))) 
// 		);
// 		auto filter_expr2 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_LESSTHAN,
// 			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 4) ),
// 			move( make_unique<BoundConstantExpression>(Value::DATE(1994,4,1)))
// 		);
// 		filter_exprs_2.push_back(move(filter_expr1));
// 		filter_exprs_2.push_back(move(filter_expr2));
// 	}

// 	// aggregate (_l, l.lc, l.lr, _o, o.od, o.op)
// 	CypherSchema sch4;
// 	sch4.addColumn("O_ORDERPRIORITY", LogicalType::VARCHAR);
// 	sch4.addColumn("ORDER_COUNT", LogicalType::BIGINT );

// 	vector<unique_ptr<Expression>> agg_exprs;
// 	vector<unique_ptr<Expression>> agg_groups;
// 	// 1 keys (groups)
// 	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5) ); // o.orderpriority
// 	// 1 agg expression
// 	auto agg_expr_func = CountStarFun::GetFunction();
// 	vector<unique_ptr<Expression>> agg_expr_1_child;
// 	agg_exprs.push_back(
// 		make_unique<BoundAggregateExpression>(agg_expr_func, move(agg_expr_1_child), nullptr, nullptr, false )
// 	); // count(*)

// // pipes
// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("LINEITEM"), PropertyKeys({"L_COMMITDATE", "L_RECEIPTDATE"})) );
// 	ops.push_back( new PhysicalFilter(sch1, move(filter_exprs)) );
// 	ops.push_back( new PhysicalAdjIdxJoin(sch2, "l", LabelSet("LINEITEM"), LabelSet("IS_PART_OF"), ExpandDirection::OUTGOING, LabelSet("ORDERS"), JoinType::INNER, false, true));
// 	ops.push_back( new PhysicalNodeIdSeek(sch3, "o", LabelSet("ORDERS"), o_keys));
// 	ops.push_back( new PhysicalFilter(sch3, move(filter_exprs_2)));
// 	ops.push_back( new PhysicalHashAggregate(sch4, move(agg_exprs), move(agg_groups)));

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
// 	return pipeexec;
// }

// CypherPipelineExecutor* q4_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	CypherSchema sch4;
// 	sch4.addColumn("O_ORDERPRIORITY", LogicalType::VARCHAR);
// 	sch4.addColumn("ORDER_COUNT", LogicalType::BIGINT );

// 	// sort
// 	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1);	// orderpriroity
// 	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
// 	vector<BoundOrderByNode> orders;
// 	orders.push_back(move(order1));

// // pipes
// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( prev_pipe->pipeline->GetSink() );
// 	// op
// 	// sink
// 	ops.push_back( new PhysicalTopNSort(sch4, move(orders), (idx_t)100000, (idx_t)0));

// 	vector<CypherPipelineExecutor*> childs;
// 	childs.push_back(prev_pipe);

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
// 	return pipeexec;

// }

// CypherPipelineExecutor* q4_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	// produce
// 	CypherSchema sch4;
// 	sch4.addColumn("O_ORDERPRIORITY", LogicalType::VARCHAR);
// 	sch4.addColumn("ORDER_COUNT", LogicalType::BIGINT );

// // pipes
// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( prev_pipe->pipeline->GetSink() );
// 	// op
// 	// sink
// 	ops.push_back( new PhysicalProduceResults(sch4) );


// 	vector<CypherPipelineExecutor*> childs;
// 	childs.push_back(prev_pipe);

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
// 	return pipeexec;
// }

// }