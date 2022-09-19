#include "plans/query_plan_suite.hpp"
#include "function/aggregate/distributive_functions.hpp"

namespace duckdb {

CypherPipelineExecutor* q10_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* q10_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
CypherPipelineExecutor* q10_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);


std::vector<CypherPipelineExecutor*> QueryPlanSuite::TPCH_Q10() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = q10_pipe1(*this);
	auto p2 = q10_pipe2(*this, p1);
	auto p3 = q10_pipe3(*this, p2);
	result.push_back(p1);
	result.push_back(p2);
	result.push_back(p3);
	return result;

}

CypherPipelineExecutor* q10_pipe1(QueryPlanSuite& suite) {

	// scan lineitem
	CypherSchema sch1;
	sch1.addNode("l");
	sch1.addPropertyIntoNode("l", "L_RETURNFLAG", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("l", "L_EXTENDEDPRICE", LogicalType::DECIMAL(12, 2));
	sch1.addPropertyIntoNode("l", "L_DISCOUNT", LogicalType::DECIMAL(12, 2));
	duckdb::Value filter_val = duckdb::Value((string_t)"R");

// FIXME further pushdown (later)
	// filter_expr (_l, l.rf, l.ep, l.d)
	CypherSchema sch2 = sch1;
	vector<unique_ptr<Expression>> filter_exprs;
	{
		unique_ptr<Expression> filter_expr1;
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, 
							make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1),
							make_unique<BoundConstantExpression>(Value( (string_t) "R" ))
						);
		filter_exprs.push_back(move(filter_expr1));
	}

	// join l->o ( _l, l.rf, l.ep, l.d)
	CypherSchema sch3 = sch2;
	sch3.addNode("o");
	
	// fetch o (_l, l.rf, l.ep, l.d, _o)
	CypherSchema sch4 = sch3;
	sch4.addPropertyIntoNode("o", "O_ORDERDATE", LogicalType::DATE);

	// filter date range (_l, l.rf, l.ep, l.d, _o, o.od)
	vector<unique_ptr<Expression>> filter_exprs_2;
	{
		auto filter_expr1 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_GREATERTHANOREQUALTO,	// orderdate >= 1993-07-01
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 5) ),
			move( make_unique<BoundConstantExpression>(Value::DATE(date_t(8582))) )
		);
		auto filter_expr2 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_LESSTHAN,	// orderdate < 1993-10-01
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 5) ),
			move( make_unique<BoundConstantExpression>(Value::DATE(date_t(8674))) )
		);
		filter_exprs_2.push_back(move(filter_expr1));
	}

	// join o->c (_l, l.rf, l.ep, l.d, _o, o.od)
	CypherSchema sch5 = sch4;
	sch5.addNode("c");

	// fetch c (_l, l.rf, l.ep, l.d, _o, o.od, _c)
	CypherSchema sch6 = sch5;
	sch6.addPropertyIntoNode("c", "C_NAME", LogicalType::VARCHAR);
	sch6.addPropertyIntoNode("c", "C_ACCTBAL", LogicalType::DECIMAL(12,2));
	sch6.addPropertyIntoNode("c", "C_ADDRESS", LogicalType::VARCHAR);
	sch6.addPropertyIntoNode("c", "C_PHONE", LogicalType::VARCHAR);
	sch6.addPropertyIntoNode("c", "C_COMMENT", LogicalType::VARCHAR);

	// join c->n (_l, l.rf, l.ep, l.d, _o, o.od, _c, c.name, c.ab, c.addr, c.pho, c.cmt)
	CypherSchema sch7 = sch6;
	sch7.addNode("n");

	// fetch n (_l, l.rf, l.ep, l.d, _o, o.od, _c, c.name, c.ab, c.addr, c.pho, c.cmt, _n)
	CypherSchema sch8 = sch7;
	sch8.addPropertyIntoNode("n", "N_NAME", LogicalType::VARCHAR);
	
	// groupby (_l, l.rf, l.ep, l.d, _o, o.od, _c, c.name, c.ab, c.addr, c.pho, c.cmt, _n, n.name)	// 14 cols
	CypherSchema sch9;
	sch9.addNode("l");
	sch9.addColumn("C_NAME", LogicalType::VARCHAR);
	sch9.addColumn("C_ACCTBAL", LogicalType::DECIMAL(12,2));
	sch9.addColumn("C_ADDRESS", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("N_NAME", LogicalType::VARCHAR);
	sch9.addColumn("revenue", LogicalType::DOUBLE);

	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	// 7 keys (groups)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // _l
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 7) ); // cname
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::LogicalType::DECIMAL(12,2), 8) ); // c.ab
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 9) ); // c.addr
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 10) ); // c.pho
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 11) ); // c.cmt
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 13) ); // n.name
	// 1 agg expression

// FIXME advance expression after checking its running. (later)
	auto agg_expr_func = SumFun::GetSumAggregate(PhysicalType::DOUBLE);	// sum function of DOUBLE
	vector<unique_ptr<Expression>> agg_expr_1_child;
	agg_expr_1_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::DECIMAL(12,2), 2));		// extendedprice
	agg_exprs.push_back(
		make_unique<BoundAggregateExpression>(agg_expr_func, move(agg_expr_1_child), nullptr, nullptr, false )
	); //sum(valid)


// pipes
	std::vector<CypherPhysicalOperator *> ops;
	//src
// FIXME further add predicate to sacn and remove filter
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("lineitem"), PropertyKeys({"L_RETURNFLAG", "L_EXTENDEDPRICE", "L_DISCOUNT"})) );
	ops.push_back( new PhysicalFilter(sch2, move(filter_exprs)) );
	ops.push_back( new PhysicalAdjIdxJoin(sch3, "l", LabelSet("lineitem"), LabelSet("IS_PART_OF"), ExpandDirection::OUTGOING, LabelSet("orders"), JoinType::INNER, false, true) );
	ops.push_back( new PhysicalNodeIdSeek(sch4, "o", LabelSet("orders"), PropertyKeys({"O_ORDERDATE"}) ) );
	ops.push_back( new PhysicalFilter(sch4, move(filter_exprs_2)) );
	ops.push_back( new PhysicalAdjIdxJoin(sch5, "o", LabelSet("orders"), LabelSet("MADE_BY"), ExpandDirection::OUTGOING, LabelSet("customer"), JoinType::INNER, false, true ) );
	ops.push_back( new PhysicalNodeIdSeek(sch6, "c", LabelSet("customer"), PropertyKeys({"C_NAME", "C_ACCTBAL", "C_ADDRESS", "C_PHONE", "C_COMMENT"})) );
	ops.push_back( new PhysicalAdjIdxJoin(sch7, "c", LabelSet("customer"), LabelSet("BELONG_TO"), ExpandDirection::OUTGOING, LabelSet("nation"), JoinType::INNER, false, true) );
	ops.push_back( new PhysicalNodeIdSeek(sch8, "n", LabelSet("nation"), PropertyKeys({"N_NAME"}) ) );
	ops.push_back( new PhysicalHashAggregate(sch9, move(agg_exprs), move(agg_groups)));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;


}
CypherPipelineExecutor* q10_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	// orderby (_c, c_name, c_ab, c_addr, c_pho, c.cmt, n.name, revenue:double)
	CypherSchema sch9;
	sch9.addNode("l");
	sch9.addColumn("C_NAME", LogicalType::VARCHAR);
	sch9.addColumn("C_ACCTBAL", LogicalType::DECIMAL(12,2));
	sch9.addColumn("C_ADDRESS", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("N_NAME", LogicalType::VARCHAR);
	sch9.addColumn("revenue", LogicalType::DOUBLE);
	
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::DOUBLE, 7);		// revenue desc
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	// sink
	ops.push_back( new PhysicalTopNSort(sch9, move(orders), (idx_t) 20, (idx_t)0));

// pipes, add child
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}

CypherPipelineExecutor* q10_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	CypherSchema sch9;
	sch9.addNode("l");
	sch9.addColumn("C_NAME", LogicalType::VARCHAR);
	sch9.addColumn("C_ACCTBAL", LogicalType::DECIMAL(12,2));
	sch9.addColumn("C_ADDRESS", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("N_NAME", LogicalType::VARCHAR);
	sch9.addColumn("revenue", LogicalType::DOUBLE);

// pipes, add child

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// sink
	ops.push_back( new PhysicalProduceResults(sch9) );

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}


}