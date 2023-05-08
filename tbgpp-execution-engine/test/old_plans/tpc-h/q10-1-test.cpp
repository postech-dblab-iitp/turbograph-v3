#include "plans/query_plan_suite.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"

#include "function/scalar/operators.hpp"

#include "icecream.hpp"

namespace duckdb {

CypherPipelineExecutor* q10_1_test_pipe1(QueryPlanSuite& suite);


std::vector<CypherPipelineExecutor*> QueryPlanSuite::TPCH_Q10_1_TEST() {
icecream::ic.disable();
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = q10_1_test_pipe1(*this);
	result.push_back(p1);
icecream::ic.disable();
	return result;

}

CypherPipelineExecutor* q10_1_test_pipe1(QueryPlanSuite& suite) {

	// scan ORDERS;
	Schema sch1;
	sch1.addNode("o");
	sch1.addPropertyIntoNode("o", "O_ORDERDATE", LogicalType::DATE);
	PropertyKeys o_keys({"O_ORDERDATE"});

	// filter date range (_o, o.od)
	vector<unique_ptr<Expression>> filter_exprs_2;
	{
// FIXME change predicate when SF changes
		auto filter_expr1 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_GREATERTHANOREQUALTO,	// orderdate >= 1993-07-01
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 1) ),
			move( make_unique<BoundConstantExpression>(Value::DATE(date_t(8582))) )
		);
		auto filter_expr2 = make_unique<BoundComparisonExpression>( ExpressionType::COMPARE_LESSTHAN,	// orderdate < 1993-10-01
			move( make_unique<BoundReferenceExpression>(LogicalType::DATE, 1) ),
			move( make_unique<BoundConstantExpression>(Value::DATE(date_t(8674))) )
		);
		filter_exprs_2.push_back(move(filter_expr1));
		filter_exprs_2.push_back(move(filter_expr2));
	}

	// join o->l (_o, o.od, _l)
	Schema sch2 = sch1;
	sch2.addNode("l");

	// fetch l (_o, o.od, _l, l.rf, l.ep, l.d)
	Schema sch3 = sch2;
	sch3.addPropertyIntoNode("l", "L_RETURNFLAG", LogicalType::VARCHAR);
	sch3.addPropertyIntoNode("l", "L_EXTENDEDPRICE", LogicalType::DECIMAL(12, 2));
	sch3.addPropertyIntoNode("l", "L_DISCOUNT", LogicalType::DECIMAL(12, 2));
	duckdb::Value filter_val = duckdb::Value((string_t)"R");

// FIXME further pushdown (later)
	// filter_expr (_o, o.od, _l, l.rf, l.ep, l.d)
	Schema sch4 = sch3;
	vector<unique_ptr<Expression>> filter_exprs;
	{
		unique_ptr<Expression> filter_expr1;
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, 
							make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3),
							make_unique<BoundConstantExpression>(Value( (string_t) "R" ))
						);
		filter_exprs.push_back(move(filter_expr1));
	}
	
	// join o->c (_l, l.rf, l.ep, l.d, _o, o.od)
	Schema sch5 = sch4;
	sch5.addNode("c");

	// fetch c (_o, o.od, _l, l.rf, l.ep, l.d, _c)
	Schema sch6 = sch5;

	// join c->n (_o, o.od, _l, l.rf, l.ep, l.d, _c, c.name, c.ab, c.addr, c.pho, c.cmt)
	Schema sch7 = sch6;
	sch7.addNode("n");

	// fetch n (_o, o.od, _l, l.rf, l.ep, l.d, _c, c.name, c.ab, c.addr, c.pho, c.cmt, _n)
	Schema sch8 = sch7;
	sch8.addPropertyIntoNode("n", "N_NAME", LogicalType::VARCHAR);

	// projection (_o, o.od, _l, l.rf, l.ep, l.d, _c, c.name, c.ab, c.addr, c.pho, c.cmt, _n, n.name)
	Schema sch9;
	sch9.addNode("c");
	sch9.addColumn("C_NAME", LogicalType::VARCHAR);
	sch9.addColumn("C_ACCTBAL", LogicalType::DECIMAL(12,2));
	sch9.addColumn("C_ADDRESS", LogicalType::VARCHAR);
	sch9.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch9.addColumn("C_COMMENT", LogicalType::VARCHAR);
	sch9.addColumn("N_NAME", LogicalType::VARCHAR);
	sch9.addColumn("revenue", LogicalType::DOUBLE);	// agg
IC();
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 6) ); // _c
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 7) ); // cname
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::DECIMAL(12,2), 8) ); // c.ab
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 9) ); // c.addr
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 10) ); // c.pho
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 11) ); // c.cmt
		proj_exprs.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 13) ); // n.name

	// expression
	// sub
IC();
		auto sub_f = SubtractFun::GetFunction( LogicalType::DOUBLE,  LogicalType::DOUBLE );
		// auto sub_f_int = SubtractFun::GetFunction( LogicalType::INTEGER,  LogicalType::INTEGER );
		auto sub_lhs = make_unique<BoundConstantExpression>( Value::DOUBLE(1) );			// 1 ; push 100 to set 1
		auto sub_rhs = make_unique<BoundCastExpression>(make_unique<BoundReferenceExpression>( LogicalType::DECIMAL(12,2), 5), LogicalType::DOUBLE, false) ;	// discount
		vector<unique_ptr<Expression>> sub_args;												
		sub_args.push_back(move(sub_lhs));
		sub_args.push_back(move(sub_rhs));
		auto sub_expr = make_unique<BoundFunctionExpression>( LogicalType::DOUBLE, sub_f, move(sub_args), nullptr, false);		// 1 - discount
IC();
	// mul
		auto mul_f = MultiplyFun::GetFunction( LogicalType::DOUBLE );
		auto mul_lhs = make_unique<BoundCastExpression>(make_unique<BoundReferenceExpression>( LogicalType::DECIMAL(12,2), 4), LogicalType::DOUBLE, false) ;	// extprice
		auto mul_rhs = move(sub_expr);
		vector<unique_ptr<Expression>> mul_args;
		mul_args.push_back(move(mul_lhs));
		mul_args.push_back(move(mul_rhs));
		auto mul_expr = make_unique<BoundFunctionExpression>(LogicalType::DOUBLE, mul_f, move(mul_args), nullptr, false);
	// pushback
		proj_exprs.push_back( move(mul_expr) );
	}
IC();
	// groupby ( 8 cols including revenue )
	Schema sch10;
	sch10.addNode("c");
	sch10.addColumn("C_NAME", LogicalType::VARCHAR);
	sch10.addColumn("C_ACCTBAL", LogicalType::DECIMAL(12,2));
	sch10.addColumn("C_ADDRESS", LogicalType::VARCHAR);
	sch10.addColumn("C_PHONE", LogicalType::VARCHAR);
	sch10.addColumn("C_COMMENT", LogicalType::VARCHAR);
	sch10.addColumn("N_NAME", LogicalType::VARCHAR);
	sch10.addColumn("revenue", LogicalType::DOUBLE);	// agg

	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	// 7 keys (groups)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // _c
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1) ); // cname
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::DECIMAL(12,2), 2) ); // c.ab
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3) ); // c.addr
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4) ); // c.pho
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5) ); // c.cmt
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6) ); // n.name
	// 1 agg expression
	vector<unique_ptr<Expression>> agg_expr_1_child;
IC();
	agg_expr_1_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::DOUBLE, 7) );		// 1-blabla * blabla

	auto agg_expr_func = SumFun::GetSumAggregate(PhysicalType::DOUBLE);	// sum function of DOUBLE
	agg_exprs.push_back(
		make_unique<BoundAggregateExpression>(agg_expr_func, move(agg_expr_1_child), nullptr, nullptr, false )
	); 
IC();

// pipes
	std::vector<CypherPhysicalOperator *> ops;
	//src
// FIXME further add predicate to sacn and remove filter
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("ORDERS"), PropertyKeys({"O_ORDERDATE"}) ) );
	ops.push_back( new PhysicalFilter(sch1, move(filter_exprs_2)) );
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "o", LabelSet("ORDERS"), LabelSet("IS_PART_OF_BACKWARD"), ExpandDirection::OUTGOING, LabelSet("LINEITEM"), JoinType::INNER, false, true) );
	ops.push_back( new PhysicalNodeIdSeek(sch3, "l", LabelSet("LINEITEM"), PropertyKeys({"L_RETURNFLAG", "L_EXTENDEDPRICE", "L_DISCOUNT"})) );
	ops.push_back( new PhysicalFilter(sch4, move(filter_exprs)) );
	ops.push_back( new PhysicalAdjIdxJoin(sch5, "o", LabelSet("ORDERS"), LabelSet("MADE_BY"), ExpandDirection::OUTGOING, LabelSet("CUSTOMER"), JoinType::INNER, false, true ) );
	// ops.push_back( new PhysicalNodeIdSeek(sch6, "c", LabelSet("CUSTOMER"), PropertyKeys({"C_NAME", "C_ACCTBAL", "C_ADDRESS", "C_PHONE", "C_COMMENT"})) );
	ops.push_back( new PhysicalAdjIdxJoin(sch7, "c", LabelSet("CUSTOMER"), LabelSet("BELONG_TO"), ExpandDirection::OUTGOING, LabelSet("NATION"), JoinType::INNER, false, true) );
	ops.push_back( new PhysicalNodeIdSeek(sch8, "n", LabelSet("NATION"), PropertyKeys({"N_NAME"}) ) );

	ops.push_back( new PhysicalProduceResults(sch8));


	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

}