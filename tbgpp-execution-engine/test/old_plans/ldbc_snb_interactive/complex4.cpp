#include "plans/query_plan_suite.hpp"
#include "function/aggregate/distributive_functions.hpp"

#include "icecream.hpp"

namespace duckdb {

CypherPipelineExecutor* ic4_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* ic4_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
CypherPipelineExecutor* ic4_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
CypherPipelineExecutor* ic4_pipe4(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC4() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = ic4_pipe1(*this);
	//IC();
	auto p2 = ic4_pipe2(*this, p1);
	//IC();
	auto p3 = ic4_pipe3(*this, p2);
	//IC();
	auto p4 = ic4_pipe4(*this, p3);
	//IC();
	result.push_back(p1);
	result.push_back(p2);
	result.push_back(p3);
	result.push_back(p4);
	return result;
}

CypherPipelineExecutor* ic4_pipe1(QueryPlanSuite& suite) {
//IC();
	// person
	CypherSchema sch1; sch1.addNode("person");
	duckdb::Value filter_val; // person key
	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(21990232559429); }	//demo samsung
	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(14); }
	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(14); }
	// p-K->friend
	CypherSchema sch2 = sch1; sch2.addNode("friend");
	//friend<-HC-post:Post
	CypherSchema sch3 = sch2; sch3.addNode("post");
	// post-HT->tag
	CypherSchema sch4 = sch3; sch4.addNode("tag");

	// project post tag (_p _f _p _t)
	CypherSchema sch5;
	sch5.addNode("post");
	sch5.addNode("tag");
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::ID, 2) ) );	// post
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::ID, 3) ) );	// tag
	}
//IC();
	// aggregate - distinct (in: _p _t)
	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // group by (tag, post)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 1) ); // group by (tag, post)
//IC();
// pipe
	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
	// ops
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "person", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch3, "friend", LabelSet("Person"), LabelSet("POST_HAS_CREATOR"), ExpandDirection::INCOMING, LabelSet("Post"), JoinType::INNER, false, true));
	ops.push_back( new PhysicalAdjIdxJoin(sch4, "post", LabelSet("Post"), LabelSet("HAS_TAG"), ExpandDirection::OUTGOING, LabelSet("Tag"), JoinType::INNER, false, true));
//IC();
	ops.push_back( new PhysicalProjection(sch5, move(proj_exprs)));
	// sink
//IC();
	ops.push_back( new PhysicalHashAggregate(sch5, move(agg_exprs), move(agg_groups)));
//IC();

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
//IC();
}
CypherPipelineExecutor* ic4_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {
	// attach post.creationdate ( in: _p _t )
	CypherSchema sch1;
	sch1.addNode("post");
	sch1.addNode("tag");
	sch1.addPropertyIntoNode("post", "creationDate", LogicalType::BIGINT);
	PropertyKeys post_keys;
	post_keys.push_back("creationDate");

	// projection (_p p.cd _t)
	CypherSchema sch2;
	sch2.addNode("tag");
	sch2.addColumn("valid", LogicalType::BIGINT);
	sch2.addColumn("invalid", LogicalType::BIGINT);
	vector<unique_ptr<Expression>> expr_pj1;
	{	// tag
		auto e0 = make_unique<BoundReferenceExpression>(LogicalType::ID, 2);	// tag._id
		expr_pj1.push_back(std::move(e0));
	}
	{
		// case when BIGINT > post.creationDate >= BIGINT then 1 else 0 AS valid
		auto e_when_ll = make_unique<BoundConstantExpression>(Value::BIGINT(1335830400037));
		auto e_when_lr = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1);	// post.cd
		auto e_when_l = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(e_when_ll), move(e_when_lr));
		auto e_when_r = make_unique<BoundConstantExpression>(Value::BIGINT(1335830400000));
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(e_when_l), move(e_when_r));
		auto e_then = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		auto e_else = make_unique<BoundConstantExpression>(Value::BIGINT(0));
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		expr_pj1.push_back(std::move(e));
	}
	{ 
		// case when BIGINT > post.creationDate then 1 else 0 AS invalid
		auto e_when_l = make_unique<BoundConstantExpression>(Value::BIGINT(1335830400000));
		auto e_when_r = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1); // post.cd
		auto e_when = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(e_when_l), move(e_when_r));
		auto e_then = make_unique<BoundConstantExpression>(Value::BIGINT(1));
		auto e_else = make_unique<BoundConstantExpression>(Value::BIGINT(0));
		auto e = make_unique<BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
		expr_pj1.push_back(std::move(e));
	}

	// aggregation - gp tag, sum(valid),  // in( _t, valid, invalid)
	CypherSchema sch3;
	sch3.addNode("tag");
	sch3.addColumn("postCount", LogicalType::HUGEINT);
	sch3.addColumn("invalidPostCount", LogicalType::HUGEINT);
	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_expr_1_child;
	vector<unique_ptr<Expression>> agg_expr_2_child;
	vector<unique_ptr<Expression>> agg_groups;
	auto agg_expr_1_func = SumFun::GetSumAggregate(PhysicalType::INT64);	// sum function
	agg_expr_1_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1) ); //valid
	agg_expr_2_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 2) ); //invalid
	agg_exprs.push_back( make_unique<BoundAggregateExpression>(agg_expr_1_func, move(agg_expr_1_child), nullptr, nullptr, false  ) ); //sum(valid)
	agg_exprs.push_back( make_unique<BoundAggregateExpression>(agg_expr_1_func, move(agg_expr_2_child), nullptr, nullptr, false ) );	// sum(invalid)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // group by (tag)

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	ops.push_back( new PhysicalNodeIdSeek(sch1, "post", LabelSet("Post"), post_keys));
	ops.push_back( new PhysicalProjection(sch2, move(expr_pj1)));
	// sink
	ops.push_back( new PhysicalHashAggregate(sch3, move(agg_exprs), move(agg_groups)));	// reuse

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}
CypherPipelineExecutor* ic4_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	// filter
	CypherSchema sch1;
	sch1.addNode("tag");
	sch1.addColumn("postCount", LogicalType::HUGEINT);
	sch1.addColumn("invalidPostCount", LogicalType::HUGEINT);
	// filter preds
	vector<unique_ptr<Expression>> predicates;
	{
		// pc > 0
		unique_ptr<Expression> filter_expr1;
		auto lhs = make_unique<BoundReferenceExpression>(LogicalType::HUGEINT, 1);	// pc
		auto rhsval = duckdb::Value::UBIGINT(0);
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(lhs), std::move(rhs));
		predicates.push_back(std::move(filter_expr1));
	}
	{
		// ipc == 0
		unique_ptr<Expression> filter_expr1;
		auto lhs = make_unique<BoundReferenceExpression>(LogicalType::HUGEINT, 2);	// ipc
		auto rhsval = duckdb::Value::UBIGINT(0);
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
		predicates.push_back(std::move(filter_expr1));
	}
	D_ASSERT(predicates.size() == 2);

	// attach tag name
	CypherSchema sch2 = sch1;
	sch2.addPropertyIntoNode("tag", "name", LogicalType::VARCHAR);
	PropertyKeys tag_keys;
	tag_keys.push_back("name");

	// projection (_tag, tag.name, pc, ipc) => (tagname, pc) // 1, 2
	CypherSchema sch3;
	sch3.addColumn("tagName", LogicalType::VARCHAR);
	sch3.addColumn("postCount", LogicalType::HUGEINT);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		auto c1 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// tag.name
		auto c2 = make_unique<BoundReferenceExpression>(LogicalType::HUGEINT, 2);	// postcount
		proj_exprs.push_back(std::move(c1));
		proj_exprs.push_back(std::move(c2));
	}

	// orderby
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::HUGEINT, 1);		// postcount desc
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 0);		// tagname asc
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// op
	ops.push_back( new PhysicalFilter(sch1, move(predicates)));
	ops.push_back( new PhysicalNodeIdSeek(sch2, "tag", LabelSet("Tag"), tag_keys));
	ops.push_back( new PhysicalProjection(sch3, move(proj_exprs)));
	// sink
	ops.push_back( new PhysicalTopNSort(sch3, move(orders), (idx_t) 10, (idx_t)0));

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}

CypherPipelineExecutor* ic4_pipe4(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

	CypherSchema sch3;
	sch3.addColumn("tagName", LogicalType::VARCHAR);
	sch3.addColumn("postCount", LogicalType::HUGEINT);

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// sink
	ops.push_back( new PhysicalProduceResults(sch3) );

	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;

}


}