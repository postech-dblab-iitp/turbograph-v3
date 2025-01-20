#include "plans/query_plan_suite.hpp"

namespace s62 {

CypherPipelineExecutor* is2_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor* is2_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS2() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = is2_pipe1(*this);
	auto p2 = is2_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* is2_pipe1(QueryPlanSuite& suite) {

	// p._id, p.id
	// scan person
	Schema schema1;
	vector<LogicalType> tmp_schema1 {LogicalType::ID, LogicalType::UBIGINT};
	schema1.setStoredTypes(move(tmp_schema1));

	vector<idx_t> oids1 = {305};
	vector<vector<uint64_t>> projection_mapping1;
	projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1});

	int64_t filterKeyIndex = 1;
	s62::Value filter_val = s62::Value::UBIGINT(2783);

// FIXME
	// if(suite.LDBC_SF==1) { filter_val = s62::Value::UBIGINT(35184372099695); }
	// if(suite.LDBC_SF==10) { filter_val = s62::Value::UBIGINT(14); }
	// if(suite.LDBC_SF==100) { filter_val = s62::Value::UBIGINT(14); }

// expand (person <- message)
	// p._id, p.id, m._id
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema2.setStoredTypes(move(tmp_schema2));

	vector<uint32_t> inner_col_map2 = {2, 2};
	vector<uint32_t> outer_col_map2 = {0, 1};

// fetch message properties
	// p._id, p.id, m._id, m._id, m.id, m.creationDate, m.content
	Schema schema3;
	vector<LogicalType> tmp_schema3 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR};
	schema3.setStoredTypes(move(tmp_schema3));
	vector<idx_t> oids3 = {333};
	vector<vector<uint64_t>> projection_mapping3;
	projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 5});
	vector<uint32_t> inner_col_map3 = {3, 4, 5, 6};
	vector<uint32_t> outer_col_map3 = {0, 1, 2};

	// projection
	// p._id, p.id, m._id, m._id, m.id, m.creationDate, m.content
	// -> m._id, m.id, m.creationDate, m.content
	Schema schema4;
	vector<LogicalType> tmp_schema4 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR};
	schema4.setStoredTypes(move(tmp_schema4));
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::ID, 3) ) );	// m._id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 4) ) );	// m.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 5) ) );	// m.creationDate
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6) ) );	// m.content
	}

// topn (in : _mid, m.id, m.date)
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1);
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schema1, move(oids1), move(projection_mapping1), filterKeyIndex, filter_val));
	// // ops
	ops.push_back(new PhysicalAdjIdxJoin(schema2, 621, JoinType::INNER, 0, false, outer_col_map2, inner_col_map2));
	ops.push_back(new PhysicalIdSeek(schema3, 2, oids3, projection_mapping3, outer_col_map3, inner_col_map3));
	ops.push_back(new PhysicalProjection(schema4, move(proj_exprs)));
	// sink
	ops.push_back(new PhysicalTopNSort(schema4, move(orders), (idx_t)10, (idx_t)0));
	// ops.push_back(new PhysicalProduceResults(schema3));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

CypherPipelineExecutor* is2_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// get source

// varlenexpand (comment - [:REPLY_OF*0..] -> comment)
	Schema schema1;
	vector<LogicalType> tmp_schema1 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT};
	schema1.setStoredTypes(move(tmp_schema1));

	vector<uint32_t> inner_col_map1 = {4, 4};
	vector<uint32_t> outer_col_map1 = {0, 1, 2, 3};

// expand (comment -> post)
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema2.setStoredTypes(move(tmp_schema2));

	vector<uint32_t> inner_col_map2 = {5, 5};
	vector<uint32_t> outer_col_map2 = {0, 1, 2, 3, 4};

// expand (post -> person)
	Schema schema3;
	vector<LogicalType> tmp_schema3 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema3.setStoredTypes(move(tmp_schema3));

	vector<uint32_t> inner_col_map3 = {6, 6};
	vector<uint32_t> outer_col_map3 = {0, 1, 2, 3, 4, 5};

// fetch post
	Schema schema4;
	vector<LogicalType> tmp_schema4 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::ID, LogicalType::UBIGINT};
	schema4.setStoredTypes(move(tmp_schema4));
	vector<idx_t> oids4 = {367};
	vector<vector<uint64_t>> projection_mapping4;
	projection_mapping4.push_back({std::numeric_limits<uint64_t>::max(), 1});
	vector<uint32_t> inner_col_map4 = {7, 8};
	vector<uint32_t> outer_col_map4 = {0, 1, 2, 3, 4, 5, 6};

// fetch person
	Schema schema5;
	vector<LogicalType> tmp_schema5 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::ID, LogicalType::UBIGINT, LogicalType::ID, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema5.setStoredTypes(move(tmp_schema5));
	vector<idx_t> oids5 = {305};
	vector<vector<uint64_t>> projection_mapping5;
	projection_mapping5.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3});
	vector<uint32_t> inner_col_map5 = {9, 10, 11, 12};
	vector<uint32_t> outer_col_map5 = {0, 1, 2, 3, 4, 5, 6, 7, 8};
	
// project (m._id, m.id, m.creationDate, m.content, c._id, post._id, p._id, post._id, post.id, p._id, p.id, p.firstName, p.lastName)
//      -> (m.id, m.content, m.creationDate, post.id, p.id, p.firstName, p.lastName)
	Schema schema6;
	vector<LogicalType> tmp_schema6 {LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema6.setStoredTypes(tmp_schema6);

	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1)) );	// mi
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3)) );	// content
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2)) );	// cdate
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 8)) );	// postid
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 10)) );	// personid
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 11)) );	// fn
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 12)) );	// ln
	}

// Schema schema4;
// 	vector<LogicalType> tmp_schema4 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR};
// 	schema4.setStoredTypes(move(tmp_schema4));

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back( prev_pipe->pipeline->GetSink() );
	// expand ops
	ops.push_back(new PhysicalVarlenAdjIdxJoin(schema1, 561, JoinType::INNER, 0, false, 0, 8, outer_col_map1, inner_col_map1));
	ops.push_back(new PhysicalAdjIdxJoin(schema2, 543, JoinType::INNER, 4, false, outer_col_map2, inner_col_map2));
	ops.push_back(new PhysicalAdjIdxJoin(schema3, 493, JoinType::INNER, 5, false, outer_col_map3, inner_col_map3));
	// fetch ops
	ops.push_back(new PhysicalIdSeek(schema4, 5, oids4, projection_mapping4, outer_col_map4, inner_col_map4));
	ops.push_back(new PhysicalIdSeek(schema5, 6, oids5, projection_mapping5, outer_col_map5, inner_col_map5));
	// projection
	ops.push_back( new PhysicalProjection(schema6, move(proj_exprs)));
// 	// sink
	ops.push_back( new PhysicalProduceResults(schema6));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
	return pipeexec;
}

}