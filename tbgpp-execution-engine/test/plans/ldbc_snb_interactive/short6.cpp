
#include "plans/query_plan_suite.hpp"

namespace duckdb {

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test12() {
// 	Schema schema;
// 	vector<LogicalType> tmp_schema {LogicalType::ID, LogicalType::UBIGINT};
// 	schema.setStoredTypes(move(tmp_schema));
// 	// schema.addNode("m");
// 	// schema.addPropertyIntoNode("m", "ID", LogicalType::UBIGINT);
// 	vector<idx_t> oids = {305};
// 	vector<vector<uint64_t>> projection_mapping;
// 	projection_mapping.push_back({std::numeric_limits<uint64_t>::max(), 1});
// 	int64_t filterKeyIndex = 1;
// 	duckdb::Value filter_val = duckdb::Value::UBIGINT(94);

// 	// varlen expand (comment -> comment)
// 	Schema schema2;
// 	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::ID};
// 	schema2.setStoredTypes(move(tmp_schema2));
// 	// schema2.addNode("c");
// 	vector<uint32_t> inner_col_map;
// 	vector<uint32_t> outer_col_map;

// 	inner_col_map.push_back(2);
// 	inner_col_map.push_back(2);

// 	outer_col_map.push_back(0);
// 	outer_col_map.push_back(1);

// 	// pipe 1
// 	std::vector<CypherPhysicalOperator *> ops;
// 	// source
// 	ops.push_back(new PhysicalNodeScan(schema, move(oids), move(projection_mapping), filterKeyIndex, filter_val));
// 	// operators
// 	ops.push_back(new PhysicalVarlenAdjIdxJoin(schema2, 525, JoinType::INNER, 0, false, 0, 8, outer_col_map, inner_col_map));
// 	// ops.push_back(new PhysicalAdjIdxJoin(schema2, "p", LabelSet("Person"), LabelSet("LIKES"), ExpandDirection::OUTGOING, LabelSet("Comment"), JoinType::INNER, false, true));
// 	// sink
// 	ops.push_back(new PhysicalProduceResults(schema2));

// 	auto pipe1 = new CypherPipeline(ops);
// 	auto ctx1 = new ExecutionContext(&context);
// 	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
// 	// wrap pipeline into vector
// 	std::vector<CypherPipelineExecutor*> result;
// 	result.push_back(pipeexec1);
// 	return result;
// }

// LDBC IS6
std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS6() {
	Schema schema;
	vector<LogicalType> tmp_schema {LogicalType::ID, LogicalType::UBIGINT};
	schema.setStoredTypes(move(tmp_schema));
	// schema.addNode("m");
	// schema.addPropertyIntoNode("m", "ID", LogicalType::UBIGINT);
	vector<idx_t> oids1 = {333};
	vector<vector<uint64_t>> projection_mapping1;
	projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1});
	int64_t filterKeyIndex = 1;
	duckdb::Value filter_val = duckdb::Value::UBIGINT(1099511628400);
	// duckdb::Value filter_val = duckdb::Value::UBIGINT(549757114029);

	// varlen expand (comment -> comment)
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema2.setStoredTypes(move(tmp_schema2));

	vector<uint32_t> inner_col_map2 = {2, 2};
	vector<uint32_t> outer_col_map2 = {0, 1};

	// expand (comment -> post)
	Schema schema3;
	vector<LogicalType> tmp_schema3 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema3.setStoredTypes(move(tmp_schema3));

	vector<uint32_t> inner_col_map3 = {3, 3};
	vector<uint32_t> outer_col_map3 = {0, 1, 2};

	// expand (post <- forum)
	Schema schema4;
	vector<LogicalType> tmp_schema4 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema4.setStoredTypes(move(tmp_schema4));

	vector<uint32_t> inner_col_map4 = {4, 4};
	vector<uint32_t> outer_col_map4 = {0, 1, 2, 3};

	// expand (forum <- person)
	Schema schema5;
	vector<LogicalType> tmp_schema5 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	schema5.setStoredTypes(move(tmp_schema5));

	vector<uint32_t> inner_col_map5 = {5, 5};
	vector<uint32_t> outer_col_map5 = {0, 1, 2, 3, 4};

	// fetch forum
	Schema schema6;
	vector<LogicalType> tmp_schema6 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::ID, LogicalType::UBIGINT, LogicalType::VARCHAR};
	schema6.setStoredTypes(move(tmp_schema6));
	vector<idx_t> oids6 = {391};
	vector<vector<uint64_t>> projection_mapping6;
	projection_mapping6.push_back({std::numeric_limits<uint64_t>::max(), 1, 2});
	vector<uint32_t> inner_col_map6 = {6, 7, 8};
	vector<uint32_t> outer_col_map6 = {0, 1, 2, 3, 4, 5};

	// fetch person
	Schema schema7;
	vector<LogicalType> tmp_schema7 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::ID, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::ID, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema7.setStoredTypes(move(tmp_schema7));
	vector<idx_t> oids7 = {305};
	vector<vector<uint64_t>> projection_mapping7;
	projection_mapping7.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3});
	vector<uint32_t> inner_col_map7 = {9, 10, 11, 12};
	vector<uint32_t> outer_col_map7 = {0, 1, 2, 3, 4, 5, 6, 7, 8};

	// projection
	Schema schema8;
	vector<LogicalType> tmp_schema8 {LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema8.setStoredTypes(move(tmp_schema8));
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 7) ) );	// f.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 8) ) );	// f.title
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 10) ) );	// mod.id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 11) ) );	// mod.firstName
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 12) ) );	// mod.lastName
	}

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	// ops.push_back(new PhysicalNodeScan(schema, move(oids), move(projection_mapping)));
	ops.push_back(new PhysicalNodeScan(schema, move(oids1), move(projection_mapping1), filterKeyIndex, filter_val));
	// expand operators
	ops.push_back(new PhysicalVarlenAdjIdxJoin(schema2, 561, JoinType::INNER, 0, false, 0, 8, outer_col_map2, inner_col_map2));
	ops.push_back(new PhysicalAdjIdxJoin(schema3, 543, JoinType::INNER, 2, false, outer_col_map3, inner_col_map3));
	ops.push_back(new PhysicalAdjIdxJoin(schema4, 643, JoinType::INNER, 3, false, outer_col_map4, inner_col_map4));
	ops.push_back(new PhysicalAdjIdxJoin(schema5, 595, JoinType::INNER, 4, false, outer_col_map5, inner_col_map5));
	// fetch operators
	ops.push_back(new PhysicalIdSeek(schema6, 4, oids6, projection_mapping6, outer_col_map6, inner_col_map6));
	ops.push_back(new PhysicalIdSeek(schema7, 5, oids7, projection_mapping7, outer_col_map7, inner_col_map7));
	// projection
	ops.push_back(new PhysicalProjection(schema8, move(proj_exprs)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema8));
	// ops.push_back(new PhysicalProduceResults(schema));

	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

}