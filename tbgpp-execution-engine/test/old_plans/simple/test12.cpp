
#include "old_plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test12() {
	CypherSchema schema;
	vector<LogicalType> tmp_schema {LogicalType::ID, LogicalType::UBIGINT};
	schema.setStoredTypes(move(tmp_schema));
	// schema.addNode("m");
	// schema.addPropertyIntoNode("m", "ID", LogicalType::UBIGINT);
	vector<idx_t> oids = {305};
	vector<vector<uint64_t>> projection_mapping;
	projection_mapping.push_back({std::numeric_limits<uint64_t>::max(), 1});

	// varlen expand (comment -> comment)
	CypherSchema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::UBIGINT, LogicalType::ID};
	schema2.setStoredTypes(move(tmp_schema2));
	// schema2.addNode("c");
	vector<uint32_t> inner_col_map;
	vector<uint32_t> outer_col_map;

	inner_col_map.push_back(2);
	inner_col_map.push_back(2);

	outer_col_map.push_back(0);
	outer_col_map.push_back(1);

	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
	// source
	ops.push_back(new PhysicalNodeScan(schema, move(oids), move(projection_mapping)));
	// operators
	ops.push_back(new PhysicalVarlenAdjIdxJoin(schema2, 525, JoinType::INNER, 0, false, 0, 2, outer_col_map, inner_col_map));
	// ops.push_back(new PhysicalAdjIdxJoin(schema2, "p", LabelSet("Person"), LabelSet("LIKES"), ExpandDirection::OUTGOING, LabelSet("Comment"), JoinType::INNER, false, true));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2));

	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
}

}