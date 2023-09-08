#include "plans/query_plan_suite.hpp"

namespace duckdb {

CypherPipelineExecutor *sch1_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST1() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch1_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch1_pipe1(QueryPlanSuite& suite) {
	// scan Metabolite
	// 305, 307 schema
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
									 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	vector<string> tmp_schema1_1_name {"_id", "sub_class", "chebi_id", "pubchem_compound_id",
									 "synonyms", "class", "direct_parent", "super_class",
									 "kingdom", "average_molecular_weight", "name", "description",
									 "id", "checimal_formula"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 309 schema
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_2_name {"_id",
									 "synonyms",
									 "average_molecular_weight", "name", "description",
									 "id", "checimal_formula"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	vector<Schema> schemas = {schema1_1, schema1_2};
	// vector<Schema> schemas = {schema1_1};

	vector<idx_t> oids1 = {307, 309};
	// vector<idx_t> oids1 = {305};
	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	vector<LogicalType> scan_types1;
	projection_mapping1.push_back({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}); // 305, 307
	projection_mapping1.push_back({0, 4, 9, 10, 11, 12, 13});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6});

	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
									 LogicalType::VARCHAR, LogicalType::VARCHAR};	
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_schema1_1_name);

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schemas, schema2, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// ops.push_back(new PhysicalNodeScan(schema1_1, move(oids1), move(projection_mapping1)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

}