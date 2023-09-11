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
									 "kingdom", "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
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
									 "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 327 schema
	Schema schema1_12;
	vector<LogicalType> tmp_schema1_12 {LogicalType::ID, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_12_name {"_id", "sub_class",
									 "synonyms", "class", "direct_parent", "super_class",
									 "kingdom", "average_molecular_weight", "name",
									 "id", "chemical_formula"};
	schema1_12.setStoredTypes(move(tmp_schema1_12));
	schema1_12.setStoredColumnNames(tmp_schema1_12_name);

	// 329 schema
	Schema schema1_13;
	vector<LogicalType> tmp_schema1_13 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_13_name {"_id", "chebi_id", "pubchem_compound_id",
									 "synonyms",
									 "description", "name",
									 "id", "chemical_formula"};
	schema1_13.setStoredTypes(move(tmp_schema1_13));
	schema1_13.setStoredColumnNames(tmp_schema1_13_name);

	vector<Schema> schemas_1_2 = {schema1_1, schema1_2};
	vector<Schema> schemas_12_13 = {schema1_12, schema1_13};
	vector<Schema> schemas_2_13 = {schema1_2, schema1_13};
	// vector<Schema> schemas = {schema1_1};

	Schema union_schema;
	// 1 + 3 / 12 + 13: 0 1 2 3 4 5 6 7 8 9 10 11 12
	// vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	// vector<string> tmp_union_schema_name {"_id", "sub_class", "chebi_id", "pubchem_compound_id",
	// 								 "synonyms", "class", "direct_parent", "super_class",
	// 								 "kingdom", "average_molecular_weight", "description", "name",
	// 								 "id", "chemical_formula"};
	// 3 + 13: 1 2 3 8 9 10 11 12
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	vector<string> tmp_union_schema_name {"_id", "chebi_id", "pubchem_compound_id",
									 "synonyms",
									 "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
	union_schema.setStoredTypes(move(tmp_union_schema));
	union_schema.setStoredColumnNames(tmp_union_schema_name);

	// vector<idx_t> oids1 = {307, 309};
	// vector<idx_t> oids1 = {327, 329};
	vector<idx_t> oids1 = {309, 329};
	// vector<idx_t> oids1 = {305};
	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	vector<LogicalType> scan_types1;
	// 307 + 309
	// projection_mapping1.push_back({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}); // 305, 307
	// projection_mapping1.push_back({0, 4, 9, 10, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6}); // 309
	// 327 + 329
	// projection_mapping1.push_back({0, 1, 4, 5, 6, 7, 8, 9, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); // 327
	// projection_mapping1.push_back({0, 2, 3, 4, 10, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7}); // 329
	// 309 + 329: 1 2 3 8 9 10 11 12
	projection_mapping1.push_back({0, 3, 4, 5, 6, 7, 8});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6}); // 309
	projection_mapping1.push_back({0, 1, 2, 3, 5, 6, 7, 8});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7}); // 329

	Schema schema2;
	// vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_union_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// projection_mapping2.push_back({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	1, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	2, 3, 4, 5, 6});
	// 327 + 329
	// projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8, 9, 
	// 	std::numeric_limits<uint8_t>::max(), 11, 12, 13});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, 4, std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), 10, 11, 12, 13});
	// 309 + 329
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
		3, 4, 5, 6, 7, 8});
	projection_mapping2.push_back({0, 1, 2, 3, std::numeric_limits<uint8_t>::max(), 5, 6, 7, 8});

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schemas_2_13, union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// ops.push_back(new PhysicalNodeScan(schema1_1, move(oids1), move(projection_mapping1)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2, projection_mapping2));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}

CypherPipelineExecutor *sch2_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST2() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch2_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch2_pipe1(QueryPlanSuite& suite) {
	// scan Metabolite
	// 305, 307 schema
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
									 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	vector<string> tmp_schema1_1_name {"_id", "sub_class", "chebi_id", "pubchem_compound_id",
									 "synonyms", "class", "direct_parent", "super_class",
									 "kingdom", "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
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
									 "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 327 schema
	Schema schema1_12;
	vector<LogicalType> tmp_schema1_12 {LogicalType::ID, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_12_name {"_id", "sub_class",
									 "synonyms", "class", "direct_parent", "super_class",
									 "kingdom", "average_molecular_weight", "name",
									 "id", "chemical_formula"};
	schema1_12.setStoredTypes(move(tmp_schema1_12));
	schema1_12.setStoredColumnNames(tmp_schema1_12_name);

	// 329 schema
	Schema schema1_13;
	vector<LogicalType> tmp_schema1_13 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_13_name {"_id", "chebi_id", "pubchem_compound_id",
									 "synonyms",
									 "description", "name",
									 "id", "chemical_formula"};
	schema1_13.setStoredTypes(move(tmp_schema1_13));
	schema1_13.setStoredColumnNames(tmp_schema1_13_name);

	vector<Schema> schemas_1_2 = {schema1_1, schema1_2};
	vector<Schema> schemas_12_13 = {schema1_12, schema1_13};
	vector<Schema> schemas_2_13 = {schema1_2, schema1_13};
	// vector<Schema> schemas = {schema1_1};

	Schema union_schema;
	// 1 + 3 / 12 + 13: 0 1 2 3 4 5 6 7 8 9 10 11 12
	// vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	// vector<string> tmp_union_schema_name {"_id", "sub_class", "chebi_id", "pubchem_compound_id",
	// 								 "synonyms", "class", "direct_parent", "super_class",
	// 								 "kingdom", "average_molecular_weight", "description", "name",
	// 								 "id", "chemical_formula"};
	// 3 + 13: 1 2 3 8 9 10 11 12
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	vector<string> tmp_union_schema_name {"_id", "chebi_id", "pubchem_compound_id",
									 "synonyms",
									 "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
	union_schema.setStoredTypes(move(tmp_union_schema));
	union_schema.setStoredColumnNames(tmp_union_schema_name);

	// vector<idx_t> oids1 = {307, 309};
	// vector<idx_t> oids1 = {327, 329};
	vector<idx_t> oids1 = {309, 329};
	// vector<idx_t> oids1 = {305};
	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	vector<LogicalType> scan_types1;
	// 307 + 309
	// projection_mapping1.push_back({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}); // 305, 307
	// projection_mapping1.push_back({0, 4, 9, 10, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6}); // 309
	// 327 + 329
	// projection_mapping1.push_back({0, 1, 4, 5, 6, 7, 8, 9, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); // 327
	// projection_mapping1.push_back({0, 2, 3, 4, 10, 11, 12, 13});
	// scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7}); // 329
	// 309 + 329: 1 2 3 8 9 10 11 12
	projection_mapping1.push_back({0, 3, 4, 5, 6, 7, 8});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6}); // 309
	projection_mapping1.push_back({0, 1, 2, 3, 5, 6, 7, 8});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7}); // 329

	Schema schema2;
	// vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_union_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// projection_mapping2.push_back({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	1, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	2, 3, 4, 5, 6});
	// 327 + 329
	// projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8, 9, 
	// 	std::numeric_limits<uint8_t>::max(), 11, 12, 13});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, 4, std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), 10, 11, 12, 13});
	// 309 + 329
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
		3, 4, 5, 6, 7, 8});
	projection_mapping2.push_back({0, 1, 2, 3, std::numeric_limits<uint8_t>::max(), 5, 6, 7, 8});

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schemas_2_13, union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// ops.push_back(new PhysicalNodeScan(schema1_1, move(oids1), move(projection_mapping1)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2, projection_mapping2));
	
	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{2}, {2}};
	vector<vector<Schema>> pipelien_schemas = { schemas_2_13, schemas_2_13 };
	vector<Schema> pipeline_union_schema = { union_schema, union_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(2);
	flow_graph[1].resize(2);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

CypherPipelineExecutor *sch3_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST3() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch3_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch3_pipe1(QueryPlanSuite& suite) {
	// scan Metabolite
	// 305, 307 schema
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,			// 0, 1, 2
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 3, 4, 5, 6
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,	// 7, 8, 9, 10
									 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	vector<string> tmp_schema1_1_name {"_id", "sub_class", "chebi_id", "pubchem_compound_id",
									 "synonyms", "class", "direct_parent", "super_class",
									 "kingdom", "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
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
									 "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 327 schema
	Schema schema1_12;
	vector<LogicalType> tmp_schema1_12 {LogicalType::ID, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_12_name {"_id", "sub_class",
									 "synonyms", "class", "direct_parent", "super_class",
									 "kingdom", "average_molecular_weight", "name",
									 "id", "chemical_formula"};
	schema1_12.setStoredTypes(move(tmp_schema1_12));
	schema1_12.setStoredColumnNames(tmp_schema1_12_name);

	// 329 schema
	Schema schema1_13;
	vector<LogicalType> tmp_schema1_13 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	vector<string> tmp_schema1_13_name {"_id", "chebi_id", "pubchem_compound_id",
									 "synonyms",
									 "description", "name",
									 "id", "chemical_formula"};
	schema1_13.setStoredTypes(move(tmp_schema1_13));
	schema1_13.setStoredColumnNames(tmp_schema1_13_name);

	vector<Schema> schemas_1_2 = {schema1_1, schema1_2};
	vector<Schema> schemas_12_13 = {schema1_12, schema1_13};
	vector<Schema> schemas_2_13 = {schema1_2, schema1_13};
	// vector<Schema> schemas = {schema1_1};

	Schema union_schema;
	// 3 + 13: 1 2 3 8 9 10 11 12
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};												// 11, 12
	vector<string> tmp_union_schema_name {"_id", "chebi_id", "pubchem_compound_id",
									 "synonyms",
									 "average_molecular_weight", "description", "name",
									 "id", "chemical_formula"};
	union_schema.setStoredTypes(move(tmp_union_schema));
	union_schema.setStoredColumnNames(tmp_union_schema_name);

	// vector<idx_t> oids1 = {307, 309};
	// vector<idx_t> oids1 = {327, 329};
	vector<idx_t> oids1 = {309, 329};
	// vector<idx_t> oids1 = {305};
	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	vector<LogicalType> scan_types1;
	// 309 + 329: 1 2 3 8 9 10 11 12
	projection_mapping1.push_back({0, 3, 4, 5, 6, 7, 8});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6}); // 309
	projection_mapping1.push_back({0, 1, 2, 3, 5, 6, 7, 8});
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7}); // 329

	// filter preds
	vector<unique_ptr<Expression>> predicates;
	unique_ptr<Expression> filter_expr1;
	{
		auto lhs = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 8);
		auto rhsval = duckdb::Value("C13H18N2O2");
		// auto lhs = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 0);
		// auto rhsval = duckdb::Value::UBIGINT(8589934592);
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
	}
	predicates.push_back(std::move(filter_expr1));

	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_union_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// 309 + 329
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
		3, 4, 5, 6, 7, 8});
	projection_mapping2.push_back({0, 1, 2, 3, std::numeric_limits<uint8_t>::max(), 5, 6, 7, 8});

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schemas_2_13, union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	ops.push_back(new PhysicalFilter(schema2, std::move(predicates)));
	// sink
	ops.push_back(new PhysicalProduceResults(schema2, projection_mapping2));
	
	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{2}, {2}};
	vector<vector<Schema>> pipelien_schemas = { schemas_2_13, schemas_2_13 };
	vector<Schema> pipeline_union_schema = { union_schema, union_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(2);
	flow_graph[1].resize(2);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

}