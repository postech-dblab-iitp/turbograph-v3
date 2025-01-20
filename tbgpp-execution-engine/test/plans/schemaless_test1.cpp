#include "plans/query_plan_suite.hpp"

namespace s62 {

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
		auto rhsval = s62::Value("C13H18N2O2");
		// auto lhs = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 0);
		// auto rhsval = s62::Value::UBIGINT(8589934592);
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

CypherPipelineExecutor *sch4_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST4() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch4_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch4_pipe1(QueryPlanSuite& suite) {
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
		auto rhsval = s62::Value("C13H18N2O2");
		// auto lhs = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 0);
		// auto rhsval = s62::Value::UBIGINT(8589934592);
		auto rhs = make_unique<BoundConstantExpression>(rhsval);
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lhs), std::move(rhs));
	}
	predicates.push_back(std::move(filter_expr1));

	vector<unique_ptr<Expression>> proj_exprs;
	{
		auto c1 = make_unique<BoundReferenceExpression>(LogicalType::ID, 0);	// _id
		auto c2 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// chebi_id
		auto c3 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 7);	// id
		auto c4 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 8);	// chemical_formula
		proj_exprs.push_back(std::move(c1));
		proj_exprs.push_back(std::move(c2));
		proj_exprs.push_back(std::move(c3));
		proj_exprs.push_back(std::move(c4));
	}

	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR};
	vector<string> tmp_schema_name2 {"_id", "chebi_id", "id", "chemical_formula"};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_schema_name2);
	vector<vector<uint8_t>> projection_mapping2;
	// 309 + 329
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	3, 4, 5, 6, 7, 8});
	// projection_mapping2.push_back({0, 1, 2, 3, std::numeric_limits<uint8_t>::max(), 5, 6, 7, 8});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3});
	projection_mapping2.push_back({0, 1, 2, 3});

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(schemas_2_13, union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	// ops.push_back(new PhysicalFilter(schema2, std::move(predicates)));
	ops.push_back(new PhysicalProjection(schema2, std::move(proj_exprs)));
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

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch5_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST5() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch5_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch5_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_union_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
		std::numeric_limits<uint8_t>::max(), 6, 7});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
		4, 5, 6, 7});
	projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 6, 7});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7});

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	// ops.push_back(new PhysicalFilter(schema2, std::move(predicates)));
	// ops.push_back(new PhysicalProjection(schema2, std::move(proj_exprs)));
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));
	
	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, post_schemas };
	vector<Schema> pipeline_union_schema = { post_union_schema, post_union_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	flow_graph[1][2] = 2;
	flow_graph[1][3] = 3;
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch6_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST6() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch6_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch6_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	// vector<Schema> post_schemas = {schema1_1, schema1_3, schema1_4};
	// vector<idx_t> oids1 = {443, 447, 449};
	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	// expand (post -[has_creator]-> person)
	Schema schema2_1;
	vector<LogicalType> tmp_schema2_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id"};
	schema2_1.setStoredTypes(move(tmp_schema2_1));
	schema2_1.setStoredColumnNames(tmp_schema2_1_name);
	Schema schema2_2;
	vector<LogicalType> tmp_schema2_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_2.setStoredTypes(move(tmp_schema2_2));
	schema2_2.setStoredColumnNames(tmp_schema2_2_name);
	Schema schema2_3;
	vector<LogicalType> tmp_schema2_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id"};
	schema2_3.setStoredTypes(move(tmp_schema2_3));
	schema2_3.setStoredColumnNames(tmp_schema2_3_name);
	Schema schema2_4;
	vector<LogicalType> tmp_schema2_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_4.setStoredTypes(move(tmp_schema2_4));
	schema2_4.setStoredColumnNames(tmp_schema2_4_name);
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_name {"post._id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_schema2_name);
	vector<uint32_t> inner_col_map2 = {8};
	vector<uint32_t> outer_col_map2 = {0, 1, 2, 3, 4, 5, 6, 7};
	vector<vector<uint32_t>> outer_col_maps2 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7}};
	vector<Schema> schema2s = {schema2_1, schema2_2, schema2_3, schema2_4};

	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_schema2_name);
	vector<vector<uint8_t>> projection_mapping2;
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
		std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
		4, 5, 6, 7, 8});
	projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8});

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	// ops.push_back(new PhysicalFilter(schema2, std::move(predicates)));
	// ops.push_back(new PhysicalProjection(schema2, std::move(proj_exprs)));
	ops.push_back(new PhysicalAdjIdxJoin(schema2, 523, JoinType::INNER, 0, false, outer_col_maps2, inner_col_map2)); // post_has_creator
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));
	
	// schema flow graph
	size_t pipeline_length = 3;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::BINARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4, 1}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, schema2s, schema2s };
	vector<Schema> pipeline_union_schema = { post_union_schema, final_schema, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[2].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	flow_graph[1][2] = 2;
	flow_graph[1][3] = 3;
	flow_graph[2][0] = 0;
	flow_graph[2][1] = 1;
	flow_graph[2][2] = 2;
	flow_graph[2][3] = 3;
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch7_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST7() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch7_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch7_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	// vector<Schema> post_schemas = {schema1_1, schema1_3, schema1_4};
	// vector<idx_t> oids1 = {443, 447, 449};
	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	// expand (post -[has_creator]-> person)
	Schema schema2_1;
	vector<LogicalType> tmp_schema2_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id"};
	schema2_1.setStoredTypes(move(tmp_schema2_1));
	schema2_1.setStoredColumnNames(tmp_schema2_1_name);
	Schema schema2_2;
	vector<LogicalType> tmp_schema2_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_2.setStoredTypes(move(tmp_schema2_2));
	schema2_2.setStoredColumnNames(tmp_schema2_2_name);
	Schema schema2_3;
	vector<LogicalType> tmp_schema2_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id"};
	schema2_3.setStoredTypes(move(tmp_schema2_3));
	schema2_3.setStoredColumnNames(tmp_schema2_3_name);
	Schema schema2_4;
	vector<LogicalType> tmp_schema2_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_4.setStoredTypes(move(tmp_schema2_4));
	schema2_4.setStoredColumnNames(tmp_schema2_4_name);
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_name {"post._id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_schema2_name);
	vector<uint32_t> inner_col_map2 = {8};
	vector<uint32_t> outer_col_map2 = {0, 1, 2, 3, 4, 5, 6, 7};
	vector<vector<uint32_t>> outer_col_maps2 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7}};
	vector<Schema> schema2s = {schema2_1, schema2_2, schema2_3, schema2_4};

	// fetch person
	Schema schema3_1;
	vector<LogicalType> tmp_schema3_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_1.setStoredTypes(move(tmp_schema3_1));
	schema3_1.setStoredColumnNames(tmp_schema3_1_name);
	Schema schema3_2;
	vector<LogicalType> tmp_schema3_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_2.setStoredTypes(move(tmp_schema3_2));
	schema3_2.setStoredColumnNames(tmp_schema3_2_name);
	Schema schema3_3;
	vector<LogicalType> tmp_schema3_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_3.setStoredTypes(move(tmp_schema3_3));
	schema3_3.setStoredColumnNames(tmp_schema3_3_name);
	Schema schema3_4;
	vector<LogicalType> tmp_schema3_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_4.setStoredTypes(move(tmp_schema3_4));
	schema3_4.setStoredColumnNames(tmp_schema3_4_name);
	Schema schema3;
	vector<LogicalType> tmp_schema3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_name {"post._id", "post.content", "post.imageFile", "post.length", 
									 "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3.setStoredTypes(move(tmp_schema3));
	schema3.setStoredColumnNames(tmp_schema3_name);
	vector<idx_t> oids3 = {379, 381};
	vector<vector<uint64_t>> projection_mapping3;
	vector<vector<uint64_t>> scan_projection_mapping3;
	projection_mapping3.push_back({0, 1, 2, 3, 7, 8, 9, 10}); // 379
	scan_projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7});
	projection_mapping3.push_back({0, 4, 5, 6, 7, 8, 9, 10}); // 381
	scan_projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7});
	vector<bool> null_possible3 = {false, true, true, true, true, true, true, false, false, false, false};
	vector<vector<uint32_t>> inner_col_map3 = {{9, 10, 11, 12, 16, 17, 18, 19},
											   {9, 13, 14, 15, 16, 17, 18, 19}};
	vector<uint32_t> outer_col_map3 = {0, 1, 2, 3, 4, 5, 6, 7, 8};
	vector<vector<uint32_t>> outer_col_maps3 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7, 8},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7, 8},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7, 8},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7, 8}};
	vector<Schema> schema3s = {schema3_1, schema3_2, schema3_3, schema3_4};


	// Produce results
	Schema schema4_1;
	vector<LogicalType> tmp_schema4_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_1.setStoredTypes(move(tmp_schema4_1));
	schema4_1.setStoredColumnNames(tmp_schema4_1_name);
	Schema schema4_2;
	vector<LogicalType> tmp_schema4_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_2.setStoredTypes(move(tmp_schema4_2));
	schema4_2.setStoredColumnNames(tmp_schema4_2_name);
	Schema schema4_3;
	vector<LogicalType> tmp_schema4_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_3.setStoredTypes(move(tmp_schema4_3));
	schema4_3.setStoredColumnNames(tmp_schema4_3_name);
	Schema schema4_4;
	vector<LogicalType> tmp_schema4_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_4.setStoredTypes(move(tmp_schema4_4));
	schema4_4.setStoredColumnNames(tmp_schema4_4_name);
	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									  	  LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
										  LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_final_schema_name {"post._id", "post.content", "post.imageFile", "post.length", 
										  "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
										  "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_final_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	3, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	6, 7, 8, 9, 
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 12,
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
		4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
	// 	4, 5, 6, 7, 8});
	// projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8});
	vector<Schema> final_schemas = {schema4_1, schema4_2, schema4_3, schema4_4};

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	// ops.push_back(new PhysicalFilter(schema2, std::move(predicates)));
	// ops.push_back(new PhysicalProjection(schema2, std::move(proj_exprs)));
	ops.push_back(new PhysicalAdjIdxJoin(schema2, 523, JoinType::INNER, 0, false, outer_col_maps2, inner_col_map2)); // post_has_creator
	// ops.push_back(new PhysicalIdSeek(schema3, 8, oids3, projection_mapping3, outer_col_maps3, inner_col_map3, scan_projection_mapping3)); // fetch person
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));
	
	// schema flow graph
	size_t pipeline_length = 4;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::BINARY, OperatorType::BINARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4, 1}, {4, 2}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, schema2s, schema3s, final_schemas };
	vector<Schema> pipeline_union_schema = { post_union_schema, schema2, schema3, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[2].resize(8);
	flow_graph[3].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	flow_graph[1][2] = 2;
	flow_graph[1][3] = 3;
	flow_graph[2][0] = 0;
	flow_graph[2][1] = 1;
	flow_graph[2][2] = 2;
	flow_graph[2][3] = 3;
	flow_graph[2][4] = 0;
	flow_graph[2][5] = 1;
	flow_graph[2][6] = 2;
	flow_graph[2][7] = 3;
	flow_graph[3][0] = 0;
	flow_graph[3][1] = 1;
	flow_graph[3][2] = 2;
	flow_graph[3][3] = 3;
	
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch8_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST8() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch8_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* sch8_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	// vector<Schema> post_schemas = {schema1_1, schema1_3, schema1_4};
	// vector<idx_t> oids1 = {443, 447, 449};
	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	// expand (post -[has_creator]-> person)
	Schema schema2_1;
	vector<LogicalType> tmp_schema2_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id"};
	schema2_1.setStoredTypes(move(tmp_schema2_1));
	schema2_1.setStoredColumnNames(tmp_schema2_1_name);
	Schema schema2_2;
	vector<LogicalType> tmp_schema2_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_2.setStoredTypes(move(tmp_schema2_2));
	schema2_2.setStoredColumnNames(tmp_schema2_2_name);
	Schema schema2_3;
	vector<LogicalType> tmp_schema2_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id"};
	schema2_3.setStoredTypes(move(tmp_schema2_3));
	schema2_3.setStoredColumnNames(tmp_schema2_3_name);
	Schema schema2_4;
	vector<LogicalType> tmp_schema2_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_4.setStoredTypes(move(tmp_schema2_4));
	schema2_4.setStoredColumnNames(tmp_schema2_4_name);
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_name {"post._id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_schema2_name);
	vector<uint32_t> inner_col_map2 = {8};
	vector<uint32_t> outer_col_map2 = {0, 1, 2, 3, 4, 5, 6, 7};
	vector<vector<uint32_t>> outer_col_maps2 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7}};
	vector<Schema> schema2s = {schema2_1, schema2_2, schema2_3, schema2_4};

	// fetch person
	Schema schema3_1;
	vector<LogicalType> tmp_schema3_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_1.setStoredTypes(move(tmp_schema3_1));
	schema3_1.setStoredColumnNames(tmp_schema3_1_name);
	Schema schema3_2;
	vector<LogicalType> tmp_schema3_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_2.setStoredTypes(move(tmp_schema3_2));
	schema3_2.setStoredColumnNames(tmp_schema3_2_name);
	Schema schema3_3;
	vector<LogicalType> tmp_schema3_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_3.setStoredTypes(move(tmp_schema3_3));
	schema3_3.setStoredColumnNames(tmp_schema3_3_name);
	Schema schema3_4;
	vector<LogicalType> tmp_schema3_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_4.setStoredTypes(move(tmp_schema3_4));
	schema3_4.setStoredColumnNames(tmp_schema3_4_name);
	Schema schema3;
	vector<LogicalType> tmp_schema3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_name {"post._id", "post.content", "post.imageFile", "post.length", 
									 "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3.setStoredTypes(move(tmp_schema3));
	schema3.setStoredColumnNames(tmp_schema3_name);
	vector<idx_t> oids3 = {379, 381};
	vector<vector<uint64_t>> projection_mapping3;
	vector<vector<uint64_t>> scan_projection_mapping3;
	projection_mapping3.push_back({0, 1, 2, 3, 7, 8, 9, 10}); // 379
	scan_projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7});
	projection_mapping3.push_back({0, 4, 5, 6, 7, 8, 9, 10}); // 381
	scan_projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7});
	vector<bool> null_possible3 = {false, true, true, true, true, true, true, false, false, false, false};
	vector<vector<uint32_t>> inner_col_map3 = {{9, 10, 11, 12, 16, 17, 18, 19},
											   {9, 13, 14, 15, 16, 17, 18, 19}};
	vector<uint32_t> outer_col_map3 = {0, 1, 2, 3, 4, 5, 6, 7, 8};
	vector<vector<uint32_t>> outer_col_maps3 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7, 8},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7, 8},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7, 8},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7, 8}};
	vector<Schema> schema3s = {schema3_1, schema3_2, schema3_3, schema3_4};


	// Produce results
	Schema schema4_1;
	vector<LogicalType> tmp_schema4_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_1.setStoredTypes(move(tmp_schema4_1));
	schema4_1.setStoredColumnNames(tmp_schema4_1_name);
	Schema schema4_2;
	vector<LogicalType> tmp_schema4_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_2.setStoredTypes(move(tmp_schema4_2));
	schema4_2.setStoredColumnNames(tmp_schema4_2_name);
	Schema schema4_3;
	vector<LogicalType> tmp_schema4_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_3.setStoredTypes(move(tmp_schema4_3));
	schema4_3.setStoredColumnNames(tmp_schema4_3_name);
	Schema schema4_4;
	vector<LogicalType> tmp_schema4_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_4.setStoredTypes(move(tmp_schema4_4));
	schema4_4.setStoredColumnNames(tmp_schema4_4_name);
	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									  	  LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
										  LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_final_schema_name {"post._id", "post.content", "post.imageFile", "post.length", 
										  "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
										  "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_final_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	3, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	6, 7, 8, 9, 
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 12,
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
		4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
	// 	4, 5, 6, 7, 8});
	// projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8});
	vector<Schema> final_schemas = {schema4_1, schema4_2, schema4_3, schema4_4};

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	// ops.push_back(new PhysicalFilter(schema2, std::move(predicates)));
	// ops.push_back(new PhysicalProjection(schema2, std::move(proj_exprs)));
	ops.push_back(new PhysicalAdjIdxJoin(schema2, 523, JoinType::INNER, 0, false, outer_col_maps2, inner_col_map2)); // post_has_creator
	// ops.push_back(new PhysicalIdSeek(schema3, 8, oids3, projection_mapping3, outer_col_maps3, inner_col_map3, scan_projection_mapping3)); // fetch person
	ops.push_back(new PhysicalTop(final_schema, 10, 0));
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));
	
	// schema flow graph
	size_t pipeline_length = 5;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::BINARY, OperatorType::BINARY, OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4, 1}, {4, 2}, {4}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, schema2s, schema3s, final_schemas, final_schemas };
	vector<Schema> pipeline_union_schema = { post_union_schema, schema2, schema3, final_schema, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[2].resize(8);
	flow_graph[3].resize(4);
	flow_graph[4].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	flow_graph[1][2] = 2;
	flow_graph[1][3] = 3;
	flow_graph[2][0] = 0;
	flow_graph[2][1] = 1;
	flow_graph[2][2] = 2;
	flow_graph[2][3] = 3;
	flow_graph[2][4] = 0;
	flow_graph[2][5] = 1;
	flow_graph[2][6] = 2;
	flow_graph[2][7] = 3;
	flow_graph[3][0] = 0;
	flow_graph[3][1] = 1;
	flow_graph[3][2] = 2;
	flow_graph[3][3] = 3;
	flow_graph[4][0] = 0;
	flow_graph[4][1] = 1;
	flow_graph[4][2] = 2;
	flow_graph[4][3] = 3;
	
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch9_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor *sch9_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor *prev_pipe);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST9() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch9_pipe1(*this);
	auto p2 = sch9_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* sch9_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	// vector<Schema> post_schemas = {schema1_1, schema1_3, schema1_4};
	// vector<idx_t> oids1 = {443, 447, 449};
	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	// expand (post -[has_creator]-> person)
	Schema schema2_1;
	vector<LogicalType> tmp_schema2_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id"};
	schema2_1.setStoredTypes(move(tmp_schema2_1));
	schema2_1.setStoredColumnNames(tmp_schema2_1_name);
	Schema schema2_2;
	vector<LogicalType> tmp_schema2_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_2.setStoredTypes(move(tmp_schema2_2));
	schema2_2.setStoredColumnNames(tmp_schema2_2_name);
	Schema schema2_3;
	vector<LogicalType> tmp_schema2_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id"};
	schema2_3.setStoredTypes(move(tmp_schema2_3));
	schema2_3.setStoredColumnNames(tmp_schema2_3_name);
	Schema schema2_4;
	vector<LogicalType> tmp_schema2_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2_4.setStoredTypes(move(tmp_schema2_4));
	schema2_4.setStoredColumnNames(tmp_schema2_4_name);
	Schema schema2;
	vector<LogicalType> tmp_schema2 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT};
	vector<string> tmp_schema2_name {"post._id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id", "person._id"};
	schema2.setStoredTypes(move(tmp_schema2));
	schema2.setStoredColumnNames(tmp_schema2_name);
	vector<uint32_t> inner_col_map2 = {8};
	vector<uint32_t> outer_col_map2 = {0, 1, 2, 3, 4, 5, 6, 7};
	vector<vector<uint32_t>> outer_col_maps2 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7}};
	vector<Schema> schema2s = {schema2_1, schema2_2, schema2_3, schema2_4};

	// fetch person
	Schema schema3_1;
	vector<LogicalType> tmp_schema3_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_1.setStoredTypes(move(tmp_schema3_1));
	schema3_1.setStoredColumnNames(tmp_schema3_1_name);
	Schema schema3_2;
	vector<LogicalType> tmp_schema3_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_2.setStoredTypes(move(tmp_schema3_2));
	schema3_2.setStoredColumnNames(tmp_schema3_2_name);
	Schema schema3_3;
	vector<LogicalType> tmp_schema3_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_3.setStoredTypes(move(tmp_schema3_3));
	schema3_3.setStoredColumnNames(tmp_schema3_3_name);
	Schema schema3_4;
	vector<LogicalType> tmp_schema3_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3_4.setStoredTypes(move(tmp_schema3_4));
	schema3_4.setStoredColumnNames(tmp_schema3_4_name);
	Schema schema3;
	vector<LogicalType> tmp_schema3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema3_name {"post._id", "post.content", "post.imageFile", "post.length", 
									 "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema3.setStoredTypes(move(tmp_schema3));
	schema3.setStoredColumnNames(tmp_schema3_name);
	vector<idx_t> oids3 = {379, 381};
	vector<vector<uint64_t>> projection_mapping3;
	vector<vector<uint64_t>> scan_projection_mapping3;
	projection_mapping3.push_back({0, 1, 2, 3, 7, 8, 9, 10}); // 379
	scan_projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7});
	projection_mapping3.push_back({0, 4, 5, 6, 7, 8, 9, 10}); // 381
	scan_projection_mapping3.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5, 6, 7});
	vector<bool> null_possible3 = {false, true, true, true, true, true, true, false, false, false, false};
	vector<vector<uint32_t>> inner_col_map3 = {{9, 10, 11, 12, 16, 17, 18, 19},
											   {9, 13, 14, 15, 16, 17, 18, 19}};
	vector<uint32_t> outer_col_map3 = {0, 1, 2, 3, 4, 5, 6, 7, 8};
	vector<vector<uint32_t>> outer_col_maps3 = {
		{0, std::numeric_limits<uint32_t>::max(), 2, 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7, 8},
		{0, std::numeric_limits<uint32_t>::max(), 2, std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7, 8},
		{0, 1, std::numeric_limits<uint32_t>::max(), 3, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 6, 7, 8},
		{0, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max(), 4, 5, 6, 7, 8}};
	vector<Schema> schema3s = {schema3_1, schema3_2, schema3_3, schema3_4};

	// Top N Sort
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 7);
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 18);
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

	// Produce results
	Schema schema4_1;
	vector<LogicalType> tmp_schema4_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_1.setStoredTypes(move(tmp_schema4_1));
	schema4_1.setStoredColumnNames(tmp_schema4_1_name);
	Schema schema4_2;
	vector<LogicalType> tmp_schema4_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_2.setStoredTypes(move(tmp_schema4_2));
	schema4_2.setStoredColumnNames(tmp_schema4_2_name);
	Schema schema4_3;
	vector<LogicalType> tmp_schema4_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_3.setStoredTypes(move(tmp_schema4_3));
	schema4_3.setStoredColumnNames(tmp_schema4_3_name);
	Schema schema4_4;
	vector<LogicalType> tmp_schema4_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_4.setStoredTypes(move(tmp_schema4_4));
	schema4_4.setStoredColumnNames(tmp_schema4_4_name);
	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									  	  LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
										  LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_final_schema_name {"post._id", "post.content", "post.imageFile", "post.length", 
										  "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
										  "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_final_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	3, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	6, 7, 8, 9, 
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 12,
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
		4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
	// 	4, 5, 6, 7, 8});
	// projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8});
	vector<Schema> final_schemas = {schema4_1, schema4_2, schema4_3, schema4_4};

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// intermediate ops
	ops.push_back(new PhysicalAdjIdxJoin(schema2, 523, JoinType::INNER, 0, false, outer_col_maps2, inner_col_map2)); // post_has_creator
	// ops.push_back(new PhysicalIdSeek(schema3, 8, oids3, projection_mapping3, outer_col_maps3, inner_col_map3, scan_projection_mapping3)); // fetch person
	// sink
	ops.push_back(new PhysicalTopNSort(final_schema, move(orders), 10, 0));
	
	// schema flow graph
	size_t pipeline_length = 4;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::BINARY, OperatorType::BINARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4, 1}, {4, 2}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, schema2s, schema3s, final_schemas };
	vector<Schema> pipeline_union_schema = { post_union_schema, schema2, schema3, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[2].resize(8);
	flow_graph[3].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	flow_graph[1][2] = 2;
	flow_graph[1][3] = 3;
	flow_graph[2][0] = 0;
	flow_graph[2][1] = 1;
	flow_graph[2][2] = 2;
	flow_graph[2][3] = 3;
	flow_graph[2][4] = 0;
	flow_graph[2][5] = 1;
	flow_graph[2][6] = 2;
	flow_graph[2][7] = 3;
	flow_graph[3][0] = 0;
	flow_graph[3][1] = 1;
	flow_graph[3][2] = 2;
	flow_graph[3][3] = 3;
	
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

CypherPipelineExecutor *sch9_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor *prev_pipe) {
	// Produce results
	Schema schema4_1;
	vector<LogicalType> tmp_schema4_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_1_name {"post._id", "imageFile", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_1.setStoredTypes(move(tmp_schema4_1));
	schema4_1.setStoredColumnNames(tmp_schema4_1_name);
	Schema schema4_2;
	vector<LogicalType> tmp_schema4_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_2_name {"post._id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_2.setStoredTypes(move(tmp_schema4_2));
	schema4_2.setStoredColumnNames(tmp_schema4_2_name);
	Schema schema4_3;
	vector<LogicalType> tmp_schema4_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_3_name {"post._id", "content", "length", 
									   "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_3.setStoredTypes(move(tmp_schema4_3));
	schema4_3.setStoredColumnNames(tmp_schema4_3_name);
	Schema schema4_4;
	vector<LogicalType> tmp_schema4_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT,
									 LogicalType::UBIGINT,
									 LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_schema4_4_name {"post._id", "locationIP", "browserUsed", "creationDate", "id", "person._id",
									 "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	schema4_4.setStoredTypes(move(tmp_schema4_4));
	schema4_4.setStoredColumnNames(tmp_schema4_4_name);
	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									  	  LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
										  LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR};
	vector<string> tmp_final_schema_name {"post._id", "post.content", "post.imageFile", "post.length", 
										  "post.locationIP", "post.browserUsed", "post.creationDate", "post.id", "person._id",
										  "person._id", "person.browserUsed", "person.speaks", "person.email",
									 "person.locationIP", "person.birthday", "person.creationDate", 
									 "person.firstName", "person.lastName", "person.id", "person.gender"};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_final_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	3, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	6, 7, 8, 9, 
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 12,
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(),
	// 	19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
		4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
		std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8, 9, 
		10, 11, 12, 13, 14, 15,
		16, 17, 18, 19});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, 3, std::numeric_limits<uint8_t>::max(),
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), 2, std::numeric_limits<uint8_t>::max(), 
	// 	4, 5, 6, 7, 8});
	// projection_mapping2.push_back({0, 1, std::numeric_limits<uint8_t>::max(), 3, std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 6, 7, 8});
	// projection_mapping2.push_back({0, std::numeric_limits<uint8_t>::max(), std::numeric_limits<uint8_t>::max(), 
	// 	std::numeric_limits<uint8_t>::max(), 4, 5, 6, 7, 8});
	vector<Schema> final_schemas = {schema4_1, schema4_2, schema4_3, schema4_4};

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(prev_pipe->pipeline->GetSink());
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));

	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4}};
	vector<vector<Schema>> pipelien_schemas = { final_schemas, final_schemas };
	vector<Schema> pipeline_union_schema = { final_schema, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 1;
	flow_graph[1][2] = 2;
	flow_graph[1][3] = 3;
	
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch10_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor *sch10_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor *prev_pipe);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST10() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch10_pipe1(*this);
	auto p2 = sch10_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* sch10_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	// Top N Sort
	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 6);
	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, move(order_expr_1));
	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 7);
	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, move(order_expr_2));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order1));
	orders.push_back(move(order2));

	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_union_schema_name);
	vector<Schema> final_schemas = { final_schema };

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// sink
	ops.push_back(new PhysicalTopNSort(final_schema, move(orders), 20, 0));
	
	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, final_schemas };
	vector<Schema> pipeline_union_schema = { post_union_schema, post_union_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 0;
	flow_graph[1][2] = 0;
	flow_graph[1][3] = 0;
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

CypherPipelineExecutor *sch10_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor *prev_pipe) {
	// Produce results
	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_union_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	projection_mapping2.push_back({0, 1, 2, 3, 4, 5, 6, 7});
	vector<Schema> final_schemas = { final_schema };

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(prev_pipe->pipeline->GetSink());
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));

	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{1}, {1}};
	vector<vector<Schema>> pipelien_schemas = { final_schemas, final_schemas };
	vector<Schema> pipeline_union_schema = { final_schema, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(1);
	flow_graph[1].resize(1);
	flow_graph[0][0] = 0;
	flow_graph[1][0] = 0;
	
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg, childs);
	return pipeexec;
}

// /data/ldbc/sf1_schemaless
CypherPipelineExecutor *sch11_pipe1(QueryPlanSuite& suite);
CypherPipelineExecutor *sch11_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor *prev_pipe);

std::vector<CypherPipelineExecutor *> QueryPlanSuite::SCHEMALESS_TEST11() {
	std::vector<CypherPipelineExecutor*> result;
	auto p1 = sch11_pipe1(*this);
	auto p2 = sch11_pipe2(*this, p1);
	result.push_back(p1);
	result.push_back(p2);
	return result;
}

CypherPipelineExecutor* sch11_pipe1(QueryPlanSuite& suite) {
	// scan Post
	// 443 schema: 1 2 5 6
	Schema schema1_1;
	vector<LogicalType> tmp_schema1_1 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_1_name {"_id", "imageFile", "length", 
									   "creationDate", "id"};
	schema1_1.setStoredTypes(move(tmp_schema1_1));
	schema1_1.setStoredColumnNames(tmp_schema1_1_name);

	// 445 schema: 1 3 4 5 6
	Schema schema1_2;
	vector<LogicalType> tmp_schema1_2 {LogicalType::ID, LogicalType::VARCHAR,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_2_name {"_id", "imageFile",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_2.setStoredTypes(move(tmp_schema1_2));
	schema1_2.setStoredColumnNames(tmp_schema1_2_name);

	// 447 schema: 0 2 5 6
	Schema schema1_3;
	vector<LogicalType> tmp_schema1_3 {LogicalType::ID, LogicalType::VARCHAR, LogicalType::BIGINT,
									   LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_3_name {"_id", "content", "length", 
									   "creationDate", "id"};
	schema1_3.setStoredTypes(move(tmp_schema1_3));
	schema1_3.setStoredColumnNames(tmp_schema1_3_name);

	// 449 schema: 3 4 5 6
	Schema schema1_4;
	vector<LogicalType> tmp_schema1_4 {LogicalType::ID,
									   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_schema1_4_name {"_id",
									   "locationIP", "browserUsed", "creationDate", "id"};
	schema1_4.setStoredTypes(move(tmp_schema1_4));
	schema1_4.setStoredColumnNames(tmp_schema1_4_name);

	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
									 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<string> tmp_union_schema_name {"_id", "content", "imageFile", "length", 
										  "locationIP", "browserUsed", "creationDate", "id"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	vector<vector<uint64_t>> projection_mapping1;
	vector<vector<uint64_t>> scan_projection_mapping1;
	projection_mapping1.push_back({0, 2, 3, 6, 7}); // 443
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 2, 4, 5, 6, 7}); // 445
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4, 5});
	projection_mapping1.push_back({0, 1, 3, 6, 7}); // 447
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});
	projection_mapping1.push_back({0, 4, 5, 6, 7}); // 449
	scan_projection_mapping1.push_back({std::numeric_limits<uint64_t>::max(), 1, 2, 3, 4});

	vector<Schema> post_schemas = {schema1_1, schema1_2, schema1_3, schema1_4};
	vector<idx_t> oids1 = {443, 445, 447, 449};

	// aggregate - distinct (in: _p _t)
	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_groups;
	agg_groups.push_back(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4));

	Schema final_schema;
	// vector<LogicalType> tmp_final_schema {LogicalType::ID, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
	// 								 LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::UBIGINT};
	vector<LogicalType> tmp_final_schema {LogicalType::VARCHAR};
	vector<string> tmp_final_schema_name {"locationIP"};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_final_schema_name);
	vector<Schema> final_schemas = { final_schema };

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(new PhysicalNodeScan(post_schemas, post_union_schema, move(oids1), move(projection_mapping1), move(scan_projection_mapping1)));
	// sink
	ops.push_back( new PhysicalHashAggregate(final_schema, move(agg_exprs), move(agg_groups)));
	
	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{4}, {4}};
	vector<vector<Schema>> pipelien_schemas = { post_schemas, final_schemas };
	vector<Schema> pipeline_union_schema = { post_union_schema, post_union_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(4);
	flow_graph[1].resize(4);
	flow_graph[0][0] = 0;
	flow_graph[0][1] = 1;
	flow_graph[0][2] = 2;
	flow_graph[0][3] = 3;
	flow_graph[1][0] = 0;
	flow_graph[1][1] = 0;
	flow_graph[1][2] = 0;
	flow_graph[1][3] = 0;
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg);
	return pipeexec;
}

CypherPipelineExecutor *sch11_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor *prev_pipe) {
	// Produce results
	Schema post_union_schema;
	vector<LogicalType> tmp_union_schema {LogicalType::VARCHAR};
	vector<string> tmp_union_schema_name {"locationIP"};
	post_union_schema.setStoredTypes(move(tmp_union_schema));
	post_union_schema.setStoredColumnNames(tmp_union_schema_name);

	Schema final_schema;
	vector<LogicalType> tmp_final_schema {LogicalType::VARCHAR};
	final_schema.setStoredTypes(move(tmp_final_schema));
	final_schema.setStoredColumnNames(tmp_union_schema_name);
	vector<vector<uint8_t>> projection_mapping2;
	projection_mapping2.push_back({0, 1, 2, 3, 4, 5, 6, 7});
	vector<Schema> final_schemas = { final_schema };

// pipe
	std::vector<CypherPhysicalOperator *> ops;
	// src
	ops.push_back(prev_pipe->pipeline->GetSink());
	// sink
	ops.push_back(new PhysicalProduceResults(final_schema, projection_mapping2));

	// schema flow graph
	size_t pipeline_length = 2;
	vector<OperatorType> pipeline_operator_types {OperatorType::UNARY, OperatorType::UNARY};
	vector<vector<uint64_t>> num_schemas_of_childs = {{1}, {1}};
	vector<vector<Schema>> pipelien_schemas = { final_schemas, final_schemas };
	vector<Schema> pipeline_union_schema = { final_schema, final_schema };
	SchemaFlowGraph sfg(pipeline_length, pipeline_operator_types, num_schemas_of_childs, pipelien_schemas, pipeline_union_schema);
	vector<vector<idx_t>> flow_graph;
	flow_graph.resize(pipeline_length);
	flow_graph[0].resize(1);
	flow_graph[1].resize(1);
	flow_graph[0][0] = 0;
	flow_graph[1][0] = 0;
	
	sfg.SetFlowGraph(flow_graph);

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	vector<CypherPipelineExecutor*> childs;
	childs.push_back(prev_pipe);
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, sfg, childs);
	return pipeexec;
}

}