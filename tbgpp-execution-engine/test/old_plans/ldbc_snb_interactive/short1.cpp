#include "plans/query_plan_suite.hpp"

namespace s62 {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS1() {

	// scan schema
	Schema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "birthday", s62::LogicalType::BIGINT);
	schema.addPropertyIntoNode("n", "firstName", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "lastName", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "gender", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "browserUsed", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "locationIP", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "creationDate", s62::LogicalType::BIGINT);
	
	// scan params
	LabelSet scan_labels;
	PropertyKeys scan_propertyKeys;
	scan_labels.insert("Person");
	scan_propertyKeys.push_back("birthday");
	scan_propertyKeys.push_back("firstName");
	scan_propertyKeys.push_back("lastName");
	scan_propertyKeys.push_back("gender");
	scan_propertyKeys.push_back("browserUsed");
	scan_propertyKeys.push_back("locationIP");
	scan_propertyKeys.push_back("creationDate");
	
	// Filter
	s62::Value filter_val; // person key
	if(LDBC_SF==1) { filter_val = s62::Value::UBIGINT(35184372099695); }
	if(LDBC_SF==10) { filter_val = s62::Value::UBIGINT(14); }
	if(LDBC_SF==100) { filter_val = s62::Value::UBIGINT(14); }

	// Expand
	Schema expandschema = schema;
	expandschema.addNode("p");
	
	// FetchId
	Schema schema3 = expandschema;
	schema3.addPropertyIntoNode("p", "id", s62::LogicalType::UBIGINT );
	PropertyKeys seek_propertyKeys;
	seek_propertyKeys.push_back("id");

	// 0 1 2 3 4 5 6 7 8 9
	// _ b f l g b l c _ i

	// Project
	Schema project_schema;
	project_schema.addColumn("firstName", s62::LogicalType::VARCHAR);
	project_schema.addColumn("lastName", s62::LogicalType::VARCHAR);
	project_schema.addColumn("birthday", s62::LogicalType::BIGINT);
	project_schema.addColumn("locationIP", s62::LogicalType::VARCHAR);
	project_schema.addColumn("browserUsed", s62::LogicalType::VARCHAR);
	project_schema.addColumn("cityId", s62::LogicalType::UBIGINT);
	project_schema.addColumn("gender", s62::LogicalType::VARCHAR);
	project_schema.addColumn("creationDate", s62::LogicalType::BIGINT);
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 2)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 9)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 4)) );
		proj_exprs.push_back( move(make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 7)) );
	}
	// pipe 1
	std::vector<CypherPhysicalOperator *> ops;
		// source
	ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys, "id", filter_val));
		//operators
	ops.push_back(new PhysicalAdjIdxJoin(expandschema, "n", LabelSet("Person"), LabelSet("IS_LOCATED_IN"), ExpandDirection::OUTGOING, LabelSet("Place"), JoinType::INNER, false, true));
	ops.push_back(new PhysicalNodeIdSeek(schema3, "p", LabelSet("Place"), seek_propertyKeys));
	ops.push_back(new PhysicalProjection(project_schema, move(proj_exprs)));
		// sink
	ops.push_back(new PhysicalProduceResults(project_schema));
	
	auto pipe1 = new CypherPipeline(ops);
	auto ctx1 = new ExecutionContext(&context);
	auto pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
	// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	result.push_back(pipeexec1);
	return result;
	
}

}