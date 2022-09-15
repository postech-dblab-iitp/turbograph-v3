// #include "plans/query_plan_suite.hpp"


// namespace duckdb {

// CypherPipelineExecutor* ic9_pipe1(QueryPlanSuite& suite);
// CypherPipelineExecutor* ic9_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
// CypherPipelineExecutor* ic9_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC9() {

// 	std::vector<CypherPipelineExecutor*> result;
// 	auto p1 = ic9_pipe1(*this);
// 	auto p2 = ic9_pipe2(*this, p1);
// 	auto p3 = ic9_pipe2(*this, p2);
// 	result.push_back(p1);
// 	result.push_back(p2);
// 	result.push_back(p3);
// 	return result;
// }

// CypherPipelineExecutor* ic9_pipe1(QueryPlanSuite& suite) {

// 	// scan person
// 	CypherSchema sch1;
// 	sch1.addNode("root");
// 	duckdb::Value filter_val; // person key
// 	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(14); }
// 	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(14); }
// 	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(14); }

// 	// person->friend
// 	CypherSchema sch2 = sch1;
// 	sch2.addNode("friend");

// 	// filter (_root, _friend)
// 	vector<unique_ptr<Expression>> filter_exprs;
// 	{
// 		unique_ptr<Expression> filter_expr1;
// 		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, 
// 							make_unique<BoundReferenceExpression>(LogicalType::ID, 0),
// 							make_unique<BoundReferenceExpression>(LogicalType::ID, 1)
// 						);
// 		filter_exprs.push_back(move(filter_expr1));
// 	}

// 	// projection	(_friend)
// 	CypherSchema sch3;
// 	sch3.addNode("friend");

// 	// agg - distinct friend ids
// 	vector<unique_ptr<Expression>> agg_exprs;
// 	vector<unique_ptr<Expression>> agg_groups;
// 	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // distinct friends

// // pipe

// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
// 	// ops
// 	ops.push_back( new PhysicalAdjIdxJoin(sch2, "person", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
// 	// sink
// 	ops.push_back( new PhysicalHashAggregate(sch5, move(agg_exprs), move(agg_groups)));
// //IC();

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
// 	return pipeexec;

// }
// CypherPipelineExecutor* ic9_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	// friend->comment

// 	// attach comment seek

// 	// filter

// 	// attach friend id

// 	// projection

// 	// orderby

// }
// CypherPipelineExecutor* ic9_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	//produce

// }



// }