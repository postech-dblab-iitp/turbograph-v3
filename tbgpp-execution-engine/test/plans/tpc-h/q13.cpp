// #include "plans/query_plan_suite.hpp"
// #include "function/aggregate/distributive_functions.hpp"
// #include "function/scalar/string_functions.hpp"


// namespace duckdb {

// CypherPipelineExecutor* q13_pipe1(QueryPlanSuite& suite);
// CypherPipelineExecutor* q13_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
// CypherPipelineExecutor* q13_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
// CypherPipelineExecutor* q13_pipe4(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::TPCH_Q13() {

// 	std::vector<CypherPipelineExecutor*> result;
// 	auto p1 = q13_pipe1(*this);
// 	auto p2 = q13_pipe2(*this, p1);
// 	auto p3 = q13_pipe3(*this, p2);
// 	auto p4 = q13_pipe4(*this, p3);
// 	result.push_back(p1);
// 	result.push_back(p2);
// 	result.push_back(p3);
// 	result.push_back(p4);
// 	return result;

// }

// CypherPipelineExecutor* q13_pipe1(QueryPlanSuite& suite) {
	
// 	// scan o with O_COMMENT


// 	// filter (_o, o.c)

// 	// optional match (_o, o.c)

// 	// aggregate (_o, o.c, _c)

// // pipes
// 	std::vector<CypherPhysicalOperator *> ops;

// 	ops.push_back( new PhysicalNodeScan(sch1) );
// 	ops.push_back( new PhysicalFilter(sch1) );
// 	ops.push_back( new PhysicalAdjIdxJoin(sch2) );
// 	ops.push_back( new PhysicalHashAggregate(sch3) );

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
// 	return pipeexec;
// }

// CypherPipelineExecutor* q13_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe ) {

// 	// aggregate2 (c_id, c_count)

// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( prev_pipe->pipeline->GetSink() );
// 	// op
// 	// sink
// 	ops.push_back( new PhysicalHashAggregate(sch1) );

// // pipes, add child
// 	vector<CypherPipelineExecutor*> childs;
// 	childs.push_back(prev_pipe);

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
// 	return pipeexec;
// }


// CypherPipelineExecutor* q13_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	// orderby (c_count, custdist)


// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( prev_pipe->pipeline->GetSink() );
// 	// op
// 	// sink
// 	ops.push_back( new PhysicalTopNSort(sch1) );

// // pipes, add child
// 	vector<CypherPipelineExecutor*> childs;
// 	childs.push_back(prev_pipe);

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
// 	return pipeexec;
// }

// CypherPipelineExecutor* q13_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	// produce!
	
	
// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( prev_pipe->pipeline->GetSink() );
// 	// op
// 	// sink
// 	ops.push_back( new PhysicalProduceResults(sch1));

// // pipes, add child
// 	vector<CypherPipelineExecutor*> childs;
// 	childs.push_back(prev_pipe);

// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
// 	return pipeexec;


// }


// }