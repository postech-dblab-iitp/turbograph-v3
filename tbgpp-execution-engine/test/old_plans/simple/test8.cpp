
#include "plans/query_plan_suite.hpp"

#include "function/aggregate/distributive_functions.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test8() {

/*
// Q : count(*); : aggregation without grouping (Match n return count n)
*/

// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	CypherPipelineExecutor* pipeexec1;
	CypherPhysicalOperator* pipe1_sink_op;
	CypherPipelineExecutor* pipeexec2;

// pipe 1

	CypherSchema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "name", duckdb::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", duckdb::LogicalType::UBIGINT);
	
	// scan params
	LabelSet scan_labels;
	scan_labels.insert("Organisation");
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");	// n._id, n.name, n.id

	// sort params
	unique_ptr<Expression> aggr_expr;
	
	// define agg op
	CypherSchema agg_schema;
	agg_schema.addColumn("count", LogicalType::BIGINT);
	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_expr_1_child;
	agg_expr_1_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) );	 		// col
	AggregateFunction agg_expr_1_function = CountFun::GetFunction();
	agg_exprs.push_back( make_unique<BoundAggregateExpression>( agg_expr_1_function, move(agg_expr_1_child), nullptr, nullptr, false  ) ); // count(col) TODO
	pipe1_sink_op = new PhysicalHashAggregate(agg_schema, move(agg_exprs));
	
	{
		std::vector<CypherPhysicalOperator *> ops;
		// source
		ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys) );
		// operators
		// sink
		ops.push_back(pipe1_sink_op);	

		auto pipe1 = new CypherPipeline(ops);
		auto ctx1 = new ExecutionContext(&context);
		pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
		result.push_back(pipeexec1);
	}

// pipe 2
	{
		std::vector<CypherPhysicalOperator *> ops;
		// source
		ops.push_back(pipe1_sink_op);
		// operators
		// sink
		ops.push_back(new PhysicalProduceResults(agg_schema));

		auto pipe1 = new CypherPipeline(ops);
		auto ctx1 = new ExecutionContext(&context);
		vector<CypherPipelineExecutor*> childs;
		childs.push_back(pipeexec1);
		pipeexec2 = new CypherPipelineExecutor(ctx1, pipe1, childs);
		result.push_back(pipeexec2);
	}

	// return
	return result;
}

}