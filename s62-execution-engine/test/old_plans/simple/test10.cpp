
#include "plans/query_plan_suite.hpp"

#include "function/aggregate/distributive_functions.hpp"
#include "function/aggregate/nested_functions.hpp"

#include "icecream.hpp"

namespace s62 {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test10() {

/*
// Q : group by (name)) collect(id) unwind(id) ; groupby + unwind 
*/

// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	CypherPipelineExecutor* pipeexec1;
	CypherPhysicalOperator* pipe1_sink_op;
	CypherPipelineExecutor* pipeexec2;

// pipe 1

	Schema schema;
	schema.addNode("n");
	schema.addPropertyIntoNode("n", "name", s62::LogicalType::VARCHAR);
	schema.addPropertyIntoNode("n", "id", s62::LogicalType::UBIGINT);
	
	// scan params
	LabelSet scan_labels;
	scan_labels.insert("Organisation");
	PropertyKeys scan_propertyKeys;
	scan_propertyKeys.push_back("name");
	scan_propertyKeys.push_back("id");	// n._id, n.name, n.id

	// sort params
	unique_ptr<Expression> aggr_expr;
	// define agg op
	Schema agg_schema;
	agg_schema.addColumn("name", LogicalType::VARCHAR);
	agg_schema.addColumn("list", LogicalType::LIST(LogicalType::UBIGINT) );

	vector<unique_ptr<Expression>> agg_exprs;
	vector<unique_ptr<Expression>> agg_expr_1_child;
	vector<unique_ptr<Expression>> agg_groups;
	agg_expr_1_child.push_back( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2) ); // n.id
	auto agg_expr_1_func =  ListFun::GetFunction();
	agg_expr_1_func.bind(context, agg_expr_1_func, agg_expr_1_child);	// (important) list function needs binding of child type
	agg_exprs.push_back( make_unique<BoundAggregateExpression>(agg_expr_1_func, move(agg_expr_1_child), nullptr, nullptr, false  ) ); // collect(id)
	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1) ); // order by name
	
	pipe1_sink_op = new PhysicalHashAggregate(agg_schema, move(agg_exprs), move(agg_groups));
	
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
	Schema unwind_schema;
	unwind_schema.addColumn("name", LogicalType::VARCHAR);
	unwind_schema.addColumn("id", LogicalType::UBIGINT);
	{
		std::vector<CypherPhysicalOperator *> ops;
		// source
		ops.push_back(pipe1_sink_op);
		// operators
		ops.push_back(new PhysicalUnwind(unwind_schema, (idx_t)1));	// unnest id column
		// sink
		ops.push_back(new PhysicalProduceResults(unwind_schema));

		auto pipe1 = new CypherPipeline(ops);
		auto ctx1 = new ExecutionContext(&context);
		vector<CypherPipelineExecutor*> childs;
		childs.push_back(pipeexec1);
		pipeexec2 = new CypherPipelineExecutor(ctx1, pipe1, childs);
		result.push_back(pipeexec2);
	}
	// return
	return result;
IC();
}

}