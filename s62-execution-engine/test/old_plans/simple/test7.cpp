
#include "plans/query_plan_suite.hpp"

namespace s62 {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test7() {

/*
// Q : Sort by n.name LIMIT 3 DESC;
===================================================
[ResultSetSummary] Total 3 tuples. Showing top 10:
        VARCHAR UBIGINT
        Ţiriac_Air      1017
        Škoda_Auto_University   2470
        İzmir_University_of_Economics   6581
===================================================
*/

// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	CypherPipelineExecutor* pipeexec1;
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
	unique_ptr<Expression> order_expr = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// n.name (n._id, n.name, n.id)
	BoundOrderByNode order(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr));
	vector<BoundOrderByNode> orders;
	orders.push_back(move(order));

	{
		std::vector<CypherPhysicalOperator *> ops;
		// source
		ops.push_back(new PhysicalNodeScan(schema, scan_labels, scan_propertyKeys) );
		// operators
		// sink
	ops.push_back(new PhysicalTopNSort(schema, move(orders), (idx_t)3, (idx_t)0));	

		auto pipe1 = new CypherPipeline(ops);
		auto ctx1 = new ExecutionContext(&context);
		pipeexec1 = new CypherPipelineExecutor(ctx1, pipe1);
		result.push_back(pipeexec1);
	}

// pipe 2

	unique_ptr<Expression> order_expr2 = make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 1);	// n.name (n._id, n.name, n.id)
	BoundOrderByNode order2(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr2));
	vector<BoundOrderByNode> orders2;
	orders2.push_back(move(order2));
	{
		std::vector<CypherPhysicalOperator *> ops;
		// source
		ops.push_back(new PhysicalTopNSort(schema, move(orders2), (idx_t)3, (idx_t)0));
		// operators
		// sink
		ops.push_back(new PhysicalProduceResults(schema));

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