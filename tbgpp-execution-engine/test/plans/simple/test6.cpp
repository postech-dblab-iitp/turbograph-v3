
#include "plans/query_plan_suite.hpp"

namespace duckdb {

std::vector<CypherPipelineExecutor*> QueryPlanSuite::Test6() {

// Q : Sort by n.id LIMIT 3 DESC;

// wrap pipeline into vector
	std::vector<CypherPipelineExecutor*> result;
	CypherPipelineExecutor* pipeexec1;
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
	unique_ptr<Expression> order_expr = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);	// n.id (n._id, n.name, n.id)
	//unique_ptr<Expression> order_expr = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1);	// n.id (n._id, n.id)
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

	unique_ptr<Expression> order_expr2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 2);	// n.id (n._id, n.name, n.id)
	//unique_ptr<Expression> order_expr2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1);	// n.id (n._id, n.id)
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