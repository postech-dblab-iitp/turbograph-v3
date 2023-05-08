#include "plans/query_plan_suite.hpp"

//  --nodes:IMAGES images --nodes:CATEGORIES categories --nodes:ANNOTATIONS annotations --nodes:LICENSES licenses

namespace duckdb {

CypherPipelineExecutor* coco3a_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::COCO_Q3A() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = coco3a_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* coco3a_pipe1(QueryPlanSuite& suite) {

	Schema sch1;
	sch1.addNode("images");
	sch1.addPropertyIntoNode("images", "id", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "license", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "file_name", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("images", "coco_url", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("images", "height", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "width", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "date_captured", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("images", "flickr_url", LogicalType::VARCHAR);

	// (_img, id, li, fn, cu, h, w, dc, fu)
	// width >= 600 and width <= 800
	vector<unique_ptr<Expression>> filter_exprs;
	{
		unique_ptr<Expression> filter_expr1;
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, 
							make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 6),
							make_unique<BoundConstantExpression>(Value::BIGINT( 600 ))
						);
		unique_ptr<Expression> filter_expr2;
		filter_expr2 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, 
							make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 6),
							make_unique<BoundConstantExpression>(Value::BIGINT( 800 ))
						);
		filter_exprs.push_back(move(filter_expr1));
	}

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("IMAGES"),
		PropertyKeys({"id", "license", "file_name", "coco_url", "height", "width", "date_captured", "flickr_url"})
	));
	//ops
	ops.push_back( new PhysicalFilter(sch1, move(filter_exprs)));	// width >= 600 and width <= 800
	ops.push_back( new PhysicalTop(sch1, (idx_t) 100, (idx_t)0)); // offset 0 limit 100
	//sink
	ops.push_back( new PhysicalProduceResults(sch1));
	
	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


}