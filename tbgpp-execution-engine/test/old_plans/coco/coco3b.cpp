#include "plans/query_plan_suite.hpp"

//  --nodes:IMAGES images --nodes:CATEGORIES categories --nodes:ANNOTATIONS annotations --nodes:LICENSES licenses

namespace duckdb {

CypherPipelineExecutor* coco3b_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::COCO_Q3B() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = coco3b_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* coco3b_pipe1(QueryPlanSuite& suite) {

	Schema sch1;
	sch1.addNode("annotations");
	sch1.addPropertyIntoNode("annotations", "category_id", LogicalType::BIGINT);

	// (_id, category_id)
	// catid=144 or catid =140
	vector<unique_ptr<Expression>> filter_exprs;
	{
		unique_ptr<Expression> filter_expr1;
		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::CONJUNCTION_OR,
							make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL,
								make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1),
								make_unique<BoundConstantExpression>(Value::BIGINT( 144 ))
							),
							make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL,
								make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 1),
								make_unique<BoundConstantExpression>(Value::BIGINT( 140 ))
							)
						);
		filter_exprs.push_back(move(filter_expr1));
	}

	// adjidxjoin
	Schema sch2 = sch1;
	sch2.addNode("images");

	// attach properties
	Schema sch3 = sch2;
	sch3.addPropertyIntoNode("images", "id", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("images", "license", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("images", "file_name", LogicalType::VARCHAR);
	sch3.addPropertyIntoNode("images", "coco_url", LogicalType::VARCHAR);
	sch3.addPropertyIntoNode("images", "height", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("images", "width", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("images", "date_captured", LogicalType::VARCHAR);
	sch3.addPropertyIntoNode("images", "flickr_url", LogicalType::VARCHAR);
	PropertyKeys image_keys = PropertyKeys({"id", "license", "file_name", "coco_url", "height", "width", "date_captured", "flickr_url"});

	// projection images(_id, id, license, .....)
	Schema sch4;
	sch4.addNode("images");
	sch4.addPropertyIntoNode("images", "id", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "license", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "file_name", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("images", "coco_url", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("images", "height", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "width", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "date_captured", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("images", "flickr_url", LogicalType::VARCHAR);

	// (_anno, category_id, _images, id, li, ...)
	vector<unique_ptr<Expression>> proj_exprs;
	{
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 3) ) );	// id
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 4) ) );	// li
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 5) ) );	// fn
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6) ) );	// cu
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 7) ) );	// h
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 8) ) );	// w
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 9) ) );	// dc
		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 10) ) );	// fu
	}
	
	std::vector<CypherPhysicalOperator *> ops;
//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("ANNOTATIONS"), PropertyKeys({"category_id"}) ));
//ops
	ops.push_back( new PhysicalFilter(sch1, move(filter_exprs)));	// or...
	// annotations -> images
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "annotations", LabelSet("ANNOTATIONS"), LabelSet("ANNOTATION_IMAGE"), ExpandDirection::OUTGOING, LabelSet("IMAGES"),  JoinType::INNER, false, true));
	// attach properties
	ops.push_back( new PhysicalNodeIdSeek(sch3, "images", LabelSet("IMAGES"), image_keys ));
	// projection
	ops.push_back( new PhysicalProjection(sch4, move(proj_exprs)));
	// limit
	ops.push_back( new PhysicalTop(sch4, (idx_t) 100, (idx_t)0)); // offset 0 limit 100
//sink
	ops.push_back( new PhysicalProduceResults(sch4));
	
	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


}