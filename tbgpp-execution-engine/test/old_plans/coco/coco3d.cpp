#include "plans/query_plan_suite.hpp"

//  --nodes:IMAGES images --nodes:CATEGORIES categories --nodes:ANNOTATIONS annotations --nodes:LICENSES licenses

namespace duckdb {

CypherPipelineExecutor* coco3d_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::COCO_Q3D() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = coco3d_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* coco3d_pipe1(QueryPlanSuite& suite) {

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

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("IMAGES"),
		PropertyKeys({"id", "license", "file_name", "coco_url", "height", "width", "date_captured", "flickr_url"}),
		"date_captured", duckdb::Value("1975-08-21")	// date_captured = "blabla"
	));
	//ops
	ops.push_back( new PhysicalTop(sch1, (idx_t) 1, (idx_t)0)); // offset 0 limit 1
	//sink
	ops.push_back( new PhysicalProduceResults(sch1));
	
	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


}