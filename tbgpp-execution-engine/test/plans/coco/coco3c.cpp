#include "plans/query_plan_suite.hpp"

//  --nodes:IMAGES images --nodes:CATEGORIES categories --nodes:ANNOTATIONS annotations --nodes:LICENSES licenses

namespace duckdb {

CypherPipelineExecutor* coco3c_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::COCO_Q3C() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = coco3c_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* coco3c_pipe1(QueryPlanSuite& suite) {

	CypherSchema sch1;
	sch1.addNode("categories");
	CypherSchema sch2 = sch1;
	sch2.addNode("annotations");
	CypherSchema sch3 = sch2;	// _c _a _i
	sch3.addNode("images");
	CypherSchema sch4 = sch3;
	sch4.addPropertyIntoNode("images", "id", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "license", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "file_name", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("images", "coco_url", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("images", "height", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "width", LogicalType::BIGINT);
	sch4.addPropertyIntoNode("images", "date_captured", LogicalType::VARCHAR);
	sch4.addPropertyIntoNode("images", "flickr_url", LogicalType::VARCHAR);
	PropertyKeys image_keys = PropertyKeys({"id", "license", "file_name", "coco_url", "height", "width", "date_captured", "flickr_url"});

	std::vector<CypherPhysicalOperator *> ops;
//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("CATEGORIES"), PropertyKeys({}), "name", Value("fruit") ) );	// cat name == fruit
// ops
	// categories <- annotations (incoming)
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "categories", LabelSet("CATEGORIES"), LabelSet("ANNOTATION_CATEGORY_BACKWARD"), ExpandDirection::OUTGOING, LabelSet("CATEGORIES"), JoinType::INNER, false, true));
	// annotations -> images (outgoing)
	ops.push_back( new PhysicalAdjIdxJoin(sch3, "annotations", LabelSet("ANNOTATIONS"), LabelSet("ANNOTATION_IMAGE"), ExpandDirection::OUTGOING, LabelSet("IMAGES"), JoinType::INNER, false, true));
	// fetch image properties
	ops.push_back( new PhysicalNodeIdSeek(sch4, "images", LabelSet("IMAGES"), image_keys));
	// limit 100
	ops.push_back( new PhysicalTop(sch4, (idx_t) 100, (idx_t)0)); // offset 0 limit 100
// sink
	ops.push_back( new PhysicalProduceResults(sch4));
	
	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


}