#include "plans/query_plan_suite.hpp"

//  --nodes:IMAGES images --nodes:CATEGORIES categories --nodes:ANNOTATIONS annotations --nodes:LICENSES licenses

namespace duckdb {

CypherPipelineExecutor* coco2_pipe1(QueryPlanSuite& suite);

std::vector<CypherPipelineExecutor*> QueryPlanSuite::COCO_Q2() {

	std::vector<CypherPipelineExecutor*> result;
	auto p1 = coco2_pipe1(*this);
	result.push_back(p1);
	return result;
}

CypherPipelineExecutor* coco2_pipe1(QueryPlanSuite& suite) {

	Schema sch1;
	// images property
	sch1.addNode("images");
	sch1.addPropertyIntoNode("images", "id", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "license", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "file_name", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("images", "coco_url", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("images", "height", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "width", LogicalType::BIGINT);
	sch1.addPropertyIntoNode("images", "date_captured", LogicalType::VARCHAR);
	sch1.addPropertyIntoNode("images", "flickr_url", LogicalType::VARCHAR);
	PropertyKeys image_keys = PropertyKeys({"id", "license", "file_name", "coco_url", "height", "width", "date_captured", "flickr_url"});

	Schema sch2 = sch1;
	sch2.addNode("annotations");
	
	// annotations property
	Schema sch3 = sch2;
	sch3.addPropertyIntoNode("annotations", "id", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("annotations", "segmentation", LogicalType::LIST(LogicalType::DOUBLE));
	sch3.addPropertyIntoNode("annotations", "area", LogicalType::DOUBLE);
	sch3.addPropertyIntoNode("annotations", "iscrowd", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("annotations", "image_id", LogicalType::BIGINT);
	sch3.addPropertyIntoNode("annotations", "bbox", LogicalType::LIST(LogicalType::DOUBLE));
	sch3.addPropertyIntoNode("annotations", "category_id", LogicalType::BIGINT);
	PropertyKeys annotation_keys = PropertyKeys({"id", "segmentation", "area", "iscrowd", "image_id", "bbox", "category_id"});
	// PropertyKeys annotation_keys = PropertyKeys({"id", "area", "iscrowd", "image_id", "category_id"});

	Schema sch4 = sch3;
	sch4.addNode("categories");

	Schema sch5 = sch4;
	sch5.addPropertyIntoNode("categories", "id", LogicalType::BIGINT);
	sch5.addPropertyIntoNode("categories", "name", LogicalType::VARCHAR);
	sch5.addPropertyIntoNode("categories", "supercategory", LogicalType::VARCHAR);
	PropertyKeys category_keys = PropertyKeys({"id", "name", "supercategory"});

	Schema sch6 = sch5;
	sch6.addNode("licenses");

	Schema sch7 = sch6;
	sch7.addPropertyIntoNode("licenses", "id", LogicalType::BIGINT);
	sch7.addPropertyIntoNode("licenses", "name", LogicalType::VARCHAR);
	sch7.addPropertyIntoNode("licenses", "url", LogicalType::VARCHAR);
	PropertyKeys license_keys = PropertyKeys({"id", "name", "url"});
	

	std::vector<CypherPhysicalOperator *> ops;
	//src
	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("IMAGES"), image_keys,
		"id", duckdb::Value::BIGINT(1)	  // predicate images.id = 1
	));
	//ops
	// images <- annotations (incoming)
	ops.push_back( new PhysicalAdjIdxJoin(sch2, "images", LabelSet("IMAGES"), LabelSet("ANNOTATION_IMAGE_BACKWARD"), ExpandDirection::OUTGOING, LabelSet("ANNOTATIONS"), JoinType::INNER, false, true));
	// annotations properties
	ops.push_back( new PhysicalNodeIdSeek(sch3, "annotations", LabelSet("ANNOTATIONS"), annotation_keys));
	// annotations -> categories (outgoing)
	ops.push_back( new PhysicalAdjIdxJoin(sch4, "annotations", LabelSet("ANNOTATIONS"), LabelSet("ANNOTATION_CATEGORY"), ExpandDirection::OUTGOING, LabelSet("CATEGORIES"), JoinType::INNER, false, true));
	// categories properties 
	ops.push_back( new PhysicalNodeIdSeek(sch5, "categories", LabelSet("CATEGORIES"), category_keys));
	// images -> licenses (outgoing)
	ops.push_back( new PhysicalAdjIdxJoin(sch6, "images", LabelSet("IMAGES"), LabelSet("IMAGE_LICENSE"), ExpandDirection::OUTGOING, LabelSet("LICENSES"), JoinType::INNER, false, true));
	// licenses properties
	ops.push_back( new PhysicalNodeIdSeek(sch7, "licenses", LabelSet("LICENSES"), license_keys));
	//sink
	ops.push_back( new PhysicalProduceResults(sch7));

	auto pipe = new CypherPipeline(ops);
	auto ctx = new ExecutionContext(&(suite.context));
	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
	return pipeexec;
}


}