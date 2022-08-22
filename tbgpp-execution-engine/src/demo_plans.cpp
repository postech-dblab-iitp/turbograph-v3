

#include "demo_plans.hpp"

#include "main/database.hpp"
#include "main/client_context.hpp"
//#include "parallel/pipeline.hpp"
//#include "execution/executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/node_scan.hpp"
#include "execution/physical_operator/physical_dummy_operator.hpp"
#include "execution/physical_operator/produce_results.hpp"
#include "execution/physical_operator/naive_expand.hpp"
#include "execution/physical_operator/simple_filter.hpp"
#include "execution/physical_operator/simple_projection.hpp"
#include "execution/physical_operator/edge_fetch.hpp"
//#include "execution/physical_operator/projection.hpp"
#include "execution/physical_operator/limit.hpp"

#include "storage/graph_store.hpp"

#include "typedef.hpp"
#include <vector>

#include <iostream>

namespace duckdb {

QueryPlanSuite::QueryPlanSuite(ClientContext &context): context(context) {  }

}