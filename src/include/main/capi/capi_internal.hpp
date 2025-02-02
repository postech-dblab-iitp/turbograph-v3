//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/api/c-api/capi_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "main/capi/s62.h"
#include "main/capi/s62.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include "common/case_insensitive_map.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "nlohmann/json.hpp"
#include <cstring>
#include <cassert>
#include <string>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

using json = nlohmann::json;

namespace duckdb {
	
s62_type ConvertCPPTypeToC(const LogicalType &type);
LogicalTypeId ConvertCTypeToCPP(s62_type c_type);
idx_t GetCTypeSize(s62_type type);
std::string generatePostgresStylePlan(std::vector<CypherPipelineExecutor*>& executors, bool is_executed = false);

} // namespace duckdb