//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/capi_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "s62.h"
#include "s62.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include "common/case_insensitive_map.hpp"

#include <cstring>
#include <cassert>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

namespace duckdb {

struct PreparedStatementWrapper {
	//! Map of name -> values
	case_insensitive_map_t<Value> values;
	unique_ptr<CypherPreparedStatement> statement;
};


} // namespace duckdb