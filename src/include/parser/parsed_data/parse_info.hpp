//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/parse_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

struct ParseInfo {
	virtual ~ParseInfo() {
	}
};

} // namespace duckdb
