//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/set_operation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

enum class SetOperationType : uint8_t { NONE = 0, UNION = 1, EXCEPT = 2, INTERSECT = 3 };
}
