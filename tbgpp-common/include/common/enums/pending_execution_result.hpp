//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/pending_execution_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace s62 {

enum class PendingExecutionResult : uint8_t { RESULT_READY, RESULT_NOT_READY, EXECUTION_ERROR };

} // namespace s62
