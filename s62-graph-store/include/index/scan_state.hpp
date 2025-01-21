//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace s62 {
class ColumnSegment;
class LocalTableStorage;
class Index;
class RowGroup;
class UpdateSegment;
class TableScanState;
class ColumnSegment;
class ValiditySegment;
class TableFilterSet;

struct IndexScanState {
	virtual ~IndexScanState() {
	}
};

} // namespace s62
