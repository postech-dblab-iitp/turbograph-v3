//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/enums/tableref_type.hpp"
#include "parser/parsed_data/sample_options.hpp"

namespace s62 {

class BoundTableRef {
public:
	explicit BoundTableRef(TableReferenceType type) : type(type) {
	}
	virtual ~BoundTableRef() {
	}

	//! The type of table reference
	TableReferenceType type;
	//! The sample options (if any)
	unique_ptr<SampleOptions> sample;
};
} // namespace s62
