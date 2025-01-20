//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_parameter_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "common/unordered_map.hpp"

namespace s62 {

struct BoundParameterData {
	BoundParameterData() {
	}
	BoundParameterData(Value val) : value(move(val)), return_type(value.type()) {
	}

	Value value;
	LogicalType return_type;
};

using bound_parameter_map_t = unordered_map<idx_t, shared_ptr<BoundParameterData>>;

struct BoundParameterMap {
	BoundParameterMap(vector<BoundParameterData> &parameter_data) : parameter_data(parameter_data) {
	}

	bound_parameter_map_t parameters;
	vector<BoundParameterData> &parameter_data;

	LogicalType GetReturnType(idx_t index) {
		if (index >= parameter_data.size()) {
			return LogicalTypeId::UNKNOWN;
		}
		return parameter_data[index].return_type;
	}
};

} // namespace s62
