#pragma once

#include "common/typedefs.hpp"
#include "common/types/value.hpp"


namespace duckdb {
	
enum class FilterPushdownType: uint8_t {
	FP_EQ,
	FP_RANGE,
	FP_COMPLEX
};

struct RangeFilterValue {
	Value l_value;
	Value r_value;
	bool l_inclusive;
	bool r_inclusive;
};

typedef vector<int64_t> FilterKeyIdxs;
typedef vector<Value> EQFilterValues;
typedef vector<RangeFilterValue> RangeFilterValues;

}  // namespace duckdb
