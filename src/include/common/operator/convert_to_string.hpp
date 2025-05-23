//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/convert_to_string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types.hpp"
#include "common/exception.hpp"

namespace duckdb {

struct ConvertToString {
	template <class SRC>
	static inline string Operation(SRC input) {
		throw InternalException("Unrecognized type for ConvertToString %s", GetTypeId<SRC>());
	}
};

template <>
string ConvertToString::Operation(bool input);
template <>
string ConvertToString::Operation(int8_t input);
template <>
string ConvertToString::Operation(int16_t input);
template <>
string ConvertToString::Operation(int32_t input);
template <>
string ConvertToString::Operation(int64_t input);
template <>
string ConvertToString::Operation(uint8_t input);
template <>
string ConvertToString::Operation(uint16_t input);
template <>
string ConvertToString::Operation(uint32_t input);
template <>
string ConvertToString::Operation(uint64_t input);
template <>
string ConvertToString::Operation(hugeint_t input);
template <>
string ConvertToString::Operation(float input);
template <>
string ConvertToString::Operation(double input);
template <>
string ConvertToString::Operation(interval_t input);
template <>
string ConvertToString::Operation(date_t input);
template <>
string ConvertToString::Operation(dtime_t input);
template <>
string ConvertToString::Operation(timestamp_t input);
template <>
string ConvertToString::Operation(string_t input);

} // namespace duckdb
