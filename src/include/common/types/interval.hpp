//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/interval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.hpp"

namespace duckdb {

//! The Interval class is a static class that holds helper functions for the Interval
//! type.
class Interval {
public:
	static constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
	static constexpr const int32_t MONTHS_PER_CENTURY = 1200;
	static constexpr const int32_t MONTHS_PER_DECADE = 120;
	static constexpr const int32_t MONTHS_PER_YEAR = 12;
	static constexpr const int32_t MONTHS_PER_QUARTER = 3;
	static constexpr const int32_t DAYS_PER_WEEK = 7;
	//! only used for interval comparison/ordering purposes, in which case a month counts as 30 days
	static constexpr const int64_t DAYS_PER_MONTH = 30;
	static constexpr const int64_t DAYS_PER_YEAR = 365;
	static constexpr const int64_t MSECS_PER_SEC = 1000;
	static constexpr const int32_t SECS_PER_MINUTE = 60;
	static constexpr const int32_t MINS_PER_HOUR = 60;
	static constexpr const int32_t HOURS_PER_DAY = 24;
	static constexpr const int32_t SECS_PER_HOUR = SECS_PER_MINUTE * MINS_PER_HOUR;
	static constexpr const int32_t SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;
	static constexpr const int32_t SECS_PER_WEEK = SECS_PER_DAY * DAYS_PER_WEEK;

	static constexpr const int64_t MICROS_PER_MSEC = 1000;
	static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
	static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
	static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
	static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
	static constexpr const int64_t MICROS_PER_WEEK = MICROS_PER_DAY * DAYS_PER_WEEK;
	static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

	static constexpr const int64_t NANOS_PER_MICRO = 1000;
	static constexpr const int64_t NANOS_PER_MSEC = NANOS_PER_MICRO * MICROS_PER_MSEC;
	static constexpr const int64_t NANOS_PER_SEC = NANOS_PER_MSEC * MSECS_PER_SEC;
	static constexpr const int64_t NANOS_PER_MINUTE = NANOS_PER_SEC * SECS_PER_MINUTE;
	static constexpr const int64_t NANOS_PER_HOUR = NANOS_PER_MINUTE * MINS_PER_HOUR;
	static constexpr const int64_t NANOS_PER_DAY = NANOS_PER_HOUR * HOURS_PER_DAY;
	static constexpr const int64_t NANOS_PER_WEEK = NANOS_PER_DAY * DAYS_PER_WEEK;

public:
	//! Convert a string to an interval object
	static bool FromString(const string &str, interval_t &result);
	//! Convert a string to an interval object
	static bool FromCString(const char *str, idx_t len, interval_t &result, string *error_message, bool strict);
	//! Convert an interval object to a string
	static string ToString(const interval_t &val);

	//! Convert milliseconds to a normalised interval
	DUCKDB_API static interval_t FromMicro(int64_t micros);

	//! Get Interval in milliseconds
	static int64_t GetMilli(const interval_t &val);

	//! Get Interval in microseconds
	static int64_t GetMicro(const interval_t &val);

	//! Get Interval in Nanoseconds
	static int64_t GetNanoseconds(const interval_t &val);

	//! Returns the age between two timestamps (including 30 day months)
	static interval_t GetAge(timestamp_t timestamp_1, timestamp_t timestamp_2);

	//! Returns the exact difference between two timestamps (days and seconds)
	static interval_t GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2);

	//! Add an interval to a date
	static date_t Add(date_t left, interval_t right);
	//! Add an interval to a timestamp
	static timestamp_t Add(timestamp_t left, interval_t right);
	//! Add an interval to a time. In case the time overflows or underflows, modify the date by the overflow.
	//! For example if we go from 23:00 to 02:00, we add a day to the date
	static dtime_t Add(dtime_t left, interval_t right, date_t &date);

	//! Comparison operators
	static bool Equals(interval_t left, interval_t right);
	static bool GreaterThan(interval_t left, interval_t right);
	static bool GreaterThanEquals(interval_t left, interval_t right);
};
} // namespace duckdb
