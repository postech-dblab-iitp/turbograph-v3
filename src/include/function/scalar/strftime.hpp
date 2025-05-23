//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/strftime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"
#include "common/vector.hpp"

namespace duckdb {

enum class StrTimeSpecifier : uint8_t {
	ABBREVIATED_WEEKDAY_NAME = 0,    // %a - Abbreviated weekday name. (Sun, Mon, ...)
	FULL_WEEKDAY_NAME = 1,           // %A Full weekday name. (Sunday, Monday, ...)
	WEEKDAY_DECIMAL = 2,             // %w - Weekday as a decimal number. (0, 1, ..., 6)
	DAY_OF_MONTH_PADDED = 3,         // %d - Day of the month as a zero-padded decimal. (01, 02, ..., 31)
	DAY_OF_MONTH = 4,                // %-d - Day of the month as a decimal number. (1, 2, ..., 30)
	ABBREVIATED_MONTH_NAME = 5,      // %b - Abbreviated month name. (Jan, Feb, ..., Dec)
	FULL_MONTH_NAME = 6,             // %B - Full month name. (January, February, ...)
	MONTH_DECIMAL_PADDED = 7,        // %m - Month as a zero-padded decimal number. (01, 02, ..., 12)
	MONTH_DECIMAL = 8,               // %-m - Month as a decimal number. (1, 2, ..., 12)
	YEAR_WITHOUT_CENTURY_PADDED = 9, // %y - Year without century as a zero-padded decimal number. (00, 01, ..., 99)
	YEAR_WITHOUT_CENTURY = 10,       // %-y - Year without century as a decimal number. (0, 1, ..., 99)
	YEAR_DECIMAL = 11,               // %Y - Year with century as a decimal number. (2013, 2019 etc.)
	HOUR_24_PADDED = 12,             // %H - Hour (24-hour clock) as a zero-padded decimal number. (00, 01, ..., 23)
	HOUR_24_DECIMAL = 13,            // %-H - Hour (24-hour clock) as a decimal number. (0, 1, ..., 23)
	HOUR_12_PADDED = 14,             // %I - Hour (12-hour clock) as a zero-padded decimal number. (01, 02, ..., 12)
	HOUR_12_DECIMAL = 15,            // %-I - Hour (12-hour clock) as a decimal number. (1, 2, ... 12)
	AM_PM = 16,                      // %p - Locale’s AM or PM. (AM, PM)
	MINUTE_PADDED = 17,              // %M - Minute as a zero-padded decimal number. (00, 01, ..., 59)
	MINUTE_DECIMAL = 18,             // %-M - Minute as a decimal number. (0, 1, ..., 59)
	SECOND_PADDED = 19,              // %S - Second as a zero-padded decimal number. (00, 01, ..., 59)
	SECOND_DECIMAL = 20,             // %-S - Second as a decimal number. (0, 1, ..., 59)
	MICROSECOND_PADDED = 21,         // %f - Microsecond as a decimal number, zero-padded on the left. (000000 - 999999)
	MILLISECOND_PADDED = 22,         // %g - Millisecond as a decimal number, zero-padded on the left. (000 - 999)
	UTC_OFFSET = 23,                 // %z - UTC offset in the form +HHMM or -HHMM. ( )
	TZ_NAME = 24,                    // %Z - Time zone name. ( )
	DAY_OF_YEAR_PADDED = 25,         // %j - Day of the year as a zero-padded decimal number. (001, 002, ..., 366)
	DAY_OF_YEAR_DECIMAL = 26,        // %-j - Day of the year as a decimal number. (1, 2, ..., 366)
	WEEK_NUMBER_PADDED_SUN_FIRST =
	    27, // %U - Week number of the year (Sunday as the first day of the week). All days in a new year preceding the
	        // first Sunday are considered to be in week 0. (00, 01, ..., 53)
	WEEK_NUMBER_PADDED_MON_FIRST =
	    28, // %W - Week number of the year (Monday as the first day of the week). All days in a new year preceding the
	        // first Monday are considered to be in week 0. (00, 01, ..., 53)
	LOCALE_APPROPRIATE_DATE_AND_TIME =
	    29, // %c - Locale’s appropriate date and time representation. (Mon Sep 30 07:06:05 2013)
	LOCALE_APPROPRIATE_DATE = 30, // %x - Locale’s appropriate date representation. (09/30/13)
	LOCALE_APPROPRIATE_TIME = 31  // %X - Locale’s appropriate time representation. (07:06:05)
};

struct StrTimeFormat {
public:
	virtual ~StrTimeFormat() {
	}

	static string ParseFormatSpecifier(const string &format_string, StrTimeFormat &format);

protected:
	//! The format specifiers
	vector<StrTimeSpecifier> specifiers;
	//! The literals that appear in between the format specifiers
	//! The following must hold: literals.size() = specifiers.size() + 1
	//! Format is literals[0], specifiers[0], literals[1], ..., specifiers[n - 1], literals[n]
	vector<string> literals;
	//! The constant size that appears in the format string
	idx_t constant_size;
	//! The max numeric width of the specifier (if it is parsed as a number), or -1 if it is not a number
	vector<int> numeric_width;

protected:
	void AddLiteral(string literal);
	virtual void AddFormatSpecifier(string preceding_literal, StrTimeSpecifier specifier);
};

struct StrfTimeFormat : public StrTimeFormat {
	idx_t GetLength(date_t date, dtime_t time);

	void FormatString(date_t date, int32_t data[7], char *target);
	void FormatString(date_t date, dtime_t time, char *target);

	DUCKDB_API static string Format(timestamp_t timestamp, const string &format);

protected:
	//! The variable-length specifiers. To determine total string size, these need to be checked.
	vector<StrTimeSpecifier> var_length_specifiers;
	//! Whether or not the current specifier is a special "date" specifier (i.e. one that requires a date_t object to
	//! generate)
	vector<bool> is_date_specifier;

protected:
	void AddFormatSpecifier(string preceding_literal, StrTimeSpecifier specifier) override;
	static idx_t GetSpecifierLength(StrTimeSpecifier specifier, date_t date, dtime_t time);
	char *WriteString(char *target, const string_t &str);
	char *Write2(char *target, uint8_t value);
	char *WritePadded2(char *target, uint32_t value);
	char *WritePadded3(char *target, uint32_t value);
	char *WritePadded(char *target, uint32_t value, size_t padding);
	bool IsDateSpecifier(StrTimeSpecifier specifier);
	char *WriteDateSpecifier(StrTimeSpecifier specifier, date_t date, char *target);
	char *WriteStandardSpecifier(StrTimeSpecifier specifier, int32_t data[], char *target);
};

struct StrpTimeFormat : public StrTimeFormat {
public:
	//! Type-safe parsing argument
	struct ParseResult {
		int32_t data[7];
		string error_message;
		idx_t error_position = DConstants::INVALID_INDEX;

		date_t ToDate();
		timestamp_t ToTimestamp();
		string FormatError(string_t input, const string &format_specifier);
	};

public:
	//! The full format specifier, for error messages
	string format_specifier;

public:
	DUCKDB_API static ParseResult Parse(const string &format, const string &text);

	bool Parse(string_t str, ParseResult &result);

	bool TryParseDate(string_t str, date_t &result, string &error_message);
	bool TryParseTimestamp(string_t str, timestamp_t &result, string &error_message);

	date_t ParseDate(string_t str);
	timestamp_t ParseTimestamp(string_t str);

protected:
	static string FormatStrpTimeError(const string &input, idx_t position);
	void AddFormatSpecifier(string preceding_literal, StrTimeSpecifier specifier) override;
	int NumericSpecifierWidth(StrTimeSpecifier specifier);
	int32_t TryParseCollection(const char *data, idx_t &pos, idx_t size, const string_t collection[],
	                           idx_t collection_count);
};

} // namespace duckdb
