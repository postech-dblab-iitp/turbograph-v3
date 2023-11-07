//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
//
//===----------------------------------------------------------------------===//

#pragma once


#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

//===--------------------------------------------------------------------===//
// Type Information
//===--------------------------------------------------------------------===//
typedef uint64_t idx_t;

typedef const char* s62_version;

typedef const char* Query;

typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	// bool
	DUCKDB_TYPE_BOOLEAN,
	// int8_t
	DUCKDB_TYPE_TINYINT,
	// int16_t
	DUCKDB_TYPE_SMALLINT,
	// int32_t
	DUCKDB_TYPE_INTEGER,
	// int64_t
	DUCKDB_TYPE_BIGINT,
	// uint8_t
	DUCKDB_TYPE_UTINYINT,
	// uint16_t
	DUCKDB_TYPE_USMALLINT,
	// uint32_t
	DUCKDB_TYPE_UINTEGER,
	// uint64_t
	DUCKDB_TYPE_UBIGINT,
	// float
	DUCKDB_TYPE_FLOAT,
	// double
	DUCKDB_TYPE_DOUBLE,
	// duckdb_timestamp, in microseconds
	DUCKDB_TYPE_TIMESTAMP,
	// duckdb_date
	DUCKDB_TYPE_DATE,
	// duckdb_time
	DUCKDB_TYPE_TIME,
	// duckdb_interval
	DUCKDB_TYPE_INTERVAL,
	// duckdb_hugeint
	DUCKDB_TYPE_HUGEINT,
	// const char*
	DUCKDB_TYPE_VARCHAR,
	// duckdb_blob
	DUCKDB_TYPE_BLOB,
	// decimal
	DUCKDB_TYPE_DECIMAL,
	// duckdb_timestamp, in seconds
	DUCKDB_TYPE_TIMESTAMP_S,
	// duckdb_timestamp, in milliseconds
	DUCKDB_TYPE_TIMESTAMP_MS,
	// duckdb_timestamp, in nanoseconds
	DUCKDB_TYPE_TIMESTAMP_NS,
	// enum type, only useful as logical type
	DUCKDB_TYPE_ENUM,
	// list type, only useful as logical type
	DUCKDB_TYPE_LIST,
	// struct type, only useful as logical type
	DUCKDB_TYPE_STRUCT,
	// map type, only useful as logical type
	DUCKDB_TYPE_MAP,
	// duckdb_hugeint
	DUCKDB_TYPE_UUID,
	// union type, only useful as logical type
	DUCKDB_TYPE_UNION,
	// duckdb_bit
	DUCKDB_TYPE_BIT,
} duckdb_type;

//! Days are stored as days since 1970-01-01
//! Use the duckdb_from_date/duckdb_to_date function to extract individual information
typedef struct {
	int32_t days;
} duckdb_date;

typedef struct {
	int32_t year;
	int8_t month;
	int8_t day;
} duckdb_date_struct;

//! Time is stored as microseconds since 00:00:00
//! Use the duckdb_from_time/duckdb_to_time function to extract individual information
typedef struct {
	int64_t micros;
} duckdb_time;

typedef struct {
	int8_t hour;
	int8_t min;
	int8_t sec;
	int32_t micros;
} duckdb_time_struct;

//! Timestamps are stored as microseconds since 1970-01-01
//! Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information
typedef struct {
	int64_t micros;
} duckdb_timestamp;

typedef struct {
	duckdb_date_struct date;
	duckdb_time_struct time;
} duckdb_timestamp_struct;

typedef struct {
	int32_t months;
	int32_t days;
	int64_t micros;
} duckdb_interval;

//! Hugeints are composed in a (lower, upper) component
//! The value of the hugeint is upper * 2^64 + lower
//! For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
typedef struct {
	uint64_t lower;
	int64_t upper;
} duckdb_hugeint;

typedef struct {
	uint8_t width;
	uint8_t scale;

	duckdb_hugeint value;
} duckdb_decimal;

typedef struct {
	char *data;
	idx_t size;
} duckdb_string;

/*
    The internal data representation of a VARCHAR/BLOB column
*/
typedef struct {
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
} duckdb_string_t;

typedef struct {
	void *data;
	idx_t size;
} duckdb_blob;

typedef struct {
	uint64_t offset;
	uint64_t length;
} duckdb_list_entry;

typedef struct {
#if DUCKDB_API_VERSION < DUCKDB_API_0_3_2
	void *data;
	bool *nullmask;
	duckdb_type type;
	char *name;
#else
	// deprecated, use duckdb_column_data
	void *__deprecated_data;
	// deprecated, use duckdb_nullmask_data
	bool *__deprecated_nullmask;
	// deprecated, use duckdb_column_type
	duckdb_type __deprecated_type;
	// deprecated, use duckdb_column_name
	char *__deprecated_name;
#endif
	void *internal_data;
} duckdb_column;

typedef struct {
#if DUCKDB_API_VERSION < DUCKDB_API_0_3_2
	idx_t column_count;
	idx_t row_count;
	idx_t rows_changed;
	duckdb_column *columns;
	char *error_message;
#else
	// deprecated, use duckdb_column_count
	idx_t __deprecated_column_count;
	// deprecated, use duckdb_row_count
	idx_t __deprecated_row_count;
	// deprecated, use duckdb_rows_changed
	idx_t __deprecated_rows_changed;
	// deprecated, use duckdb_column_ family of functions
	duckdb_column *__deprecated_columns;
	// deprecated, use duckdb_result_error
	char *__deprecated_error_message;
#endif
	void *internal_data;
} duckdb_result;

typedef struct _s62_prepared_statement {
	void *__prep;
} * s62_prepared_statement;

typedef struct _duckdb_extracted_statements {
	void *__extrac;
} * duckdb_extracted_statements;
typedef struct _duckdb_pending_result {
	void *__pend;
} * duckdb_pending_result;
typedef struct _duckdb_config {
	void *__cnfg;
} * duckdb_config;
typedef struct _duckdb_arrow_schema {
	void *__arrs;
} * duckdb_arrow_schema;
typedef struct _duckdb_arrow_array {
	void *__arra;
} * duckdb_arrow_array;
typedef struct _duckdb_logical_type {
	void *__lglt;
} * duckdb_logical_type;
typedef struct _duckdb_data_chunk {
	void *__dtck;
} * duckdb_data_chunk;
typedef struct _duckdb_vector {
	void *__vctr;
} * duckdb_vector;
typedef struct _duckdb_value {
	void *__val;
} * duckdb_value;

typedef enum {
    S62_SUCCESS = 0,
    S62_ERROR = 1,
} s62_state;

typedef enum {
    S62_NO_ERROR = 0,
    S62_ERROR_CONNECTION_FAILED = -1
} s62_error_code;

typedef enum {
	DUCKDB_PENDING_RESULT_READY = 0,
	DUCKDB_PENDING_RESULT_NOT_READY = 1,
	DUCKDB_PENDING_ERROR = 2,
	DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3
} duckdb_pending_state;


//===--------------------------------------------------------------------===//
// Open/Connect
//===--------------------------------------------------------------------===//

s62_state s62_connect(const char *dbname);

void s62_disconnect();

s62_state s62_is_connected();

s62_error_code s62_get_last_error(char *errmsg);

s62_version s62_get_version();


//===--------------------------------------------------------------------===//
// Query
//===--------------------------------------------------------------------===//

s62_prepared_statement* s62_prepare(Query query);

s62_state s62_bind_value(s62_prepared_statement*, int32_t param_idx, int value);

s62_state s62_execute(s62_prepared_statement* prep_query);

int s62_fetch(s62_prepared_statement* prep_query, int* value);

#ifdef __cplusplus
}
#endif