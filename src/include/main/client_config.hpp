//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/value.hpp"
#include "common/enums/output_type.hpp"
#include "common/case_insensitive_map.hpp"
// #include "common/enums/profiler_format.hpp"
#include "main/client_context.hpp"

namespace duckdb {
class ClientContext;


struct ClientConfig {
	//! If the query profiler is enabled or not.
	bool enable_profiler = false;

	//! If detailed query profiling is enabled
	// bool enable_detailed_profiling = false;
	//! The format to automatically print query profiling information in (default: disabled)
	// ProfilerPrintFormat profiler_print_format = ProfilerPrintFormat::NONE;
	//! The file to save query profiling information to, instead of printing it to the console
	//! (empty = print to console)
	// string profiler_save_location;

	// //! If the progress bar is enabled or not.
	// bool enable_progress_bar = false;
	// //! If the print of the progress bar is enabled
	// bool print_progress_bar = true;
	// //! The wait time before showing the progress bar
	// int wait_time = 2000;

	// //! Preserve identifier case while parsing.
	// //! If false, all unquoted identifiers are lower-cased (e.g. "MyTable" -> "mytable").
	// bool preserve_identifier_case = true;

	// // Whether or not aggressive query verification is enabled
	// bool query_verification_enabled = false;
	// //! Enable the running of optimizers
	// bool enable_optimizer = true;
	// //! Force parallelism of small tables, used for testing
	// bool verify_parallelism = false;
	// //! Force index join independent of table cardinality, used for testing
	// bool force_index_join = false;
	//! Force out-of-core computation for operators that support it, used for testing
	bool force_external = false;
	// //! Maximum bits allowed for using a perfect hash table (i.e. the perfect HT can hold up to 2^perfect_ht_threshold
	// //! elements)
	// idx_t perfect_ht_threshold = 12;

	// //! The explain output type used when none is specified (default: PHYSICAL_ONLY)
	// ExplainOutputType explain_output_type = ExplainOutputType::PHYSICAL_ONLY;

	// //! Generic options
	// case_insensitive_map_t<Value> set_variables;

public:
	static ClientConfig &GetConfig(ClientContext &context);
};

} // namespace duckdb
