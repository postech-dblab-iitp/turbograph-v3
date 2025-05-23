#include "function/scalar/string_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

static int64_t MismatchesScalarFunction(Vector &result, const string_t str, string_t tgt) {
	idx_t str_len = str.GetSize();
	idx_t tgt_len = tgt.GetSize();

	if (str_len != tgt_len) {
		throw InvalidInputException("Mismatch Function: Strings must be of equal length!");
	}
	if (str_len < 1) {
		throw InvalidInputException("Mismatch Function: Strings must be of length > 0!");
	}

	idx_t mismatches = 0;
	auto str_str = str.GetDataUnsafe();
	auto tgt_str = tgt.GetDataUnsafe();

	for (idx_t idx = 0; idx < str_len; ++idx) {
		if (str_str[idx] != tgt_str[idx]) {
			mismatches++;
		}
	}
	return (int64_t)mismatches;
}

static void MismatchesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return MismatchesScalarFunction(result, str, tgt); });
}

void MismatchesFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet mismatches("mismatches");
	mismatches.AddFunction(ScalarFunction("mismatches", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      LogicalType::BIGINT,
	                                      MismatchesFunction)); // Pointer to function implementation
	set.AddFunction(mismatches);

	ScalarFunctionSet hamming("hamming");
	hamming.AddFunction(ScalarFunction("mismatches", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                   MismatchesFunction)); // Pointer to function implementation
	set.AddFunction(hamming);
}

} // namespace duckdb
