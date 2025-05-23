#include "function/scalar/string_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/string_util.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

// See: https://www.kdnuggets.com/2020/10/optimizing-levenshtein-distance-measuring-text-similarity.html
// And: Iterative 2-row algorithm: https://en.wikipedia.org/wiki/Levenshtein_distance
// Note: A first implementation using the array algorithm version resulted in an error raised by duckdb
// (too muach memory usage)

static idx_t LevenshteinDistance(const string_t &txt, const string_t &tgt) {
	auto txt_len = txt.GetSize();
	auto tgt_len = tgt.GetSize();

	if (txt_len < 1) {
		throw InvalidInputException("Levenshtein Function: 1st argument too short");
	}

	if (tgt_len < 1) {
		throw InvalidInputException("Levenshtein Function: 2nd argument too short");
	}

	auto txt_str = txt.GetDataUnsafe();
	auto tgt_str = tgt.GetDataUnsafe();

	// Create two working vectors
	std::vector<idx_t> distances0(tgt_len + 1, 0);
	std::vector<idx_t> distances1(tgt_len + 1, 0);

	idx_t cost_substitution = 0;
	idx_t cost_insertion = 0;
	idx_t cost_deletion = 0;

	// initialize distances0 vector
	// edit distance for an empty txt string is just the number of characters to delete from tgt
	for (idx_t pos_tgt = 0; pos_tgt <= tgt_len; pos_tgt++) {
		distances0[pos_tgt] = pos_tgt;
	}

	for (idx_t pos_txt = 0; pos_txt < txt_len; pos_txt++) {
		// calculate distances1 (current raw distances) from the previous row

		distances1[0] = pos_txt + 1;

		for (idx_t pos_tgt = 0; pos_tgt < tgt_len; pos_tgt++) {
			cost_deletion = distances0[pos_tgt + 1] + 1;
			cost_insertion = distances1[pos_tgt] + 1;
			cost_substitution = distances0[pos_tgt];

			if (txt_str[pos_txt] != tgt_str[pos_tgt]) {
				cost_substitution += 1;
			}

			distances1[pos_tgt + 1] = MinValue(cost_deletion, MinValue(cost_substitution, cost_insertion));
		}
		// copy distances1 (current row) to distances0 (previous row) for next iteration
		// since data in distances1 is always invalidated, a swap without copy is more efficient
		distances0 = distances1;
	}

	return distances0[tgt_len];
}

static int64_t LevenshteinScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (int64_t)LevenshteinDistance(str, tgt);
}

static void LevenshteinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return LevenshteinScalarFunction(result, str, tgt); });
}

void LevenshteinFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet levenshtein("levenshtein");
	levenshtein.AddFunction(ScalarFunction("levenshtein", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::BIGINT,
	                                       LevenshteinFunction)); // Pointer to function implementation
	set.AddFunction(levenshtein);

	ScalarFunctionSet editdist3("editdist3");
	editdist3.AddFunction(ScalarFunction("levenshtein", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                     LogicalType::BIGINT, LevenshteinFunction));
	set.AddFunction(editdist3);
}

} // namespace duckdb
