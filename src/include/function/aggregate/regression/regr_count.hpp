//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/regression/regr_count.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"
#include "function/aggregate/algebraic_functions.hpp"
#include "function/aggregate/algebraic/covar.hpp"
#include "function/aggregate/algebraic/stddev.hpp"

namespace duckdb {

struct RegrCountFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		*state = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		*target += source;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		target[idx] = *state;
	}
	static bool IgnoreNull() {
		return true;
	}
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		*state += 1;
	}
};

} // namespace duckdb
