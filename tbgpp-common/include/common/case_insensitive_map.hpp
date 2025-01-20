//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/case_insensitive_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "common/unordered_set.hpp"
#include "common/string.hpp"
#include "common/string_util.hpp"

namespace s62 {

struct CaseInsensitiveStringHashFunction {
	uint64_t operator()(const string &str) const {
		std::hash<string> hasher;
		return hasher(StringUtil::Lower(str));
	}
};

struct CaseInsensitiveStringEquality {
	bool operator()(const string &a, const string &b) const {
		return StringUtil::Lower(a) == StringUtil::Lower(b);
	}
};

template <typename T>
using case_insensitive_map_t =
    unordered_map<string, T, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>;

using case_insensitive_set_t = unordered_set<string, CaseInsensitiveStringHashFunction, CaseInsensitiveStringEquality>;

} // namespace s62
