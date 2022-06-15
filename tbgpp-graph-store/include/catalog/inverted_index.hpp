#pragma once

#include "common/common.hpp"
#include "common/unordered_map.hpp"
#include "common/vector.hpp"

namespace duckdb {

template <typename Key, typename Value>
using inverted_index_t =
    unordered_map<Key, vector<Value>>;

} // namespace duckdb
