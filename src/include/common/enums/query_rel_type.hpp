#pragma once

#include <cstdint>
#include <memory>

namespace duckdb {

enum class QueryRelType : uint8_t {
    NON_RECURSIVE = 0,
    VARIABLE_LENGTH_WALK = 1,
    VARIABLE_LENGTH_TRAIL = 2,
    VARIABLE_LENGTH_ACYCLIC = 3,
    SHORTEST = 4,
    ALL_SHORTEST = 5,
    WEIGHTED_SHORTEST = 6,
    ALL_WEIGHTED_SHORTEST = 7,
};

}