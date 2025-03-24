#pragma once

#include "planner/query/graph_pattern/bound_pattern.hpp"

namespace duckdb {

class BoundNodePattern : public BoundPattern {
public:
    BoundNodePattern() = default;
};

}