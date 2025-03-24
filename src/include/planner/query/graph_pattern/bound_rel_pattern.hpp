#pragma once

#include "planner/query/graph_pattern/bound_pattern.hpp"

namespace duckdb {

class BoundRelPattern : public BoundPattern {
public:
    BoundRelPattern() = default;
};

}