#pragma once

#include <vector>
#include "common/exception.hpp"

using namespace std;
using namespace duckdb;

class Clustering {
public:
    Clustering() {}

    void initialize() {
        num_groups = 0;
        group_info.clear();
    }

    void run(uint64_t, uint64_t, const vector<uint64_t>&) {
        throw InternalException("run is not implemented");
    }    

public:
    uint64_t num_groups;
    vector<uint64_t> group_info;
};