#pragma once

#include "storage/statistics/clustering/clustering.hpp"

class DummyClustering : public Clustering {
public:
    DummyClustering() {}

    void run(uint64_t num_histograms, uint64_t num_buckets, const vector<uint64_t>& frequency_values) {
        num_groups = 1;
        group_info.resize(num_histograms);
        for (uint64_t i = 0; i < num_histograms; i++) {
            group_info[i] = 0;
        }
    }

};