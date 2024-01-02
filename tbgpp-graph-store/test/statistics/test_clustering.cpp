#include "statistics/clustering/clique.hpp"

int main() {
    CliqueClustering clique_clustering;
    
    uint64_t num_histograms = 10;
    uint64_t num_bins = 2;
    vector<uint64_t> histogram = {1,2, 2,2, 3,3, 4,4, 100,222, 6,6, 7,7, 8,8, 9,9, 100,100};

    clique_clustering.run(num_histograms, num_bins, histogram);

    // print num groups and group ids
    cout << "num groups: " << clique_clustering.num_groups << endl;
    for (uint64_t i = 0; i < clique_clustering.group_info.size(); i++) {
        cout << "group id: " << clique_clustering.group_info[i] << endl;
    }
    return 0;
}