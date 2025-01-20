#include "statistics/clustering/clique.hpp"

int main() {
    CliqueClustering clique_clustering;
    
    uint64_t num_histograms = 10;
    uint64_t num_bins = 4;
    // 254 246 234 265 283 279 232 205 251 281 277 191 267 248 243 242 244 229 281 246 244 271 272 213 265 257 254 224 245 249 254 252 255 224 215 302 248 232 253 267
    // 0 0 0 0 246 262 245 245 0 0 0 0 237 252 274 237 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 255 245 233 265 0 0 0 0
    //vector<uint64_t> histogram = {0,0,0,0,246,262,245,245,0,0,0,0,237,252,274,237,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,255,245,233,265,0,0,0,0};
    vector<uint64_t> histogram = {254, 246, 234, 265, 283, 279, 232, 205, 251, 281, 277, 191, 267, 248, 243, 242, 244, 229, 281, 246, 244, 271, 272, 213, 265, 257, 254, 224, 245, 249, 254, 252, 255, 224, 215, 302, 248, 232, 253, 267};

    clique_clustering.run(num_histograms, num_bins, histogram);

    // print num groups and group ids
    cout << "num groups: " << clique_clustering.num_groups << endl;
    for (uint64_t i = 0; i < clique_clustering.group_info.size(); i++) {
        cout << "group id: " << clique_clustering.group_info[i] << endl;
    }
    return 0;
}