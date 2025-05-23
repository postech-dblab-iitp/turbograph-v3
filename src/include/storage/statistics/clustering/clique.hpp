#pragma once

#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cmath>
#include <numeric>
#include <set>
#include <map>
#include "storage/statistics/clustering/clustering.hpp"

using namespace std;

using DenseUnit = map<uint64_t, uint64_t>;

class Cluster {
public:
    size_t id;
    vector<DenseUnit> dense_units;
    set<uint64_t> dimensions;
    set<size_t> data_point_ids;

    Cluster(const DenseUnit& units, const set<uint64_t>& dims, const set<size_t>& ids)
        : dense_units({units}), dimensions(dims), data_point_ids(ids) {}

    Cluster(const vector<DenseUnit>& units, const set<uint64_t>& dims, const set<size_t>& ids)
        : dense_units(units), dimensions(dims), data_point_ids(ids) {}

    void printCluster() {
        cout << "Dense units: ";
        cout << "[";
        for (auto unit_it = dense_units.begin(); unit_it != dense_units.end(); ++unit_it) {
            if (unit_it != dense_units.begin()) {
                cout << " ";
            }
            cout << "{";
            for (auto pair_it = unit_it->begin(); pair_it != unit_it->end(); ++pair_it) {
                if (pair_it != unit_it->begin()) {
                    cout << " ";
                }
                cout << "(" << pair_it->first << ", " << pair_it->second << ")";
            }
            cout << "}";
        }
        cout << "]";
        cout << "\nDimensions: ";
        for (auto dim_it = dimensions.begin(); dim_it != dimensions.end(); ++dim_it) {
            if (dim_it != dimensions.begin()) {
                cout << " ";
            }
            cout << *dim_it;
        }
        cout << "\nCluster size: " << data_point_ids.size() << "\nData points: ";
        for (auto id_it = data_point_ids.begin(); id_it != data_point_ids.end(); ++id_it) {
            if (id_it != data_point_ids.begin()) {
                cout << " ";
            }
            cout << *id_it;
        }
        cout << "\n";
    }

};

class CliqueClustering: public Clustering {
public:
    CliqueClustering() {}

    void run(uint64_t num_histograms, uint64_t num_buckets, const vector<uint64_t>& frequency_values) {
        initialize();

        // Run the CLIQUE clustering algorithm
        uint64_t xsi = 3; // Set your xsi value
        double tau = 0.1; // Set your tau value

        vector<vector<double>> histograms;
        reshapeHistograms(num_histograms, num_buckets, frequency_values, histograms);

        // Normalize the data
        normalizeFeatures(histograms);

        // Run the CLIQUE clustering algorithm
        vector<Cluster> clusters;
        runClique(histograms, xsi, tau, clusters);

        // Filter clusters with final dimension
        vector<Cluster> finalClusters;
        filterFinalDimClusters(clusters, num_buckets, finalClusters);
        createUnclusteredDataPointsClusters(finalClusters, num_histograms);

        // Output to num_groups and group_info
        num_groups = finalClusters.size();
        group_info.resize(num_histograms);
        for (size_t i = 0; i < finalClusters.size(); ++i) {
            for (const auto& id : finalClusters[i].data_point_ids) {
                group_info[id] = i;
            }
        } 
    }

private:
    void reshapeHistograms(uint64_t num_histograms, uint64_t num_buckets, const vector<uint64_t>& frequency_values, vector<vector<double>>& histograms) {
        histograms.resize(num_histograms, vector<double>(num_buckets, 0.0));
        for (uint64_t i = 0; i < num_histograms; ++i) {
            for (uint64_t j = 0; j < num_buckets; ++j) {
                histograms[i][j] = static_cast<double>(frequency_values[i * num_buckets + j]);
            }
        }
    }

    // Helper function to check if a data point is in a projection
    bool isDataInProjection(const vector<double>& tuple, const DenseUnit& candidate, uint64_t xsi) {
        for (const auto& [featureIndex, rangeIndex] : candidate) {
            uint64_t bucketIndex = static_cast<uint64_t>(floor(tuple[featureIndex] * xsi));
            if (bucketIndex % xsi != rangeIndex) {
                return false;
            }
        }
        return true;
    }

    // Function to normalize the data
    void normalizeFeatures(vector<vector<double>>& data) {
        for (size_t f = 0; f < data[0].size(); ++f) {
            double minElem = numeric_limits<double>::max();
            double maxElem = numeric_limits<double>::lowest();
            for (const auto& row : data) {
                minElem = std::min(minElem, row[f]);
                maxElem = std::max(maxElem, row[f]);
            }
            uint64_t range = maxElem - minElem;
            for (auto& row : data) {
                // Normalizing each feature to the range [0, 1) with 1e-5 padding
                row[f] = (row[f] - minElem) / (range + 1e-5);
            }
        }
    }


    // Function to get one-dimensional dense units
    void getOneDimDenseUnits(const vector<vector<double>>& data, uint64_t xsi, double tau, vector<DenseUnit>& oneDimDenseUnits) {
        size_t numDataPoints = data.size();
        size_t numFeatures = data[0].size();
        vector<vector<uint64_t>> projection(xsi, vector<uint64_t>(numFeatures, 0));

        for (const auto& row : data) {
            for (size_t f = 0; f < numFeatures; ++f) {
                uint64_t bucketIndex = static_cast<uint64_t>(floor(row[f] * xsi));
                projection[bucketIndex % xsi][f]++;
            }
        }

        for (size_t f = 0; f < numFeatures; ++f) {
            for (size_t unit = 0; unit < xsi; ++unit) {
                if (projection[unit][f] > tau * numDataPoints) {
                    oneDimDenseUnits.push_back({{f, unit}});
                }
            }
        }
    }

    // Function to join two dense units
    DenseUnit joinDenseUnits(const DenseUnit& unit1, const DenseUnit& unit2) {
        DenseUnit joined = unit1;
        joined.insert(unit2.begin(), unit2.end());
        return joined;
    }

    // Function to perform self-join on the dense units
    void selfJoin(const vector<DenseUnit>& prevDimDenseUnits, size_t dim, vector<DenseUnit>& candidates) {
        for (size_t i = 0; i < prevDimDenseUnits.size(); ++i) {
            for (size_t j = i + 1; j < prevDimDenseUnits.size(); ++j) {
                DenseUnit joined = joinDenseUnits(prevDimDenseUnits[i], prevDimDenseUnits[j]);
                if (joined.size() == dim && find(candidates.begin(), candidates.end(), joined) == candidates.end()) {
                    candidates.push_back(joined);
                }
            }
        }
    }

    // Function to prune candidates
    void prune(vector<DenseUnit>& candidates, const vector<DenseUnit>& prevDimDenseUnits) {
        candidates.erase(remove_if(candidates.begin(), candidates.end(),
                                [&prevDimDenseUnits](const DenseUnit& candidate) {
                                    for (const auto& [feature, rangeIndex] : candidate) {
                                        DenseUnit projection = candidate;
                                        projection.erase(feature);
                                        if (find(prevDimDenseUnits.begin(), prevDimDenseUnits.end(), projection) == prevDimDenseUnits.end()) {
                                            return true;
                                        }
                                    }
                                    return false;
                                }),
                        candidates.end());
    }

    // Function to get dense units for a given dimension
    vector<DenseUnit> getDenseUnitsForDim(const vector<vector<double>>& data, const vector<DenseUnit>& prevDimDenseUnits, size_t dim, uint64_t xsi, double tau) {
        vector<DenseUnit> candidates;
        selfJoin(prevDimDenseUnits, dim, candidates);
        prune(candidates, prevDimDenseUnits);

        vector<uint64_t> projection(candidates.size(), 0);
        size_t numDataPoints = data.size();
        for (size_t dataIndex = 0; dataIndex < numDataPoints; ++dataIndex) {
            for (size_t i = 0; i < candidates.size(); ++i) {
                if (isDataInProjection(data[dataIndex], candidates[i], xsi)) {
                    projection[i]++;
                }
            }
        }

        vector<DenseUnit> denseUnits;
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (projection[i] > tau * numDataPoints) {
                denseUnits.push_back(candidates[i]);
            }
        }
        return denseUnits;
    }

    // Function to check if two nodes are connected
    bool areNodesConnected(const DenseUnit& node1, const DenseUnit& node2) {
        if (node1.size() != node2.size() || node1.size() < 1) {
            return false;
        }
        auto it1 = node1.begin();
        auto it2 = node2.begin();
        int distance = 0;
        while (it1 != node1.end() && it2 != node2.end()) {
            if (it1->first != it2->first || abs(static_cast<int>(it1->second) - static_cast<int>(it2->second)) > 1) {
                return false;
            }
            distance += abs(static_cast<int>(it1->second) - static_cast<int>(it2->second));
            ++it1;
            ++it2;
        }
        return distance <= 1;
    }

    // Function to get cluster data point IDs
    set<size_t> getClusterDataPointIds(const vector<vector<double>>& data, const vector<DenseUnit>& cluster_dense_units, uint64_t xsi) {
        set<size_t> point_ids;
        size_t numDataPoints = data.size();
        for (size_t i = 0; i < numDataPoints; ++i) {
            for (const auto& unit: cluster_dense_units) {
                bool isInCluster = true;
                for (const auto& [featureIndex, rangeIndex] : unit) {
                    uint64_t bucketIndex = static_cast<uint64_t>(std::floor(data[i][featureIndex] * xsi));
                    if (bucketIndex % xsi != rangeIndex) {
                        isInCluster = false;
                        break;
                    }
                }
                if (isInCluster) {
                    point_ids.insert(i);
                    break;
                }
            }
        }
        return point_ids;
    }

    // Function to get clusters from dense units
    vector<Cluster> getClusters(const vector<DenseUnit>& denseUnits, const vector<vector<double>>& data, uint64_t xsi) {
        vector<vector<bool>> graph(denseUnits.size(), vector<bool>(denseUnits.size(), false));
        buildGraphFromDenseUnits(denseUnits, graph);

        vector<set<size_t>> components;
        findConnectedComponents(graph, components);

        vector<Cluster> clusters;
        for (const auto& component : components) {
            vector<DenseUnit> cluster_dense_units;
            set<uint64_t> dimensions;
            set<size_t> cluster_data_point_ids;

            for (const auto index : component) {
                cluster_dense_units.push_back(denseUnits[index]);
                for (const auto& [featureIndex, _] : denseUnits[index]) {
                    dimensions.insert(featureIndex);
                }
            }

            cluster_data_point_ids = getClusterDataPointIds(data, cluster_dense_units, xsi);
            clusters.emplace_back(cluster_dense_units, dimensions, cluster_data_point_ids);
        }

        return clusters;
    }


    // Function to build a graph from dense units
    void buildGraphFromDenseUnits(const vector<DenseUnit>& denseUnits, vector<vector<bool>>& graph) {
        size_t numDenseUnits = denseUnits.size();
        for (size_t i = 0; i < numDenseUnits; ++i) {
            for (size_t j = 0; j < numDenseUnits; ++j) {
                if (i == j) {
                    graph[i][j] = true;
                } else {
                    graph[i][j] = areNodesConnected(denseUnits[i], denseUnits[j]);
                }
            }
        }
    }

    // Function to perform DFS for connected component analysis
    void dfs(size_t node, const vector<vector<bool>>& graph, vector<bool>& visited, set<size_t>& component) {
        visited[node] = true;
        component.insert(node);
        for (size_t i = 0; i < graph.size(); ++i) {
            if (graph[node][i] && !visited[i]) {
                dfs(i, graph, visited, component);
            }
        }
    }

    // Function to find connected components in the graph
    void findConnectedComponents(const vector<vector<bool>>& graph, vector<set<size_t>>& components) {
        size_t numNodes = graph.size();
        vector<bool> visited(numNodes, false);

        for (size_t i = 0; i < numNodes; ++i) {
            if (!visited[i]) {
                set<size_t> component;
                dfs(i, graph, visited, component);
                components.push_back(component);
            }
        }
    }

    void filterFinalDimClusters(const vector<Cluster>& clusters, size_t finalDim,  vector<Cluster>& finalClusters) {
        for (const auto& cluster : clusters) {
            if (cluster.dimensions.size() == finalDim) {
                finalClusters.push_back(cluster);
            }
        }
    }

    void createUnclusteredDataPointsClusters(vector<Cluster>& finalClusters, size_t totalDataPoints) {
        vector<bool> isClustered(totalDataPoints, false);

        // Mark clustered data points
        for (const auto& cluster : finalClusters) {
            for (const auto id : cluster.data_point_ids) {
                isClustered[id] = true;
            }
        }

        // Create a cluster for each unclustered data point
        for (size_t i = 0; i < totalDataPoints; ++i) {
            if (!isClustered[i]) {
                set<size_t> dataPointId = {i};
                finalClusters.emplace_back(DenseUnit(), set<uint64_t>(), dataPointId);
            }
        }
    }

    void runClique(const vector<vector<double>>& data, uint64_t xsi, double tau, vector<Cluster>& clusters) {
        vector<DenseUnit> denseUnits;
        getOneDimDenseUnits(data, xsi, tau, denseUnits);

        // Get clusters for 1-dimensional dense units
        vector<Cluster> currentClusters = getClusters(denseUnits, data, xsi);
        clusters.insert(clusters.end(), currentClusters.begin(), currentClusters.end());

        size_t currentDim = 2;
        size_t numFeatures = data[0].size();
        while (currentDim <= numFeatures && !denseUnits.empty()) {
            denseUnits = getDenseUnitsForDim(data, denseUnits, currentDim, xsi, tau);
            currentClusters = getClusters(denseUnits, data, xsi);
            clusters.insert(clusters.end(), currentClusters.begin(), currentClusters.end());
            currentDim++;
        }
    }

};
