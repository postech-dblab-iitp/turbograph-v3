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
#include "statistics/clustering/clustering.hpp"

using namespace std;

using DenseUnit = map<uint64_t, uint64_t>;

class Cluster {
public:
    size_t id;
    DenseUnit dense_units;
    set<uint64_t> dimensions;
    set<size_t> data_point_ids;

    Cluster(const DenseUnit& units, const set<uint64_t>& dims, const set<size_t>& ids)
        : dense_units(units), dimensions(dims), data_point_ids(ids) {}

    void printCluster() {
        cout << "Dense units: ";
        for (const auto& unit : dense_units) {
            cout << "[" << unit.first << ": " << unit.second << "] ";
        }
        cout << "\nDimensions: ";
        for (const auto& dim : dimensions) {
            cout << dim << " ";
        }
        cout << "\nCluster size: " << data_point_ids.size() << "\nData points: ";
        for (const auto& id : data_point_ids) {
            cout << id << " ";
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

        vector<vector<uint64_t>> histograms;
        reshapeHistograms(num_histograms, num_buckets, frequency_values, histograms);

        // Normalize the data
        normalizeFeatures(histograms);

        // Run the CLIQUE clustering algorithm
        vector<Cluster> clusters;
        runClique(histograms, xsi, tau, clusters);

        // Output the clusters
        std::cout << "Number of clusters: " << clusters.size() << std::endl;
        for (size_t i = 0; i < clusters.size(); ++i) {
            cout << "Cluster " << i << ": ";
            clusters[i].printCluster();
        }


    }

private:
    void reshapeHistograms(uint64_t num_histograms, uint64_t num_buckets, const vector<uint64_t>& frequency_values, vector<vector<uint64_t>>& histograms) {
        histograms.reserve(num_histograms);
        for (uint64_t i = 0; i < num_histograms; ++i) {
            histograms.push_back(vector<uint64_t>(frequency_values.begin() + i * num_buckets, frequency_values.begin() + (i + 1) * num_buckets));
        }
    }

    // Helper function to check if a data point is in a projection
    bool isDataInProjection(const vector<uint64_t>& tuple, const DenseUnit& candidate, uint64_t xsi) {
        for (const auto& [featureIndex, rangeIndex] : candidate) {
            if (static_cast<uint64_t>(floor(tuple[featureIndex] * xsi % xsi)) != rangeIndex) {
                return false;
            }
        }
        return true;
    }

    // Function to normalize the data
    void normalizeFeatures(vector<vector<uint64_t>>& data) {
        for (size_t f = 0; f < data[0].size(); ++f) {
            uint64_t minElem = numeric_limits<uint64_t>::max();
            uint64_t maxElem = numeric_limits<uint64_t>::lowest();
            for (const auto& row : data) {
                minElem = min(minElem, row[f]);
                maxElem = max(maxElem, row[f]);
            }
            for (auto& row : data) {
                row[f] = (row[f] - minElem) / (maxElem - minElem);
            }
        }
    }

    // Function to get one-dimensional dense units
    void getOneDimDenseUnits(const vector<vector<uint64_t>>& data, double tau, uint64_t xsi, vector<DenseUnit>& oneDimDenseUnits) {
        size_t numDataPoints = data.size();
        size_t numFeatures = data[0].size();
        vector<vector<uint64_t>> projection(xsi, vector<uint64_t>(numFeatures, 0));

        for (const auto& row : data) {
            for (size_t f = 0; f < numFeatures; ++f) {
                projection[static_cast<uint64_t>(floor(row[f] * xsi % xsi))][f]++;
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
    vector<DenseUnit> getDenseUnitsForDim(const vector<vector<uint64_t>>& data, const vector<DenseUnit>& prevDimDenseUnits, size_t dim, uint64_t xsi, double tau) {
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
    set<size_t> getClusterDataPointIds(const vector<vector<uint64_t>>& data, const DenseUnit& cluster_dense_units, double xsi) {
        set<size_t> point_ids;
        size_t numDataPoints = data.size();
        for (size_t i = 0; i < numDataPoints; ++i) {
            bool isInCluster = true;
            for (const auto& [featureIndex, rangeIndex] : cluster_dense_units) {
                if (static_cast<uint64_t>(floor(data[i][featureIndex] * xsi)) != rangeIndex) {
                    isInCluster = false;
                    break;
                }
            }
            if (isInCluster) {
                point_ids.insert(i);
            }
        }
        return point_ids;
    }

    // Function to get clusters from dense units
    vector<Cluster> getClusters(const vector<DenseUnit>& denseUnits, const vector<vector<uint64_t>>& data, uint64_t xsi) {
        vector<vector<bool>> graph(denseUnits.size(), vector<bool>(denseUnits.size(), false));
        buildGraphFromDenseUnits(denseUnits, graph);

        vector<set<size_t>> components;
        findConnectedComponents(graph, components);

        vector<Cluster> clusters;
        for (const auto& component : components) {
            DenseUnit cluster_dense_units;
            set<uint64_t> dimensions;
            set<size_t> cluster_data_point_ids;

            for (const auto index : component) {
                for (const auto& unit : denseUnits[index]) {
                    cluster_dense_units.insert(unit);
                    dimensions.insert(unit.first);
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

    // Main CLIQUE clustering function
    void runClique(const vector<vector<uint64_t>>& data, uint64_t xsi, double tau, vector<set<size_t>>& components) {
        vector<DenseUnit> denseUnits;
        getOneDimDenseUnits(data, tau, xsi, denseUnits);

        size_t currentDim = 2;
        size_t numFeatures = data[0].size();
        while (currentDim <= numFeatures && !denseUnits.empty()) {
            denseUnits = getDenseUnitsForDim(data, denseUnits, currentDim, xsi, tau);
            currentDim++;
        }


        vector<vector<bool>> graph(denseUnits.size(), vector<bool>(denseUnits.size(), false));
        buildGraphFromDenseUnits(denseUnits, graph);
        findConnectedComponents(graph, components);
    }

    void runClique(const vector<vector<uint64_t>>& data, uint64_t xsi, double tau, vector<Cluster>& clusters) {
        vector<DenseUnit> denseUnits;
        getOneDimDenseUnits(data, tau, xsi, denseUnits);

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
