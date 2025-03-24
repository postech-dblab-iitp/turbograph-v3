#pragma once

#include <bitset>
#include <unordered_map>

#include "planner/expression.hpp"
#include "planner/query/graph_pattern/bound_node_pattern.hpp"
#include "planner/query/graph_pattern/bound_rel_pattern.hpp"

namespace duckdb {

constexpr static uint8_t MAX_NUM_QUERY_VARIABLES = 64;

class QueryGraph;
// struct SubqueryGraph;
// struct SubqueryGraphHasher;
// using subquery_graph_set_t = std::unordered_set<SubqueryGraph, SubqueryGraphHasher>;
// template<typename T>
// using subquery_graph_V_map_t = std::unordered_map<SubqueryGraph, T, SubqueryGraphHasher>;

// hash on node bitset if subgraph has no rel
// struct SubqueryGraphHasher {
//     std::size_t operator()(const SubqueryGraph& key) const;
// };

// struct SubqueryGraph {
//     const QueryGraph& queryGraph;
//     std::bitset<MAX_NUM_QUERY_VARIABLES> queryNodesSelector;
//     std::bitset<MAX_NUM_QUERY_VARIABLES> queryRelsSelector;

//     explicit SubqueryGraph(const QueryGraph& queryGraph) : queryGraph{queryGraph} {}

//     void addQueryNode(common::idx_t nodePos) { queryNodesSelector[nodePos] = true; }
//     void addQueryRel(common::idx_t relPos) { queryRelsSelector[relPos] = true; }
//     void addSubqueryGraph(const SubqueryGraph& other) {
//         queryRelsSelector |= other.queryRelsSelector;
//         queryNodesSelector |= other.queryNodesSelector;
//     }

//     common::idx_t getNumQueryRels() const { return queryRelsSelector.count(); }
//     common::idx_t getTotalNumVariables() const {
//         return queryNodesSelector.count() + queryRelsSelector.count();
//     }
//     bool isSingleRel() const {
//         return queryRelsSelector.count() == 1 && queryNodesSelector.count() == 0;
//     }

//     bool containAllVariables(const std::unordered_set<std::string>& variables) const;

//     std::unordered_set<common::idx_t> getNodeNbrPositions() const;
//     std::unordered_set<common::idx_t> getRelNbrPositions() const;
//     subquery_graph_set_t getNbrSubgraphs(uint32_t size) const;
//     std::vector<uint32_t> getConnectedNodePos(const SubqueryGraph& nbr) const;

//     // E.g. query graph (a)-[e1]->(b) and subgraph (a)-[e1], although (b) is not in subgraph, we
//     // return both (a) and (b) regardless of node selector. See needPruneJoin() in
//     // join_order_enumerator.cpp for its use case.
//     std::unordered_set<common::idx_t> getNodePositionsIgnoringNodeSelector() const;

//     std::vector<common::idx_t> getNbrNodeIndices() const;

//     bool operator==(const SubqueryGraph& other) const {
//         return queryRelsSelector == other.queryRelsSelector &&
//                queryNodesSelector == other.queryNodesSelector;
//     }

// private:
//     subquery_graph_set_t getBaseNbrSubgraph() const;
//     subquery_graph_set_t getNextNbrSubgraphs(const SubqueryGraph& prevNbr) const;
// };

// QueryGraph represents a connected pattern specified in MATCH clause.
class QueryGraph {
public:
    QueryGraph() = default;
    QueryGraph(const QueryGraph& other)
        : queryNodeNameToPosMap{other.queryNodeNameToPosMap},
          queryRelNameToPosMap{other.queryRelNameToPosMap}, queryNodes{other.queryNodes},
          queryRels{other.queryRels} {}

    std::vector<std::shared_ptr<BoundPattern>> getAllPatterns() const {
        std::vector<std::shared_ptr<BoundPattern>> patterns;
        for (auto& p : queryNodes) {
            patterns.push_back(p);
        }
        for (auto& p : queryRels) {
            patterns.push_back(p);
        }
        return patterns;
    }

    idx_t getNumQueryNodes() const { return queryNodes.size(); }

    bool containsQueryNode(const std::string& queryNodeName) const {
        return queryNodeNameToPosMap.find(queryNodeName) != queryNodeNameToPosMap.end();
    }

    std::vector<std::shared_ptr<BoundNodePattern>> getQueryNodes() const { return queryNodes; }
    std::shared_ptr<BoundNodePattern> getQueryNode(const std::string& queryNodeName) const {
        return queryNodes[getQueryNodeIdx(queryNodeName)];
    }
    std::vector<std::shared_ptr<BoundNodePattern>> getQueryNodes(
        const std::vector<idx_t>& nodePoses) const {
        std::vector<std::shared_ptr<BoundNodePattern>> result;
        result.reserve(nodePoses.size());
        for (auto nodePos : nodePoses) {
            result.push_back(queryNodes[nodePos]);
        }
        return result;
    }
    std::shared_ptr<BoundNodePattern> getQueryNode(idx_t nodePos) const {
        return queryNodes[nodePos];
    }
    idx_t getQueryNodeIdx(const BoundNodePattern& node) const {
        return getQueryNodeIdx(node.getUniqueName());
    }
    idx_t getQueryNodeIdx(const std::string& queryNodeName) const {
        return queryNodeNameToPosMap.at(queryNodeName);
    }
    void addQueryNode(std::shared_ptr<BoundNodePattern> queryNode) {
        // Note that a node may be added multiple times. We should only keep one of it.
        // E.g. MATCH (a:person)-[:knows]->(b:person), (a)-[:knows]->(c:person)
        // a will be added twice during binding
        if (containsQueryNode(queryNode->getUniqueName())) {
            return;
        } 
        queryNodeNameToPosMap.insert({queryNode->getUniqueName(), queryNodes.size()});
        queryNodes.push_back(std::move(queryNode));
    }

    idx_t getNumQueryRels() const { return queryRels.size(); }
    bool containsQueryRel(const std::string& queryRelName) const {
        return queryRelNameToPosMap.find(queryRelName) != queryRelNameToPosMap.end();
    }
    std::vector<std::shared_ptr<BoundRelPattern>> getQueryRels() const { return queryRels; }
    std::shared_ptr<BoundRelPattern> getQueryRel(const std::string& queryRelName) const {
        return queryRels.at(queryRelNameToPosMap.at(queryRelName));
    }
    std::shared_ptr<BoundRelPattern> getQueryRel(idx_t relPos) const {
        return queryRels[relPos];
    }
    idx_t getQueryRelIdx(const std::string& queryRelName) const {
        return queryRelNameToPosMap.at(queryRelName);
    }
    void addQueryRel(std::shared_ptr<BoundRelPattern> queryRel) {
        if (containsQueryRel(queryRel->getUniqueName())) {
            return;
        }
        queryRelNameToPosMap.insert({queryRel->getUniqueName(), queryRels.size()});
        queryRels.push_back(std::move(queryRel));
    }

    bool isConnected(const QueryGraph& other) const {
        for (auto& queryNode : queryNodes) {
            if (other.containsQueryNode(queryNode->getUniqueName())) {
                return true;
            }
        }
        return false;
    }

    void merge(const QueryGraph& other) {
        for (auto& otherNode : other.queryNodes) {
            addQueryNode(otherNode);
        }
        for (auto& otherRel : other.queryRels) {
            addQueryRel(otherRel);
        }
    }

private:
    std::unordered_map<std::string, uint32_t> queryNodeNameToPosMap;
    std::unordered_map<std::string, uint32_t> queryRelNameToPosMap;
    std::vector<std::shared_ptr<BoundNodePattern>> queryNodes;
    std::vector<std::shared_ptr<BoundRelPattern>> queryRels;
};

// QueryGraphCollection represents a pattern (a set of connected components) specified in MATCH
// clause.
class QueryGraphCollection {
public:
    QueryGraphCollection() = default;

    void addAndMergeQueryGraphIfConnected(std::unique_ptr<QueryGraph> queryGraphToAdd) {
        auto newQueryGraphSet = std::vector<std::unique_ptr<QueryGraph>>();
        for (auto i = 0u; i < queryGraphs.size(); i++) {
            auto queryGraph = std::move(queryGraphs[i]);
            if (queryGraph->isConnected(*queryGraphToAdd.get())) {
                queryGraphToAdd->merge(*queryGraph.get());
            } else {
                newQueryGraphSet.push_back(std::move(queryGraph));
            }
        }
        newQueryGraphSet.push_back(std::move(queryGraphToAdd));
        queryGraphs = std::move(newQueryGraphSet);
    }
    void finalize() {
        idx_t baseGraphIdx = 0;
        while (true) {
            auto prevNumGraphs = queryGraphs.size();
            queryGraphs = mergeGraphs(baseGraphIdx++);
            if (queryGraphs.size() == prevNumGraphs || baseGraphIdx == queryGraphs.size()) {
                return;
            }
        }
    }
    size_t getNumQueryGraphs() const { return queryGraphs.size(); }
    QueryGraph* getQueryGraphUnsafe(idx_t idx) { return queryGraphs[idx].get(); }
    const QueryGraph* getQueryGraph(idx_t idx) const { return queryGraphs[idx].get(); }

    bool contains(const std::string& name) const {
        for (auto& queryGraph : queryGraphs) {
            if (queryGraph->containsQueryNode(name) || queryGraph->containsQueryRel(name)) {
                return true;
            }
        }
        return false;
    }
    std::vector<std::shared_ptr<BoundNodePattern>> getQueryNodes() const {
        std::vector<std::shared_ptr<BoundNodePattern>> result;
        for (auto& queryGraph : queryGraphs) {
            for (auto& node : queryGraph->getQueryNodes()) {
                result.push_back(node);
            }
        }
        return result;
    }
    std::vector<std::shared_ptr<BoundRelPattern>> getQueryRels() const {
        std::vector<std::shared_ptr<BoundRelPattern>> result;
        for (auto& queryGraph : queryGraphs) {
            for (auto& rel : queryGraph->getQueryRels()) {
                result.push_back(rel);
            }
        }
        return result;
    }

private:
    std::vector<std::unique_ptr<QueryGraph>> mergeGraphs(idx_t baseGraphIdx) {
        auto& baseGraph = queryGraphs[baseGraphIdx];
        std::unordered_set<idx_t> mergedGraphIndices;
        mergedGraphIndices.insert(baseGraphIdx);
        while (true) {
            // find graph to merge
            idx_t graphToMergeIdx = DConstants::INVALID_IDX;
            for (auto i = 0u; i < queryGraphs.size(); ++i) {
                if (mergedGraphIndices.find(i) != mergedGraphIndices.end()) { // graph has been merged.
                    continue;
                }
                if (baseGraph->isConnected(*queryGraphs[i].get())) { // find graph to merge.
                    graphToMergeIdx = i;
                    break;
                }
            }
            if (graphToMergeIdx == DConstants::INVALID_IDX) { // No graph can be merged. Terminate.
                break;
            }
            // Perform merge
            baseGraph->merge(*queryGraphs[graphToMergeIdx].get());
            mergedGraphIndices.insert(graphToMergeIdx);
        }
        std::vector<std::unique_ptr<QueryGraph>> finalGraphs;
        for (auto i = 0u; i < queryGraphs.size(); ++i) {
            if (i == baseGraphIdx) {
                finalGraphs.push_back(baseGraph);
                continue;
            }
            if (mergedGraphIndices.find(i) != mergedGraphIndices.end()) {
                continue;
            }
            finalGraphs.push_back(std::move(queryGraphs[i]));
        }
        return finalGraphs;
    }

private:
    std::vector<std::unique_ptr<QueryGraph>> queryGraphs;
};

struct BoundGraphPattern {
    QueryGraphCollection queryGraphCollection;
    std::shared_ptr<Expression> where;

    BoundGraphPattern() = default;
};

}