#pragma once

#include <bitset>
#include <unordered_map>

#include "binder/expression/rel_expression.h"
#include "binder/expression/path_expression.h"
#include "binder/parse_tree_node.h"

namespace kuzu {
namespace binder {

const uint8_t MAX_NUM_VARIABLES = 64;

class QueryGraph;
struct SubqueryGraph;

// hash on node bitset if subgraph has no rel
struct SubqueryGraphHasher {
    std::size_t operator()(const SubqueryGraph& key) const;
};

struct SubqueryGraph {

    const QueryGraph& queryGraph;
    bitset<MAX_NUM_VARIABLES> queryNodesSelector;
    bitset<MAX_NUM_VARIABLES> queryRelsSelector;

    explicit SubqueryGraph(const QueryGraph& queryGraph) : queryGraph{queryGraph} {}

    SubqueryGraph(const SubqueryGraph& other)
        : queryGraph{other.queryGraph}, queryNodesSelector{other.queryNodesSelector},
          queryRelsSelector{other.queryRelsSelector} {}

    ~SubqueryGraph() = default;

    inline void addQueryNode(uint32_t nodePos) { queryNodesSelector[nodePos] = true; }
    inline void addQueryRel(uint32_t relPos) { queryRelsSelector[relPos] = true; }
    inline void addSubqueryGraph(const SubqueryGraph& other) {
        queryRelsSelector |= other.queryRelsSelector;
        queryNodesSelector |= other.queryNodesSelector;
    }

    inline uint32_t getNumQueryRels() const { return queryRelsSelector.count(); }
    inline uint32_t getTotalNumVariables() const {
        return queryNodesSelector.count() + queryRelsSelector.count();
    }
    inline bool isSingleRel() const {
        return queryRelsSelector.count() == 1 && queryNodesSelector.count() == 0;
    }

    bool containAllVariables(unordered_set<string>& variables) const;

    unordered_set<uint32_t> getNodeNbrPositions() const;
    unordered_set<uint32_t> getRelNbrPositions() const;
    unordered_set<SubqueryGraph, SubqueryGraphHasher> getNbrSubgraphs(uint32_t size) const;
    vector<uint32_t> getConnectedNodePos(const SubqueryGraph& nbr) const;

    // E.g. query graph (a)-[e1]->(b) and subgraph (a)-[e1], although (b) is not in subgraph, we
    // return both (a) and (b) regardless of node selector. See needPruneJoin() in
    // join_order_enumerator.cpp for its use case.
    unordered_set<uint32_t> getNodePositionsIgnoringNodeSelector() const;

    bool operator==(const SubqueryGraph& other) const {
        return queryRelsSelector == other.queryRelsSelector &&
               queryNodesSelector == other.queryNodesSelector;
    }

private:
    unordered_set<SubqueryGraph, SubqueryGraphHasher> getBaseNbrSubgraph() const;
    unordered_set<SubqueryGraph, SubqueryGraphHasher> getNextNbrSubgraphs(
        const SubqueryGraph& prevNbr) const;
};

// QueryGraph represents a connected pattern specified in MATCH clause.
enum QueryGraphType: uint8_t { NONE = 0, SHORTEST = 1, ALL_SHORTEST = 2 };

class QueryGraph : public ParseTreeNode {
public:
    QueryGraph() = default;

    QueryGraph(const QueryGraph& other)
        : queryGraphType{other.queryGraphType}, queryNodeNameToPosMap{other.queryNodeNameToPosMap},
          queryRelNameToPosMap{other.queryRelNameToPosMap},
          queryNodes{other.queryNodes}, queryRels{other.queryRels}, queryPath(other.queryPath) {}

    ~QueryGraph() = default;

    inline uint32_t getNumQueryNodes() const { return queryNodes.size(); }
    inline bool containsQueryNode(const string& queryNodeName) const {
        return queryNodeNameToPosMap.find(queryNodeName) != queryNodeNameToPosMap.end();
    }
    inline vector<shared_ptr<NodeExpression>> &getQueryNodes() { return queryNodes; }
    inline shared_ptr<NodeExpression> getQueryNode(const string& queryNodeName) const {
        return queryNodes[getQueryNodePos(queryNodeName)];
    }
    inline vector<shared_ptr<NodeExpression>> getQueryNodes(vector<uint32_t> nodePoses) const {
        vector<shared_ptr<NodeExpression>> result;
        for (auto nodePos : nodePoses) {
            result.push_back(queryNodes[nodePos]);
        }
        return result;
    }
    inline shared_ptr<NodeExpression> getQueryNode(uint32_t nodePos) const {
        return queryNodes[nodePos];
    }
    inline uint32_t getQueryNodePos(NodeExpression& node) const {
        return getQueryNodePos(node.getUniqueName());
    }
    inline uint32_t getQueryNodePos(const string& queryNodeName) const {
        return queryNodeNameToPosMap.at(queryNodeName);
    }
    void addQueryNode(shared_ptr<NodeExpression> queryNode);

    inline shared_ptr<PathExpression> getQueryPath() const {
        return queryPath;
    }

    inline void setQueryPath(string& unique_name) {
        assert(queryPath == nullptr);
        if (queryGraphType == QueryGraphType::SHORTEST) {
            queryPath = make_shared<PathExpression>(SHORTEST_PATH, unique_name, queryNodes, queryRels);
        }
        else {
            queryPath = make_shared<PathExpression>(ALL_SHORTEST_PATH, unique_name, queryNodes, queryRels);
        }
        
    }

    inline uint32_t getNumQueryRels() const { return queryRels.size(); }
    inline bool containsQueryRel(const string& queryRelName) const {
        return queryRelNameToPosMap.find(queryRelName) != queryRelNameToPosMap.end();
    }
    inline vector<shared_ptr<RelExpression>> &getQueryRels() { return queryRels; }
    inline shared_ptr<RelExpression> getQueryRel(const string& queryRelName) const {
        return queryRels.at(queryRelNameToPosMap.at(queryRelName));
    }
    inline shared_ptr<RelExpression> getQueryRel(uint32_t relPos) const {
        return queryRels[relPos];
    }
    inline uint32_t getQueryRelPos(const string& queryRelName) const {
        return queryRelNameToPosMap.at(queryRelName);
    }
    void addQueryRel(shared_ptr<RelExpression> queryRel);

    bool canProjectExpression(Expression* expression) const;

    bool isConnected(const QueryGraph& other);

    void merge(const QueryGraph& other);

    inline unique_ptr<QueryGraph> copy() const { return make_unique<QueryGraph>(*this); }

    std::string getName() override { return "[QueryGraph]"; }
    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        // nodes
        for( auto& qn: queryNodes) {
            result.push_back((ParseTreeNode*)qn.get());
        }
        // rels
        for( auto& qn: queryRels) {
            result.push_back((ParseTreeNode*)qn.get());
        }

        return result;
    }

    inline QueryGraphType getQueryGraphType() const { return queryGraphType; }

    void setQueryGraphType(QueryGraphType queryGraphType) {
        assert(this->queryGraphType == QueryGraphType::NONE);
        this->queryGraphType = queryGraphType;
    }

private:
    QueryGraphType queryGraphType = QueryGraphType::NONE;
    unordered_map<string, uint32_t> queryNodeNameToPosMap;
    unordered_map<string, uint32_t> queryRelNameToPosMap;
    vector<shared_ptr<NodeExpression>> queryNodes;
    vector<shared_ptr<RelExpression>> queryRels;
    shared_ptr<PathExpression> queryPath = nullptr; 
};

// QueryGraphCollection represents a pattern (a set of connected components) specified in MATCH
// clause.
class QueryGraphCollection : public ParseTreeNode {
public:
    QueryGraphCollection() = default;

    void addAndMergeQueryGraphIfConnected(unique_ptr<QueryGraph> queryGraphToAdd);
    inline uint32_t getNumQueryGraphs() const { return queryGraphs.size(); }
    inline QueryGraph* getQueryGraph(uint32_t idx) const { return queryGraphs[idx].get(); }

    vector<shared_ptr<NodeExpression>> getQueryNodes() const;
    vector<shared_ptr<RelExpression>> getQueryRels() const;

    unique_ptr<QueryGraphCollection> copy() const;

    std::string getName() override { return "[QueryGraphCollection]"; }
    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        for( auto& a: queryGraphs) {
            result.push_back((ParseTreeNode*)a.get());
        }
        return result;
    }

private:
    vector<unique_ptr<QueryGraph>> queryGraphs;
};

class PropertyKeyValCollection {
public:
    PropertyKeyValCollection() = default;
    PropertyKeyValCollection(const PropertyKeyValCollection& other)
        : varNameToPropertyKeyValPairs{other.varNameToPropertyKeyValPairs} {}

    void addPropertyKeyValPair(const Expression& variable, expression_pair propertyKeyValPair);
    vector<expression_pair> getPropertyKeyValPairs(const Expression& variable) const;
    vector<expression_pair> getAllPropertyKeyValPairs() const;

    // bool hasPropertyKeyValPair(const Expression& variable, const string& propertyName) const;
    // expression_pair getPropertyKeyValPair(
    //     const Expression& variable, const string& propertyName) const;

    inline unique_ptr<PropertyKeyValCollection> copy() const {
        return make_unique<PropertyKeyValCollection>(*this);
    }

private:
    // First indexed on variable name, then indexed on property name.
    // a -> { age -> pair<a.age,12>, name -> pair<name,'Alice'>}
    // unordered_map<string, unordered_map<string, expression_pair>> varNameToPropertyKeyValPairs;
    unordered_map<string, unordered_map<uint64_t, expression_pair>> varNameToPropertyKeyValPairs;
};

} // namespace binder
} // namespace kuzu
