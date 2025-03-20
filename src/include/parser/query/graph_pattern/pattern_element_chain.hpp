#pragma once

#include "parser/query/graph_pattern/node_pattern.hpp"
#include "parser/query/graph_pattern/rel_pattern.hpp"

namespace duckdb {

class PatternElementChain {
public:
    PatternElementChain(std::unique_ptr<RelPattern> relPattern, std::unique_ptr<NodePattern> nodePattern)
        : relPattern{std::move(relPattern)}, nodePattern{std::move(nodePattern)} {}

    inline const RelPattern* getRelPattern() const { return relPattern.get(); }

    inline const NodePattern* getNodePattern() const { return nodePattern.get(); }

    std::string ToString() const {
        std::stringstream ss;

        if (relPattern) {
            ss << relPattern->ToString();
        }
        
        if (nodePattern) {
            ss << nodePattern->ToString();
        }

        return ss.str();
    }


private:
    std::unique_ptr<RelPattern> relPattern;
    std::unique_ptr<NodePattern> nodePattern;
};

}