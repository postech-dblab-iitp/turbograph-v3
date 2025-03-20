#pragma once

#include <vector>

#include "parser/query/graph_pattern/pattern_element_chain.hpp"

namespace duckdb {

class PatternElement {
public:
    explicit PatternElement(std::unique_ptr<NodePattern> nodePattern)
        : nodePattern{std::move(nodePattern)} {}

    inline void setPathName(std::string name) { pathName = std::move(name); }
    inline bool hasPathName() const { return !pathName.empty(); }
    inline std::string getPathName() const { return pathName; }

    inline const NodePattern* getFirstNodePattern() const { return nodePattern.get(); }

    inline void addPatternElementChain(std::unique_ptr<PatternElementChain> patternElementChain) {
        patternElementChains.push_back(std::move(patternElementChain));
    }
    inline uint32_t getNumPatternElementChains() const { return patternElementChains.size(); }
    inline const PatternElementChain* getPatternElementChain(uint32_t idx) const {
        return patternElementChains[idx].get();
    }

    std::string ToString() const {
        std::stringstream ss;

        if (hasPathName()) {
            ss << pathName << " = ";
        }

        if (nodePattern) {
            ss << nodePattern->ToString();
        }

        for (const auto& chain : patternElementChains) {
            ss << chain->ToString();
        }

        return ss.str();
    }

private:
    std::string pathName;
    std::unique_ptr<NodePattern> nodePattern;
    std::vector<std::unique_ptr<PatternElementChain>> patternElementChains;
};

}