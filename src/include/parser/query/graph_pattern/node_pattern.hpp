#pragma once

#include <string>

#include "parser/query/graph_pattern/pattern.hpp"

namespace duckdb {

class NodePattern : public Pattern {
public:
    NodePattern(std::string name, std::vector<NodeLabel> labelNames,
        std::vector<ParsedPropertyKeyValue> propertyKeyVals)
        : Pattern(std::move(name), std::move(propertyKeyVals)), labelNames{std::move(labelNames)} {}

    virtual ~NodePattern() = default;

    inline std::vector<NodeLabel> getLabelNames() const { 
        return labelNames; 
    }

    std::string ToString() const override {
        std::stringstream ss;

        ss << "(" << variableName;

        if (!labelNames.empty()) {
            for (const auto &label : labelNames) {
                ss << ":" << label;
            }
        }

        if (!propertyKeyVals.empty()) {
            ss << " {";
            for (size_t i = 0; i < propertyKeyVals.size(); ++i) {
                auto &keyValPair = propertyKeyVals[i];
                ss << keyValPair.first << ":";
                ss << keyValPair.second->ToString();
                if (i < propertyKeyVals.size() - 1) {
                    ss << ", ";
                }
            }
            ss << "}";
        }

        ss << ")";
        return ss.str();
    }

protected:
    std::vector<NodeLabel> labelNames;
};

}