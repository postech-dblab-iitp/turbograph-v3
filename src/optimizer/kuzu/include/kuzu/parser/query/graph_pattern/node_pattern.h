#pragma once

#include <memory>
#include <string>

#include "kuzu/parser/expression/parsed_expression.h"

using namespace std;

namespace kuzu {
namespace parser {

/**
 * NodePattern represents "(nodeName:NodeTable+ {p1:v1, p2:v2, ...})"
 */
class NodePattern {
public:
    NodePattern(string name, vector<string> tableNames,
        vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : variableName{std::move(name)}, labelNames{std::move(tableNames)},
          propertyKeyValPairs{std::move(propertyKeyValPairs)} {}

    virtual ~NodePattern() = default;

    inline string getVariableName() const { return variableName; }

    inline vector<string> getLabelOrTypeNames() const {
        // MATCH (A:B) => [A,B] (intersection between labels - return labels having both A and B)
        return labelNames;
    }

    inline uint32_t getNumPropertyKeyValPairs() const { return propertyKeyValPairs.size(); }
    inline pair<string, ParsedExpression*> getProperty(uint32_t idx) const {
        return make_pair(propertyKeyValPairs[idx].first, propertyKeyValPairs[idx].second.get());
    }

protected:
    string variableName;
    vector<string> labelNames;
    vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs;
};

} // namespace parser
} // namespace kuzu
