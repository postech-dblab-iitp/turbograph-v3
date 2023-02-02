#pragma once

#include "node_pattern.h"

namespace kuzu {
namespace parser {

enum ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1 };

/**
 * RelationshipPattern represents "-[relName:RelTableName+]-"
 */
class RelPattern : public NodePattern {
public:
    //  will get 
    RelPattern(string name, vector<string> tableNames, string lowerBound, string upperBound,
        ArrowDirection arrowDirection,
        vector<pair<string, unique_ptr<ParsedExpression>>> propertyKeyValPairs)
        : NodePattern{std::move(name), std::move(tableNames), std::move(propertyKeyValPairs)},
          lowerBound{std::move(lowerBound)}, upperBound{std::move(upperBound)},
          arrowDirection{arrowDirection} {}

    ~RelPattern() = default;

    inline vector<string> getLabelOrTypeNames() const { 
        // when accessing edge, the expression should be 
        // [T1|T2|T3] => which is represented in [T1,T2,T3], which is an union between edge labels
        return labelNames;
     }

    inline string getLowerBound() const { return lowerBound; }

    inline string getUpperBound() const { return upperBound; }

    inline ArrowDirection getDirection() const { return arrowDirection; }

private:
    string lowerBound;
    string upperBound;
    ArrowDirection arrowDirection;
};

} // namespace parser
} // namespace kuzu
