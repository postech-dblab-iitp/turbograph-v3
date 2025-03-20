#pragma once

#include "parser/query/graph_pattern/pattern.hpp"
#include "common/enums/query_rel_type.hpp"

namespace duckdb {

enum class ArrowDirection : uint8_t { LEFT = 0, RIGHT = 1, BOTH = 2 };

struct RecursiveRelPatternInfo {
    std::string lowerBound;
    std::string upperBound;
    std::string weightPropertyName;
    std::string relName;
    std::string nodeName;
    std::unique_ptr<ParsedExpression> whereExpression = nullptr;
    bool hasProjection = false;
    ParsedExpressions relProjectionList;
    ParsedExpressions nodeProjectionList;

    RecursiveRelPatternInfo() = default;
};

// jhha: how Relation can inherit Node? I think this design is kinda weird.
class RelPattern : public Pattern {
public:
    RelPattern(std::string name, std::vector<std::string> typeNames, QueryRelType relType,
        ArrowDirection arrowDirection, std::vector<ParsedPropertyKeyValue> propertyKeyValPairs,
        RecursiveRelPatternInfo recursiveInfo)
        : Pattern{std::move(name), std::move(propertyKeyValPairs)},
          typeNames{std::move(typeNames)}, relType{relType}, arrowDirection{arrowDirection},
          recursiveInfo{std::move(recursiveInfo)} {}

    QueryRelType getRelType() const { return relType; }

    ArrowDirection getDirection() const { return arrowDirection; }

    inline std::vector<EdgeType> getTypeNames() const { return typeNames; }

    const RecursiveRelPatternInfo* getRecursiveInfo() const { return &recursiveInfo; }

    std::string ToString() const override {
        std::stringstream ss;

        if (arrowDirection == ArrowDirection::LEFT) {
            ss << "<-";
        } else {
            ss << "-";
        }

        ss << "[" << variableName;

        // Handle relationship types (e.g., `[:TYPE1:TYPE2]`)
        if (!typeNames.empty()) {
            for (const auto &type : typeNames) {
                ss << ":" << type;
            }
        }

        // Handle property key-value pairs (`{key: value}`)
        if (!propertyKeyVals.empty()) {
            ss << " {";
            for (size_t i = 0; i < propertyKeyVals.size(); ++i) {
                auto &keyValPair = propertyKeyVals[i];
                ss << keyValPair.first << ": " << keyValPair.second->ToString();
                if (i < propertyKeyVals.size() - 1) {
                    ss << ", ";
                }
            }
            ss << "}";
        }

        // Handle variable-length relationship (`*lower..upper`)
        if (relType != QueryRelType::NON_RECURSIVE) {
            ss << "*";

            switch (relType)
            {
            case QueryRelType::VARIABLE_LENGTH_WALK:
                break;
            case QueryRelType::VARIABLE_LENGTH_TRAIL:
                ss << " TRAIL ";
                break;
            case QueryRelType::VARIABLE_LENGTH_ACYCLIC:
                ss << " ACYCLIC ";
                break;
            case QueryRelType::SHORTEST:
            case QueryRelType::WEIGHTED_SHORTEST:
                ss << " SHORTEST ";
                break;
            case QueryRelType::ALL_SHORTEST:
            case QueryRelType::ALL_WEIGHTED_SHORTEST:
                ss << " ALL SHORTEST ";
                break;
            default:
                break;
            }

            if (!recursiveInfo.lowerBound.empty()) {
                ss << recursiveInfo.lowerBound;
            }
            if (!recursiveInfo.upperBound.empty()) {
                ss << ".." << recursiveInfo.upperBound;
            }
        }

        // Handle weight property for shortest paths
        if (relType == QueryRelType::WEIGHTED_SHORTEST || relType == QueryRelType::ALL_WEIGHTED_SHORTEST) {
            if (!recursiveInfo.weightPropertyName.empty()) {
                ss << " weight: " << recursiveInfo.weightPropertyName;
            }
        }

        // Handle recursive relationship projections
        if (recursiveInfo.hasProjection) {
            ss << " PROJECTION(";
            ss << "rel: [" << StringUtil::Join(recursiveInfo.relProjectionList, recursiveInfo.relProjectionList.size(), ", ",
                                            [](const std::unique_ptr<ParsedExpression> &expr) { return expr->ToString(); }) << "], ";
            ss << "node: [" << StringUtil::Join(recursiveInfo.nodeProjectionList, recursiveInfo.nodeProjectionList.size(), ", ",
                                                [](const std::unique_ptr<ParsedExpression> &expr) { return expr->ToString(); }) << "]";
            ss << ")";
        }

        ss << "]";

        if (arrowDirection == ArrowDirection::RIGHT) {
            ss << "->";
        } else {
            ss << "-";
        }

        return ss.str();
    }


private:
    QueryRelType relType;
    ArrowDirection arrowDirection;
    std::vector<EdgeType> typeNames;
    RecursiveRelPatternInfo recursiveInfo;
};

}