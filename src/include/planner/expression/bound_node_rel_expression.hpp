// #pragma once

// #include "common/case_insensitive_map.hpp"
// #include "planner/expression.hpp"

// namespace duckdb {
// class TableCatalogEntry;

// class NodeOrRelExpression : public Expression {
//     static constexpr ExpressionType expressionType_ = ExpressionType::PATTERN;

// public:
//     NodeOrRelExpression(common::LogicalType dataType, std::string uniqueName,
//         std::string variableName, std::vector<catalog::TableCatalogEntry*> entries)
//         : Expression{expressionType_, std::move(dataType), std::move(uniqueName)},
//           variableName(std::move(variableName)), entries{std::move(entries)} {}

//     std::string getVariableName() const { return variableName; }

//     bool isEmpty() const { return entries.empty(); }
//     bool isMultiLabeled() const { return entries.size() > 1; }
    
//     vector<CatalogObjectID> getPartitionOIDs() const { return partitionOIDs; }
//     vector<CatalogObjectID> getGraphletOIDs() const { return graphletOIDs; }

//     inline void addPartitionOIDs(const vector<CatalogObjectID>& partitionOIDsToAdd) {
//         for (auto& oid : partitionOIDsToAdd) {
//             partitionOIDs.push_back(oid);
//         }
//     }

//     void addGraphletOIDs(const vector<CatalogObjectID>& graphletOIDsToAdd) {
//         for (auto& oid : graphletOIDsToAdd) {
//             graphletOIDs.push_back(oid);
//         }
//     }

//     void addPropertyExpression(const std::string& propertyName,
//         std::unique_ptr<Expression> property);
//     bool hasPropertyExpression(const std::string& propertyName) const {
//         return propertyNameToIdx.contains(propertyName);
//     }
//     // Deep copy expression.
//     std::shared_ptr<Expression> getPropertyExpression(const std::string& propertyName) const;
//     const std::vector<std::unique_ptr<Expression>>& getPropertyExprsRef() const {
//         return propertyExprs;
//     }
//     // Deep copy expressions.
//     Expressions getPropertyExprs() const {
//         Expressions result;
//         for (auto& expr : propertyExprs) {
//             result.push_back(expr);
//         }
//         return result;
//     }

//     void setLabelExpression(std::shared_ptr<Expression> expression) {
//         labelExpression = std::move(expression);
//     }
//     std::shared_ptr<Expression> getLabelExpression() const { return labelExpression; }

//     void addPropertyDataExpr(std::string propertyName, std::shared_ptr<Expression> expr) {
//         propertyDataExprs.insert({propertyName, expr});
//     }
//     const case_insensitive_map_t<std::shared_ptr<Expression>>&
//     getPropertyDataExprRef() const {
//         return propertyDataExprs;
//     }
//     bool hasPropertyDataExpr(const std::string& propertyName) const {
//         return propertyDataExprs.find(propertyName) != propertyDataExprs.end();
//     }
//     std::shared_ptr<Expression> getPropertyDataExpr(const std::string& propertyName) const {
//         return propertyDataExprs.at(propertyName);
//     }

// protected:
//     std::string variableName;
//     vector<CatalogObjectID> partitionOIDs;
//     vector<CatalogObjectID> graphletOIDs;
//     // Index over propertyExprs on property name.
//     case_insensitive_map_t<idx_t> propertyNameToIdx;
//     // Property expressions with order.
//     std::vector<std::unique_ptr<Expression>> propertyExprs;
//     std::shared_ptr<Expression> labelExpression;
//     // Property data expressions specified by user in the form of "{propertyName : data}"
//     case_insensitive_map_t<std::shared_ptr<Expression>> propertyDataExprs;
// };

// }