#pragma once

#include "parser/parsed_expression.hpp"
#include "common/copy_constructors.hpp"
#include "common/string_util.hpp"

namespace duckdb {

class ProjectionBody {
public:
    ProjectionBody(bool isDistinct,
        std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions)
        : isDistinct{isDistinct}, projectionExpressions{std::move(projectionExpressions)} {}

    inline bool getIsDistinct() const { return isDistinct; }

    inline const std::vector<std::unique_ptr<ParsedExpression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }

    inline void setOrderByExpressions(std::vector<std::unique_ptr<ParsedExpression>> expressions,
        std::vector<bool> sortOrders) {
        orderByExpressions = std::move(expressions);
        isAscOrders = std::move(sortOrders);
    }
    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    inline const std::vector<std::unique_ptr<ParsedExpression>>& getOrderByExpressions() const {
        return orderByExpressions;
    }

    inline std::vector<bool> getSortOrders() const { return isAscOrders; }

    inline void setSkipExpression(std::unique_ptr<ParsedExpression> expression) {
        skipExpression = std::move(expression);
    }
    inline bool hasSkipExpression() const { return skipExpression != nullptr; }
    inline ParsedExpression* getSkipExpression() const { return skipExpression.get(); }

    inline void setLimitExpression(std::unique_ptr<ParsedExpression> expression) {
        limitExpression = std::move(expression);
    }
    inline bool hasLimitExpression() const { return limitExpression != nullptr; }
    inline ParsedExpression* getLimitExpression() const { return limitExpression.get(); }

    std::string ToString() const {
        string result = isDistinct ? "DISTINCT " : "";

        // Convert projection expressions to string
        result += StringUtil::Join(projectionExpressions, projectionExpressions.size(), ", ",
            [](const std::unique_ptr<ParsedExpression> &expr) { 
                return expr->ToString(); 
            });

        // Handle ORDER BY
        if (hasOrderByExpressions()) {
            size_t idx = 0; // Mutable external counter
            result += " ORDER BY " + StringUtil::Join(orderByExpressions, orderByExpressions.size(), ", ",
                [&idx, this](const std::unique_ptr<ParsedExpression> &expr) {
                    return expr->ToString() + (isAscOrders[idx++] ? " ASC" : " DESC");
                });
        }

        // Handle SKIP
        if (hasSkipExpression()) {
            result += " SKIP " + skipExpression->ToString();
        }

        // Handle LIMIT
        if (hasLimitExpression()) {
            result += " LIMIT " + limitExpression->ToString();
        }

        return result;
    }


private:
    bool isDistinct;
    std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions;
    std::vector<std::unique_ptr<ParsedExpression>> orderByExpressions;
    std::vector<bool> isAscOrders;
    std::unique_ptr<ParsedExpression> skipExpression;
    std::unique_ptr<ParsedExpression> limitExpression;
};

}