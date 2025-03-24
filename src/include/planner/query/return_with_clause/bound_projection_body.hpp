#pragma once

#include "planner/expression.hpp"

namespace duckdb {

class BoundProjectionBody {
    static constexpr uint64_t INVALID_NUMBER = UINT64_MAX;

public:
    explicit BoundProjectionBody(bool distinct)
        : distinct{distinct}, skipNumber{nullptr}, limitNumber{nullptr} {}

    bool isDistinct() const { return distinct; }

    void setProjectionExpressions(Expressions expressions) {
        projectionExpressions = std::move(expressions);
    }
    Expressions getProjectionExpressions() const { return projectionExpressions; }

    void setGroupByExpressions(Expressions expressions) {
        groupByExpressions = std::move(expressions);
    }
    Expressions getGroupByExpressions() const { return groupByExpressions; }

    void setAggregateExpressions(Expressions expressions) {
        aggregateExpressions = std::move(expressions);
    }
    bool hasAggregateExpressions() const { return !aggregateExpressions.empty(); }
    Expressions getAggregateExpressions() const { return aggregateExpressions; }

    void setOrderByExpressions(Expressions expressions, std::vector<bool> sortOrders) {
        orderByExpressions = std::move(expressions);
        isAscOrders = std::move(sortOrders);
    }
    bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    const Expressions& getOrderByExpressions() const { return orderByExpressions; }
    const std::vector<bool>& getSortingOrders() const { return isAscOrders; }

    void setSkipNumber(std::shared_ptr<Expression> number) { skipNumber = std::move(number); }
    bool hasSkip() const { return skipNumber != nullptr; }
    std::shared_ptr<Expression> getSkipNumber() const { return skipNumber; }

    void setLimitNumber(std::shared_ptr<Expression> number) { limitNumber = std::move(number); }
    bool hasLimit() const { return limitNumber != nullptr; }
    std::shared_ptr<Expression> getLimitNumber() const { return limitNumber; }

    bool hasSkipOrLimit() const { return hasSkip() || hasLimit(); }

private:
    BoundProjectionBody(const BoundProjectionBody& other)
        : distinct{other.distinct}, projectionExpressions{other.projectionExpressions},
          groupByExpressions{other.groupByExpressions},
          aggregateExpressions{other.aggregateExpressions},
          orderByExpressions{other.orderByExpressions}, isAscOrders{other.isAscOrders},
          skipNumber{other.skipNumber}, limitNumber{other.limitNumber} {}

private:
    bool distinct;
    Expressions projectionExpressions;
    Expressions groupByExpressions;
    Expressions aggregateExpressions;
    Expressions orderByExpressions;
    std::vector<bool> isAscOrders;
    std::shared_ptr<Expression> skipNumber;
    std::shared_ptr<Expression> limitNumber;
};

}