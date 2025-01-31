#pragma once

#include "kuzu/binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundProjectionBody : public ParseTreeNode{
public:
    explicit BoundProjectionBody(bool isDistinct, expression_vector projectionExpressions)
        : isDistinct{isDistinct}, projectionExpressions{move(projectionExpressions)},
          skipNumber{UINT64_MAX}, limitNumber{UINT64_MAX} {}

    BoundProjectionBody(const BoundProjectionBody& other)
        : isDistinct{other.isDistinct}, projectionExpressions{other.projectionExpressions},
          orderByExpressions{other.orderByExpressions}, isAscOrders{other.isAscOrders},
          skipNumber{other.skipNumber}, limitNumber{other.limitNumber} {}

    ~BoundProjectionBody() = default;

    inline bool getIsDistinct() const { return isDistinct; }

    inline expression_vector getProjectionExpressions() const { return projectionExpressions; }

    bool hasAggregationExpressions() const;

    void setOrderByExpressions(expression_vector expressions, vector<bool> sortOrders);

    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }

    inline const expression_vector& getOrderByExpressions() const { return orderByExpressions; }

    inline const vector<bool>& getSortingOrders() const { return isAscOrders; }

    inline void setSkipNumber(uint64_t number) { skipNumber = number; }

    inline bool hasSkip() const { return skipNumber != UINT64_MAX; }

    inline uint64_t getSkipNumber() const { return skipNumber; }

    inline void setLimitNumber(uint64_t number) { limitNumber = number; }

    inline bool hasLimit() const { return limitNumber != UINT64_MAX; }

    inline uint64_t getLimitNumber() const { return limitNumber; }

    inline bool hasSkipOrLimit() const { return hasSkip() || hasLimit(); }

    expression_vector getPropertiesToRead() const;

    inline unique_ptr<BoundProjectionBody> copy() const {
        return make_unique<BoundProjectionBody>(*this);
    }

    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        for( auto& a: projectionExpressions) {
            result.push_back((ParseTreeNode*)a.get());
        }
        for( auto& a: orderByExpressions) {
            result.push_back((ParseTreeNode*)a.get());
        }
        return result;
    }
    std::string getName() override { return "[BoundProjectionBody] projsz="+std::to_string(projectionExpressions.size()) + " orderbysz="+std::to_string(orderByExpressions.size()); }

private:
    bool isDistinct;
    expression_vector projectionExpressions;
    expression_vector orderByExpressions;
    vector<bool> isAscOrders;
    uint64_t skipNumber;
    uint64_t limitNumber;
};

} // namespace binder
} // namespace kuzu
