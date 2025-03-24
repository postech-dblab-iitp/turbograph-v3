#pragma once

#include "planner/query/reading_clause/bound_reading_clause.hpp"
#include "planner/query/return_with_clause/bound_projection_body.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class NormalizedQueryPart {
public:
    NormalizedQueryPart() = default;

    void addReadingClause(std::unique_ptr<BoundReadingClause> boundReadingClause) {
        readingClauses.push_back(std::move(boundReadingClause));
    }
    bool hasReadingClause() const { return !readingClauses.empty(); }
    uint32_t getNumReadingClause() const { return readingClauses.size(); }
    BoundReadingClause* getReadingClause(uint32_t idx) const { return readingClauses[idx].get(); }

    void setProjectionBody(std::unique_ptr<BoundProjectionBody> boundProjectionBody) {
        projectionBody = std::move(boundProjectionBody);
    }
    bool hasProjectionBody() const { return projectionBody != nullptr; }
    BoundProjectionBody* getProjectionBodyUnsafe() {
        return projectionBody.get();
    }
    const BoundProjectionBody* getProjectionBody() const {
        return projectionBody.get();
    }

    bool hasProjectionBodyPredicate() const { return projectionBodyPredicate != nullptr; }
    std::shared_ptr<Expression> getProjectionBodyPredicate() const {
        return projectionBodyPredicate;
    }
    void setProjectionBodyPredicate(const std::shared_ptr<Expression>& predicate) {
        projectionBodyPredicate = predicate;
    }

private:
    std::vector<std::unique_ptr<BoundReadingClause>> readingClauses;
    std::unique_ptr<BoundProjectionBody> projectionBody;
    std::shared_ptr<Expression> projectionBodyPredicate;
};

}
