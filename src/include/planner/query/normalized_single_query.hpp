#pragma once

#include "planner/bound_statement_result.hpp"
#include "planner/query/normalized_query_part.hpp"

namespace duckdb {

class NormalizedSingleQuery {
public:
    NormalizedSingleQuery() = default;

    inline void appendQueryPart(std::unique_ptr<NormalizedQueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline NormalizedQueryPart* getQueryPartUnsafe(uint32_t idx) { return queryParts[idx].get(); }
    inline const NormalizedQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    inline void setStatementResult(std::shared_ptr<BoundStatementResult> result) {
        statementResult = std::move(result);
    }
    inline std::shared_ptr<BoundStatementResult> getStatementResult() const { return statementResult; }

private:
    std::vector<std::unique_ptr<NormalizedQueryPart>> queryParts;
    std::shared_ptr<BoundStatementResult> statementResult;
};

} 
