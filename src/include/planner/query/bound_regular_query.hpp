#pragma once

#include "planner/bound_statement.hpp"
#include "planner/query/normalized_single_query.hpp"

namespace duckdb {

class BoundRegularQuery final : public BoundStatement {

public:
    explicit BoundRegularQuery(std::vector<bool> isUnionAll, std::shared_ptr<BoundStatementResult> statementResult)
        : BoundStatement{StatementType::SELECT_STATEMENT, std::move(statementResult)},
          isUnionAll{std::move(isUnionAll)} {}

    void addSingleQuery(std::shared_ptr<NormalizedSingleQuery> singleQuery) {
        singleQueries.push_back(std::move(singleQuery));
    }
    uint64_t getNumSingleQueries() const { return singleQueries.size(); }
    std::shared_ptr<NormalizedSingleQuery> getSingleQuery(duckdb::idx_t idx) const {
        return singleQueries[idx];
    }

    bool getIsUnionAll(duckdb::idx_t idx) const { return isUnionAll[idx]; }

private:
    std::vector<std::shared_ptr<NormalizedSingleQuery>> singleQueries;
    std::vector<bool> isUnionAll;
};

}