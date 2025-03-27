#pragma once

#include "planner/query/reading_clause/bound_reading_clause.hpp"
#include "planner/query/graph_pattern/query_graph.hpp"

namespace duckdb {

class BoundMatchClause final : public BoundReadingClause {
    static constexpr ClauseType clauseType_ = ClauseType::MATCH;

public:
    BoundMatchClause(std::shared_ptr<QueryGraphCollection> collection, MatchClauseType matchClauseType)
        : BoundReadingClause{clauseType_}, collection{std::move(collection)},
          matchClauseType{matchClauseType} {}

    std::shared_ptr<QueryGraphCollection> getQueryGraphCollection() const { 
        return collection; 
    }

    MatchClauseType getMatchClauseType() const { return matchClauseType; }

private:
    std::shared_ptr<QueryGraphCollection> collection;
    MatchClauseType matchClauseType;
};

}