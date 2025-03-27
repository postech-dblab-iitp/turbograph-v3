#include "planner/binder.hpp"
#include "common/exception.hpp"
#include "parser/query/regular_query.hpp"
#include "planner/query/bound_regular_query.hpp"
#include "planner/query/normalized_single_query.hpp"

namespace duckdb {

std::shared_ptr<BoundReadingClause> Binder::bindReadingClause(const ReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        return bindMatchClause(readingClause);
    }
    case ClauseType::UNWIND: {
        throw ParserException("UNWIND clause is not supported yet.");
    }
    case ClauseType::IN_QUERY_CALL: {
        throw ParserException("IN QUERY CALL clause is not supported yet.");
    }
    case ClauseType::LOAD_FROM: {
        throw ParserException("LOAD FROM clause is not supported yet.");
    }
    default:
        throw ParserException("Unsupported reading clause type.");
    }
}

}