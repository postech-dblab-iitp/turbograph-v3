#include "planner/binder.hpp"
#include "common/exception.hpp"
#include "parser/query/reading_clause/match_clause.hpp"
#include "planner/query/reading_clause/bound_match_clause.hpp"

namespace duckdb {

std::shared_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = (MatchClause&)readingClause;
    auto boundGraphPattern = bindGraphPattern(matchClause.getPatternElementsRef());
    if (matchClause.hasWherePredicate()) {
        boundGraphPattern->where = bindWhereExpression(*matchClause.getWherePredicate());
    }
    rewriteMatchPattern(boundGraphPattern);
    auto boundMatch = std::make_shared<BoundMatchClause>(
        std::move(boundGraphPattern->queryGraphCollection), matchClause.getMatchClauseType());
    boundMatch->setPredicate(boundGraphPattern->where);
    return boundMatch;
}

void Binder::rewriteMatchPattern(std::shared_ptr<BoundGraphPattern>& boundGraphPattern) {
    // Rewrite self loop edge
    // e.g. rewrite (a)-[e]->(a) as [a]-[e]->(b) WHERE id(a) = id(b)
    // Expressions selfLoopEdgePredicates;
    // auto& graphCollection = boundGraphPattern->queryGraphCollection;
    // for (auto i = 0u; i < graphCollection.getNumQueryGraphs(); ++i) {
    //     auto queryGraph = graphCollection.getQueryGraphUnsafe(i);
    //     for (auto& queryRel : queryGraph->getQueryRels()) {
    //         if (!queryRel->isSelfLoop()) {
    //             continue;
    //         }
    //         auto src = queryRel->getSrcNode();
    //         auto dst = queryRel->getDstNode();
    //         auto newDst = createQueryNode(dst->getVariableName(), dst->getEntries());
    //         queryGraph->addQueryNode(newDst);
    //         queryRel->setDstNode(newDst);
    //         auto predicate = expressionBinder.createEqualityComparisonExpression(
    //             src->getInternalID(), newDst->getInternalID());
    //         selfLoopEdgePredicates.push_back(std::move(predicate));
    //     }
    // }
    // if (selfLoopEdgePredicates.size() >= 1) {
    //     throw BinderException("Self loop edge is not supported yet.");
    // }
}

}