#include "parser/transformer.hpp"
#include "parser/query/reading_clause/match_clause.hpp"

namespace duckdb {

std::unique_ptr<ReadingClause> Transformer::transformReadingClause(
    CypherParser::OC_ReadingClauseContext& ctx) {
    if (ctx.oC_Match()) {
        return transformMatch(*ctx.oC_Match());
    } else if (ctx.oC_Unwind()) {
        throw ParserException("UNWIND is not supported.");
    } else if (ctx.kU_InQueryCall()) {
        throw ParserException("CALL is not supported.");
    } else if (ctx.kU_LoadFrom()) {
        throw ParserException("LOAD FROM is not supported.");
    }
}

std::unique_ptr<ReadingClause> Transformer::transformMatch(CypherParser::OC_MatchContext& ctx) {
    auto matchClauseType =
        ctx.OPTIONAL() ? MatchClauseType::OPTIONAL_MATCH : MatchClauseType::MATCH;
    auto matchClause =
        std::make_unique<MatchClause>(transformPattern(*ctx.oC_Pattern()), matchClauseType);
    if (ctx.oC_Where()) {
        matchClause->setWherePredicate(transformWhere(*ctx.oC_Where()));
    }
    if (ctx.kU_Hint()) {
        throw ParserException("HINT is not supported.");
    }
    return matchClause;
}


}