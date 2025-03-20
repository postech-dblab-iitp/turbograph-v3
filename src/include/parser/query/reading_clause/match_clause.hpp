#pragma once

#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/query/reading_clause/reading_clause.hpp"
#include "common/string_util.hpp"

namespace duckdb {

class MatchClause : public ReadingClause {
    static constexpr ClauseType clauseType_ = ClauseType::MATCH;

public:
    MatchClause(std::vector<PatternElement> patternElements,
        MatchClauseType matchClauseType)
        : ReadingClause{clauseType_}, patternElements{std::move(patternElements)},
          matchClauseType{matchClauseType} {}

    const std::vector<PatternElement>& getPatternElementsRef() const { return patternElements; }

    MatchClauseType getMatchClauseType() const { return matchClauseType; }

    std::string ToString() const override {
        string result = (matchClauseType == MatchClauseType::OPTIONAL_MATCH) ? "OPTIONAL MATCH " : "MATCH ";

        result += StringUtil::Join(
            patternElements, patternElements.size(), ", ",
            [](const PatternElement &element) { return element.ToString(); });

        if(hasWherePredicate()) {
            result += " WHERE " + getWherePredicate()->ToString();
        }

        return result;
    }
private:

    std::vector<PatternElement> patternElements;
    MatchClauseType matchClauseType;
};

}