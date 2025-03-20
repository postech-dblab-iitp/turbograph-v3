#pragma once

#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"

namespace duckdb {

class MergeClause final : public UpdatingClause {
public:
    explicit MergeClause(std::vector<PatternElement> patternElements)
        : UpdatingClause{ClauseType::MERGE}, patternElements{std::move(patternElements)} {}

    inline const std::vector<PatternElement>& getPatternElementsRef() const {
        return patternElements;
    }
    inline void addOnMatchSetItems(ParsedExpressionPair setItem) {
        onMatchSetItems.push_back(std::move(setItem));
    }
    inline bool hasOnMatchSetItems() const { return !onMatchSetItems.empty(); }
    inline const std::vector<ParsedExpressionPair>& getOnMatchSetItemsRef() const {
        return onMatchSetItems;
    }

    inline void addOnCreateSetItems(ParsedExpressionPair setItem) {
        onCreateSetItems.push_back(std::move(setItem));
    }
    inline bool hasOnCreateSetItems() const { return !onCreateSetItems.empty(); }
    inline const std::vector<ParsedExpressionPair>& getOnCreateSetItemsRef() const {
        return onCreateSetItems;
    }

private:
    std::vector<PatternElement> patternElements;
    std::vector<ParsedExpressionPair> onMatchSetItems;
    std::vector<ParsedExpressionPair> onCreateSetItems;
};

} 
