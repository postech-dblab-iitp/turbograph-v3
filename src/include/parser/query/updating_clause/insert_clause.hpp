#pragma once

#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"

namespace duckdb {

class InsertClause final : public UpdatingClause {
public:
    explicit InsertClause(std::vector<PatternElement> patternElements)
        : UpdatingClause{ClauseType::INSERT},
          patternElements{std::move(patternElements)} {};

    inline const std::vector<PatternElement>& getPatternElementsRef() const {
        return patternElements;
    }

private:
    std::vector<PatternElement> patternElements;
};

}