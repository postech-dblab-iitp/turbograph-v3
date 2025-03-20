#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"

namespace duckdb {

class SetClause final : public UpdatingClause {
public:
    SetClause() : UpdatingClause{ClauseType::SET} {};

    inline void addSetItem(ParsedExpressionPair setItem) { setItems.push_back(std::move(setItem)); }
    inline const std::vector<ParsedExpressionPair>& getSetItemsRef() const { return setItems; }

private:
    std::vector<ParsedExpressionPair> setItems;
};

}