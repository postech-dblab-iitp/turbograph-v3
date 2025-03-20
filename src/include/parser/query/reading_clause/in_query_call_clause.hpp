#pragma once

#include "parser/query/reading_clause/reading_clause.hpp"
#include "parser/query/reading_clause/yield_variable.hpp"

namespace duckdb {

class InQueryCallClause final : public ReadingClause {
    static constexpr ClauseType clauseType_ = ClauseType::IN_QUERY_CALL;

public:
    InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpression,
        std::vector<YieldVariable> yieldClause)
        : ReadingClause{clauseType_}, functionExpression{std::move(functionExpression)},
          yieldVariables{std::move(yieldClause)} {}

    const ParsedExpression* getFunctionExpression() const { return functionExpression.get(); }

    const std::vector<YieldVariable>& getYieldVariables() const { return yieldVariables; }

    std::string ToString() const override {
        std::stringstream ss;
        ss << "CALL " << functionExpression->ToString();

        if (!yieldVariables.empty()) {
            ss << " YIELD ";
            bool first = true;
            for (const auto &yieldVar : yieldVariables) {
                if (!first) {
                    ss << ", ";
                }
                ss << yieldVar.ToString();
                first = false;
            }
        }

        return ss.str();
    }

private:
    std::unique_ptr<ParsedExpression> functionExpression;
    std::vector<YieldVariable> yieldVariables;
};

}