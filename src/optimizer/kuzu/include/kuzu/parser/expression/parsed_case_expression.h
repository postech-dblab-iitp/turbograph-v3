#pragma once

#include "kuzu/parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

struct ParsedCaseAlternative {
    unique_ptr<ParsedExpression> whenExpression;
    unique_ptr<ParsedExpression> thenExpression;

    ParsedCaseAlternative(
        unique_ptr<ParsedExpression> whenExpression, unique_ptr<ParsedExpression> thenExpression)
        : whenExpression{std::move(whenExpression)}, thenExpression{std::move(thenExpression)} {}
};

// Cypher supports 2 types of CaseExpression
// 1. CASE a.age
//    WHEN 20 THEN ...
// 2. CASE
//    WHEN a.age = 20 THEN ...
class ParsedCaseExpression : public ParsedExpression {
public:
    ParsedCaseExpression(string raw) : ParsedExpression{CASE_ELSE, std::move(raw)} {};

    inline void setCaseExpression(unique_ptr<ParsedExpression> expression) {
        caseExpression = std::move(expression);
    }
    inline bool hasCaseExpression() const { return caseExpression != nullptr; }
    inline ParsedExpression* getCaseExpression() const { return caseExpression.get(); }

    inline void addCaseAlternative(unique_ptr<ParsedCaseAlternative> caseAlternative) {
        caseAlternatives.push_back(std::move(caseAlternative));
    }
    inline uint32_t getNumCaseAlternative() const { return caseAlternatives.size(); }
    inline ParsedCaseAlternative* getCaseAlternative(uint32_t idx) const {
        return caseAlternatives[idx].get();
    }

    inline void setElseExpression(unique_ptr<ParsedExpression> expression) {
        elseExpression = std::move(expression);
    }
    inline bool hasElseExpression() const { return elseExpression != nullptr; }
    inline ParsedExpression* getElseExpression() const { return elseExpression.get(); }

private:
    // Optional. If not specified, directly check next whenExpression
    unique_ptr<ParsedExpression> caseExpression;
    vector<unique_ptr<ParsedCaseAlternative>> caseAlternatives;
    // Optional. If not specified, evaluate as null
    unique_ptr<ParsedExpression> elseExpression;
};

} // namespace parser
} // namespace kuzu
