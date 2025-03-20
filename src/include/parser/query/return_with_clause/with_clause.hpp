#pragma once

#include "parser/query/return_with_clause/return_clause.hpp"

namespace duckdb {

class WithClause : public ReturnClause {
public:
    explicit WithClause(std::unique_ptr<ProjectionBody> projectionBody)
        : ReturnClause{std::move(projectionBody)} {}

    inline void setWhereExpression(std::unique_ptr<ParsedExpression> expression) {
        whereExpression = std::move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline ParsedExpression* getWhereExpression() const { return whereExpression.get(); }

    std::string ToString() const override {
        string result = "WITH " + getProjectionBody()->ToString();
        if (hasWhereExpression()) {
            result += " WHERE " + getWhereExpression()->ToString();
        }
        return result;
    }

private:
    std::unique_ptr<ParsedExpression> whereExpression;
};

}