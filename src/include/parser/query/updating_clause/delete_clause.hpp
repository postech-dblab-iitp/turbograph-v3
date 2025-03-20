#pragma once

#include "common/enums/delete_type.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"

namespace duckdb {

class DeleteClause final : public UpdatingClause {
public:
    explicit DeleteClause(DeleteNodeType deleteType)
        : UpdatingClause{ClauseType::DELETE_}, deleteType{deleteType} {};

    void addExpression(std::unique_ptr<ParsedExpression> expression) {
        expressions.push_back(std::move(expression));
    }
    DeleteNodeType getDeleteClauseType() const { return deleteType; }
    uint32_t getNumExpressions() const { return expressions.size(); }
    ParsedExpression* getExpression(uint32_t idx) const { return expressions[idx].get(); }

private:
    DeleteNodeType deleteType;
    ParsedExpressions expressions;
};

}
