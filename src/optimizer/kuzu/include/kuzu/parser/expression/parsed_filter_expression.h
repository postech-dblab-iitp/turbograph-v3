#pragma once

#include "kuzu/common/type_utils.h"


namespace kuzu {
namespace parser {

class ParsedFilterExpression : public ParsedExpression {
public:
    ParsedFilterExpression(unique_ptr<ParsedExpression> id_in_coll_expr, string rawName)
        : ParsedExpression(FILTER, std::move(id_in_coll_expr), rawName) {}

    inline void setWhereClause(unique_ptr<ParsedExpression> expression) {
        whereClause = std::move(expression);
    }
    inline bool hasWhereClause() const { return whereClause != nullptr; }
    inline ParsedExpression *getWhereClause() const { return whereClause.get(); }
private:
    unique_ptr<ParsedExpression> whereClause;
};

} // namespace parser
} // namespace kuzu
