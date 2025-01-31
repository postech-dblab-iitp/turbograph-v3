#pragma once

#include "common/type_utils.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedIdInCollExpression : public ParsedExpression {
public:
    ParsedIdInCollExpression(string var_name, unique_ptr<ParsedExpression> expr, string rawName)
        : ParsedExpression(ID_IN_COLL, std::move(expr), rawName), var_name(var_name) {}

    inline string getVarName() const { return var_name; }
private:
    string var_name;
};

} // namespace parser
} // namespace kuzu
