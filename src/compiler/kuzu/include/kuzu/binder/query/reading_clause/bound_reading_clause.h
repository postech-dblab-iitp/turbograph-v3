#pragma once

#include <memory>

#include "binder/expression/expression.h"
#include "common/clause_type.h"
#include "binder/parse_tree_node.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

class BoundReadingClause : public ParseTreeNode {

public:
    explicit BoundReadingClause(ClauseType clauseType) : clauseType{clauseType} {}
    virtual ~BoundReadingClause() = default;

    ClauseType getClauseType() const { return clauseType; }

    inline virtual expression_vector getSubPropertyExpressions() const = 0;

    inline virtual unique_ptr<BoundReadingClause> copy() = 0;

private:
    ClauseType clauseType;
};
} // namespace binder
} // namespace kuzu
