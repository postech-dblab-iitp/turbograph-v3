#pragma once

#include <cstdint>
#include <list>

#include "kuzu/binder/bound_statement_result.h"
#include "kuzu/binder/parse_tree_node.h"
#include "kuzu/common/statement_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

class BoundStatement : public ParseTreeNode {
public:
    explicit BoundStatement(
        StatementType statementType, unique_ptr<BoundStatementResult> statementResult)
        : statementType{statementType}, statementResult{std::move(statementResult)} {}

    virtual ~BoundStatement() = default;

    inline StatementType getStatementType() const { return statementType; }

    inline BoundStatementResult* getStatementResult() const { return statementResult.get(); }

    inline bool isDDL() const { return StatementTypeUtils::isDDL(statementType); }
    inline bool isCopyCSV() const { return StatementTypeUtils::isCopyCSV(statementType); }

    virtual inline bool isReadOnly() const { return false; }

private:
    StatementType statementType;
    unique_ptr<BoundStatementResult> statementResult;
};

} // namespace binder
} // namespace kuzu
