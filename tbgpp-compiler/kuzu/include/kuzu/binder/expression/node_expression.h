#pragma once

#include "node_rel_expression.h"
#include "property_expression.h"
#include "binder/parse_tree_node.h"

#include <string>

namespace kuzu {
namespace binder {

class NodeExpression : public NodeOrRelExpression, ParseTreeNode  {
public:
    NodeExpression(const string& uniqueName, vector<table_id_t> tableIDs)
        : NodeOrRelExpression{NODE, uniqueName, std::move(tableIDs)} {}

    inline void setInternalIDProperty(unique_ptr<Expression> expression) {
        internalIDExpression = std::move(expression);
    }
    inline shared_ptr<Expression> getInternalIDProperty() const {
        assert(internalIDExpression != nullptr);
        return internalIDExpression->copy();
    }
    inline string getInternalIDPropertyName() const {
        return internalIDExpression->getUniqueName();
    }

    std::string getName() override {
        return "[NodeExpr] rn=" + internalIDExpression->getRawName(); }
    std::list<ParseTreeNode*> getChildren() override { 
        std::list<ParseTreeNode*> result;
        return result;
    }

private:
    unique_ptr<Expression> internalIDExpression;
};

} // namespace binder
} // namespace kuzu
