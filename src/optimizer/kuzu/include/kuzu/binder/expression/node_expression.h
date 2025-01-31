#pragma once

#include "kuzu/binder/expression/node_rel_expression.h"
#include "kuzu/binder/expression/property_expression.h"
#include "kuzu/binder/parse_tree_node.h"

#include <string>

namespace kuzu {
namespace binder {

class NodeExpression : public NodeOrRelExpression  {
public:
    NodeExpression(const string& uniqueName, vector<table_id_t> partitionIDs, vector<table_id_t> tableIDs)
        : NodeOrRelExpression{NODE, uniqueName, std::move(partitionIDs), std::move(tableIDs)} {}

    inline void setInternalIDProperty(unique_ptr<Expression> expression) {
        internalIDExpression = std::move(expression);
    }
    inline shared_ptr<Expression> getInternalIDProperty() {
        assert(internalIDExpression != nullptr);
        used_columns[0] = true;
        return internalIDExpression->copy();
    }
    inline string getInternalIDPropertyName() const {
        return internalIDExpression->getUniqueName();
    }

    std::string getName() override {
        return "[NodeExpr] rn=" + internalIDExpression->getRawName(); }
    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        return result;
    }

private:
    unique_ptr<Expression> internalIDExpression;
};

} // namespace binder
} // namespace kuzu
