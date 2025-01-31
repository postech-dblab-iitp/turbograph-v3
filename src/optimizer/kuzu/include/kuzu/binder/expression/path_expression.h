#pragma once

#include "kuzu/binder/expression/expression.h"
#include "kuzu/binder/expression/node_expression.h"
#include "kuzu/binder/expression/rel_expression.h"
#include "kuzu/binder/parse_tree_node.h"

#include <string>

namespace kuzu {
namespace binder {

class PathExpression : public Expression  {
public:
    PathExpression(ExpressionType exp_type, const string& uniqueName, vector<shared_ptr<NodeExpression>>& queryNodes, vector<shared_ptr<RelExpression>>& queryRels)
        : Expression{exp_type, DataTypeID::PATH, uniqueName}, queryNodes(queryNodes), queryRels(queryRels) {}

    std::string getName() override {
        return "[PathExpr] rn=" + uniqueName; }

    inline vector<shared_ptr<NodeExpression>> getQueryNodes() const {
        return queryNodes;
    }

    inline vector<shared_ptr<RelExpression>> getQueryRels() const {
        return queryRels;
    }

private:
    vector<shared_ptr<NodeExpression>> queryNodes;
    vector<shared_ptr<RelExpression>> queryRels;
};

} // namespace binder
} // namespace kuzu
