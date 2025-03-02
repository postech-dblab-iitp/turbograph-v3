#pragma once

#include "kuzu/common/exception.h"
#include "kuzu/binder/expression/node_expression.h"
#include "kuzu/binder/parse_tree_node.h"


namespace kuzu {
namespace binder {

class RelExpression : public NodeOrRelExpression {
public:
    RelExpression(const string& uniqueName, vector<table_id_t> partitionIDs, shared_ptr<NodeExpression> srcNode,
        shared_ptr<NodeExpression> dstNode, uint64_t lowerBound, uint64_t upperBound)
        : NodeOrRelExpression{DataTypeID::REL, uniqueName, std::move(partitionIDs),  NodeRelType::EDGE}, srcNode{std::move(srcNode)},
          dstNode{std::move(dstNode)}, lowerBound{lowerBound}, upperBound{upperBound} {}

    inline bool isBoundByMultiLabeledNode() const {
        return srcNode->isMultiLabeled() || dstNode->isMultiLabeled();
    }

    inline shared_ptr<NodeExpression> getSrcNode() const { return srcNode; }
    inline string getSrcNodeName() const { return srcNode->getUniqueName(); }
    inline shared_ptr<NodeExpression> getDstNode() const { return dstNode; }
    inline string getDstNodeName() const { return dstNode->getUniqueName(); }

    inline uint64_t getLowerBound() const { return lowerBound; }
    inline uint64_t getUpperBound() const { return upperBound; }
    inline bool isVariableLength() const { return !(lowerBound == 1 && upperBound == 1); }

    inline bool hasInternalIDProperty() const { return hasPropertyExpression(INTERNAL_ID_PROPERTY_KEY_ID); }
    inline shared_ptr<Expression> getInternalIDProperty() {
        return getPropertyExpression(INTERNAL_ID_PROPERTY_KEY_ID);
    }

    std::string getName() override {
        return "[RelExpr] src=" + srcNode->getRawName() + ", dst=" + dstNode->getRawName() ; }
    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        return result;
    }

private:
    shared_ptr<NodeExpression> srcNode;
    shared_ptr<NodeExpression> dstNode;
    uint64_t lowerBound;    // can start from 0
    uint64_t upperBound;    // can be -1, which is infinite loop
};

} // namespace binder
} // namespace kuzu
