#pragma once

#include <list>
#include <string>

using namespace kuzu::common;

namespace kuzu {
namespace binder {

class ParseTreeNode {
public:

    virtual std::list<ParseTreeNode*> getChildNodes() { return std::list<ParseTreeNode*>(); }
    virtual std::string getName() { return "[_ParseTreeNode] _base"; }

    ParseTreeNode* generateLeafNode(std::string name, std::string content) {
        assert(false);
        return nullptr;
    }
};

}
}