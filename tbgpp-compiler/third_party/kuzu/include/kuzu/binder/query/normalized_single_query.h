#pragma once

#include "binder/bound_statement_result.h"
#include "normalized_query_part.h"
#include "binder/parse_tree_node.h"

namespace kuzu {
namespace binder {

class NormalizedSingleQuery : public ParseTreeNode {
public:
    NormalizedSingleQuery() = default;
    ~NormalizedSingleQuery() = default;

    inline bool isReadOnly() const {
        for (auto& queryPart : queryParts) {
            if (queryPart->hasUpdatingClause()) {
                return false;
            }
        }
        return true;
    }

    inline void appendQueryPart(unique_ptr<NormalizedQueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline NormalizedQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    expression_vector getPropertiesToRead() const;

    std::list<ParseTreeNode*> getChildren() override { 
        std::list<ParseTreeNode*> result;
        for( auto& a: queryParts) {
            result.push_back((ParseTreeNode*)a.get());
        }
        return result;
    }
    std::string getName() override { return "[NormalizedSingleQuery]"; }

private:
    vector<unique_ptr<NormalizedQueryPart>> queryParts;
};

} // namespace binder
} // namespace kuzu
