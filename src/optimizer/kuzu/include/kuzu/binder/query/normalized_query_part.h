#pragma once

#include "kuzu/binder/query/reading_clause/bound_unwind_clause.h"
#include "kuzu/binder/query/reading_clause/query_graph.h"
#include "kuzu/binder/query/return_with_clause/bound_projection_body.h"
#include "kuzu/binder/query/updating_clause/bound_updating_clause.h"
#include "kuzu/binder/parse_tree_node.h"

namespace kuzu {
namespace binder {

class NormalizedQueryPart : public ParseTreeNode {
public:
    NormalizedQueryPart() = default;
    ~NormalizedQueryPart() = default;

    inline void addReadingClause(unique_ptr<BoundReadingClause> boundReadingClause) {
        readingClauses.push_back(move(boundReadingClause));
    }
    inline bool hasReadingClause() const { return !readingClauses.empty(); }
    inline uint32_t getNumReadingClause() const { return readingClauses.size(); }
    inline BoundReadingClause* getReadingClause(uint32_t idx) const {
        return readingClauses[idx].get();
    }

    inline void addUpdatingClause(unique_ptr<BoundUpdatingClause> boundUpdatingClause) {
        updatingClauses.push_back(move(boundUpdatingClause));
    }
    inline bool hasUpdatingClause() const { return !updatingClauses.empty(); }
    inline uint32_t getNumUpdatingClause() const { return updatingClauses.size(); }
    inline BoundUpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }

    inline void setProjectionBody(unique_ptr<BoundProjectionBody> boundProjectionBody) {
        projectionBody = move(boundProjectionBody);
    }
    inline bool hasProjectionBody() const { return projectionBody != nullptr; }
    inline BoundProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    inline bool hasProjectionBodyPredicate() const { return projectionBodyPredicate != nullptr; }
    inline shared_ptr<Expression> getProjectionBodyPredicate() const {
        return projectionBodyPredicate;
    }
    inline void setProjectionBodyPredicate(const shared_ptr<Expression>& predicate) {
        projectionBodyPredicate = predicate;
    }

    expression_vector getPropertiesToRead() const;

    std::list<ParseTreeNode*> getChildNodes() override { 
        std::list<ParseTreeNode*> result;
        for( auto& a: readingClauses) {
            result.push_back((ParseTreeNode*)a.get());
        }
        // for( auto& a: updatingClauses) {
        //     result.push_back((ParseTreeNode*)a.get());
        // }
        result.push_back((ParseTreeNode*)projectionBody.get());
        return result;
    }
    std::string getName() override { return "[NormalizedQueryPart]"; }

private:
    vector<unique_ptr<BoundReadingClause>> readingClauses;
    vector<unique_ptr<BoundUpdatingClause>> updatingClauses;
    unique_ptr<BoundProjectionBody> projectionBody;
    shared_ptr<Expression> projectionBodyPredicate;
};

} // namespace binder
} // namespace kuzu
