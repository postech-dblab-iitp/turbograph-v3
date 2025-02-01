#pragma once

#include "kuzu/binder/query/bound_single_query.h"
#include "kuzu/binder/query/normalized_single_query.h"

namespace kuzu {
namespace binder {

class QueryNormalizer {

public:
    static unique_ptr<NormalizedSingleQuery> normalizeQuery(const BoundSingleQuery& singleQuery);

private:
    static unique_ptr<BoundQueryPart> normalizeFinalMatchesAndReturnAsQueryPart(
        const BoundSingleQuery& singleQuery);

    static unique_ptr<NormalizedQueryPart> normalizeQueryPart(const BoundQueryPart& queryPart);
};

} // namespace binder
} // namespace kuzu
