#include "planner/binder.hpp"
#include "common/exception.hpp"
#include "parser/query/regular_query.hpp"
#include "planner/query/bound_regular_query.hpp"
#include "planner/query/normalized_single_query.hpp"

namespace duckdb {

void validateUnionColumnsOfTheSameType(
    const std::vector<std::unique_ptr<NormalizedSingleQuery>>& normalizedSingleQueries) {
    if (normalizedSingleQueries.size() <= 1) {
        return;
    }
    auto columns = normalizedSingleQueries[0]->getStatementResult()->getColumns();
    for (auto i = 1u; i < normalizedSingleQueries.size(); i++) {
        auto otherColumns = normalizedSingleQueries[i]->getStatementResult()->getColumns();
        if (columns.size() != otherColumns.size()) {
            throw BinderException("The number of columns to union/union all must be the same.");
        }
        // Check whether the dataTypes in union expressions are exactly the same in each single
        // query.
        for (auto j = 0u; j < columns.size(); j++) {
            if (columns[j]->return_type != otherColumns[j]->return_type) {
                throw BinderException("The data type of column " + columns[j]->GetName() +
                                      " is not the same in all union/union all queries.");
            }
        }
    }
}

void validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery) {
    auto unionAllExpressionCounter = 0u;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries() - 1; i++) {
        unionAllExpressionCounter += regularQuery.getIsUnionAll(i);
    }
    if ((0 < unionAllExpressionCounter) &&
        (unionAllExpressionCounter < regularQuery.getNumSingleQueries() - 1)) {
        throw BinderException("Union and union all can not be used together.");
    }
}

std::unique_ptr<BoundStatement> Binder::bindQuery(const CypherStatement& statement) {
    auto& regularQuery = dynamic_cast<const RegularQuery&>(statement);
    std::vector<std::unique_ptr<NormalizedSingleQuery>> normalizedSingleQueries;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        // Don't clear scope within bindSingleQuery() yet because it is also used for subquery
        // binding.
        scope.clear();
        normalizedSingleQueries.push_back(bindSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    validateUnionColumnsOfTheSameType(normalizedSingleQueries);
    auto boundRegularQuery = std::make_unique<BoundRegularQuery>(regularQuery.getIsUnionAll(),
        normalizedSingleQueries[0]->getStatementResult());
    for (auto& normalizedSingleQuery : normalizedSingleQueries) {
        boundRegularQuery->addSingleQuery(std::move(normalizedSingleQuery));
    }
    validateIsAllUnionOrUnionAll(*boundRegularQuery);
    return boundRegularQuery;
}


std::unique_ptr<NormalizedSingleQuery> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    return nullptr;
}

}