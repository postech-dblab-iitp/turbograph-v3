#pragma once

#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "parser/cypher_statement.hpp"
#include "parser/tokens.hpp"
#include "planner/bound_statement.hpp"
#include "planner/bound_tokens.hpp"
#include "planner/binder_scope.hpp"

namespace duckdb {

class Binder {
public:
    explicit Binder(std::shared_ptr<ClientContext>& client)
        : lastExpressionId{0}, client{client} {}

    std::unique_ptr<BoundStatement> bind(const CypherStatement& statement);

    /*** bind query ***/
    std::unique_ptr<BoundStatement> bindQuery(const CypherStatement& statement);
    std::unique_ptr<NormalizedSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

private:
    std::shared_ptr<ClientContext> client;
    duckdb::idx_t lastExpressionId;
    BinderScope scope;
};

}