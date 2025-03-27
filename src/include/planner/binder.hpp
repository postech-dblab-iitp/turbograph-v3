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
    std::unique_ptr<BoundStatement> bindQuery(const RegularQuery& regularQuery);
    std::shared_ptr<NormalizedSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);
    std::shared_ptr<NormalizedQueryPart> bindQueryPart(const QueryPart& queryPart);

    /*** bind where ***/
    std::shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    /*** bind reading clause ***/
    std::shared_ptr<BoundReadingClause> bindReadingClause(const ReadingClause& readingClause);
    std::shared_ptr<BoundReadingClause> bindMatchClause(const ReadingClause& readingClause);
    void rewriteMatchPattern(std::shared_ptr<BoundGraphPattern>& boundGraphPattern);

    /*** bind projection clause ***/
    std::shared_ptr<BoundWithClause> bindWithClause(const WithClause& withClause);
    std::shared_ptr<BoundReturnClause> bindReturnClause(const ReturnClause& returnClause);

    /*** bind updating clause ***/
    std::shared_ptr<BoundUpdatingClause> bindUpdatingClause(const UpdatingClause& updatingClause);

    /*** bind graph pattern ***/
    std::shared_ptr<BoundGraphPattern> bindGraphPattern(const std::vector<PatternElement>& graphPattern);

private:
    std::shared_ptr<ClientContext> client;
    duckdb::idx_t lastExpressionId;
    BinderScope scope;
};

}