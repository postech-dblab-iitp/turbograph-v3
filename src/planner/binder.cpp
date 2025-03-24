#include "planner/binder.hpp"
#include "common/exception.hpp"
#include "parser/cypher_statement.hpp"

namespace duckdb {

std::unique_ptr<BoundStatement> Binder::bind(const CypherStatement& statement) {
    switch (statement.type) {
        case StatementType::SELECT_STATEMENT:
            return bindQuery(statement);
        default: 
            throw BinderException("Unsupported statement type");
            return nullptr;
    }
}

}