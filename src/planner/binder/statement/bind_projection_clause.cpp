#include "planner/binder.hpp"
#include "common/exception.hpp"

namespace duckdb {


std::shared_ptr<BoundWithClause> Binder::bindWithClause(const WithClause& withClause) {
    return nullptr;
}

std::shared_ptr<BoundReturnClause> Binder::bindReturnClause(const ReturnClause& returnClause) {
    return nullptr;

}

}