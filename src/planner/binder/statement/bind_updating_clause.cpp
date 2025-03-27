#include "planner/binder.hpp"
#include "common/exception.hpp"

namespace duckdb {

std::shared_ptr<BoundUpdatingClause> Binder::bindUpdatingClause(
    const UpdatingClause& updatingClause) {
    throw BinderException("Updating clause binding is not implemented yet.");
    return nullptr;
}

}