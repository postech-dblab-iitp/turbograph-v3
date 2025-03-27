#include "planner/binder.hpp"
#include "common/exception.hpp"

namespace duckdb {

// A graph pattern contains node/rel and a set of key-value pairs associated with the variable. We
// bind node/rel as query graph and key-value pairs as a separate collection. This collection is
// interpreted in two different ways.
//    - In MATCH clause, these are additional predicates to WHERE clause
//    - In UPDATE clause, there are properties to set.
// We do not store key-value pairs in query graph primarily because we will merge key-value
// std::pairs with other predicates specified in WHERE clause.
std::shared_ptr<BoundGraphPattern> Binder::bindGraphPattern(const std::vector<PatternElement>& graphPattern) {
    return nullptr;
}

}