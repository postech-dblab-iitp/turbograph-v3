#include "execution/base_aggregate_hashtable.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"

namespace s62 {

BaseAggregateHashTable::BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> payload_types_p)
    : buffer_manager(buffer_manager), payload_types(move(payload_types_p)) {
}

} // namespace s62
