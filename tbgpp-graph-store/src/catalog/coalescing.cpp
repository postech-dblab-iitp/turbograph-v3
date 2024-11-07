#include "catalog/coalescing.hpp"


namespace duckdb {

Coalescing::GroupingAlgorithm Coalescing::grouping_algo =
    Coalescing::GroupingAlgorithm::DEFAULT;

}