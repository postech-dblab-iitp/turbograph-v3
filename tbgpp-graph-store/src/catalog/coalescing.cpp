#include "catalog/coalescing.hpp"


namespace s62 {

Coalescing::GroupingAlgorithm Coalescing::grouping_algo =
    Coalescing::GroupingAlgorithm::MERGEALL;

}