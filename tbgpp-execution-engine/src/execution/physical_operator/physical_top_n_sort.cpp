#include "typedef.hpp"

#include "execution/physical_operator/physical_top_n_sort.hpp"

#include "execution/expression_executor.hpp"

#include <string>

#include "execution/physical_operator.hpp"
#include "common/allocator.hpp"

#include "icecream.hpp"

namespace duckdb {


std::string PhysicalTopNSort::ParamsToString() const {
	return "topnsort-param";
}

std::string PhysicalTopNSort::ToString() const {
	return "TopNSort";
}


}