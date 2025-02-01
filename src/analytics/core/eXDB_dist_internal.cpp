#include "analytics/core/eXDB_dist_internal.hpp"

__thread int64_t core_id::my_core_id_ = -1;
int64_t core_id::core_counts_ = 0;

void core_id::set_core_ids(int nTs) {
	#pragma omp parallel num_threads(nTs)
	{
		set_my_core_id(omp_get_thread_num());
	}
}

