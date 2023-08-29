#include <iomanip>
#include <stdarg.h>
#include "parallel/turbo_dist_internal.hpp"

duckdb::PartitionID num_partitions__ = -1;
duckdb::PartitionID my_partition_id__ = 0;


duckdb::PartitionID& PartitionStatistics::num_partitions_ = num_partitions__;
duckdb::PartitionID& PartitionStatistics::my_partition_id_ = my_partition_id__;




void PartitionStatistics::init() {
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);	/* get current process id */
	MPI_Comm_size(MPI_COMM_WORLD, &size);	/* get number of processes */
	my_partition_id__ = rank;

	num_partitions__ = size;

	PartitionStatistics::wait_for_all();
}

void  PartitionStatistics::close() {
	// per_partition_num_nodes_.close();
	// per_machine_first_node_id_.close();
	// per_machine_last_node_id_.close();

	// for (auto& timer: per_partition_timers_) {
	// 	timer.close();
	// }
	// for (auto& timer: per_partition_minor_timers_) {
	// 	timer.close();
	// }
}

duckdb::PartitionID& PartitionStatistics::num_machines() {
	return num_partitions_;
}

duckdb::PartitionID& PartitionStatistics::my_machine_id() {
	return my_partition_id_;
}

void PartitionStatistics::wait_for_all() {
	MPI_Barrier(MPI_COMM_WORLD);
}


int64_t LocalStatistics::page_buffer_size_in_bytes_ = -1;


std::vector<double> LocalStatistics::timers_[NTimers];
double LocalStatistics::timers_prv[NTimers];
std::vector<double> LocalStatistics::minor_timers_[NTimers];
double LocalStatistics::minor_timers_prv[NTimers];

std::map<std::string, int64_t> LocalStatistics::mem_alloc_info;


// Partition-aware APIs
// bool LocalStatistics::IsInternalVertex(node_t node_id) {
// 	return PartitionStatistics::my_first_internal_vid() <= node_id && node_id <= PartitionStatistics::my_last_internal_vid();
// }

int64_t& LocalStatistics::page_buffer_size_in_bytes() {
	return page_buffer_size_in_bytes_;
}


// node_t LocalStatistics::first_local_internal_vid() {
// 	return 0;
// }
// node_t LocalStatistics::last_local_internal_vid() {
// 	return PartitionStatistics::my_num_internal_nodes() - 1;
// }

//
//		first_local_vid																		last_local_vid
// first_local_internal_vid		last_local_internal_vid		first_local_cut_vid				last_local_cut_vid
//															first_cut_vertex_table_index	last_cut_vertex_table_index
// [					...					]						[				...				]

// (Global) VertexID to (Local) VertexID

void LocalStatistics::register_mem_alloc_info(const char* name, int64_t MBytes) {
	std::string str_name(name);
	if(MBytes != 0) {
		mem_alloc_info[str_name] += MBytes;
	} else {
        mem_alloc_info.erase(str_name);
        //mem_alloc_info[str_name] = 0;
	}
	if (PartitionStatistics::my_machine_id() == 0) {
        // printProcessMemoryUsage(name, MBytes, get_total_mem_size_allocated());
    }
}

void LocalStatistics::print_mem_alloc_info() {
	if (PartitionStatistics::my_machine_id() == 0) {
		std::cout << "======================================================== Memory Allocation Info =======================================================" << std::endl;
		int64_t sum = LocalStatistics::get_total_mem_size_allocated();
		for (auto& elem : mem_alloc_info) {
            std::cout << elem.first << "\t" << elem.second << " MB" << std::endl;
        }
        std::cout << "[Current Total Memory Usage]" << "\t" << sum << " MB" << std::endl;
		std::cout << "=======================================================================================================================================" << std::endl;
	}
	PartitionStatistics::wait_for_all();
}


int64_t LocalStatistics::get_total_mem_size_allocated() {
	int64_t sum = 0;
	for (auto& elem : mem_alloc_info) {
		sum += elem.second;
	}
	return sum;
}