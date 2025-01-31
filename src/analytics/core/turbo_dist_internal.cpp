#include <iomanip>
#include <stdarg.h>
#include "turbo_dist_internal.hpp"

Statistics* final_statistics_[N_PERF_METRICS];

PartitionID num_partitions__ = -1;
PartitionID my_partition_id__ = 0;

PartitionID& PartitionStatistics::num_partitions_ = num_partitions__;
PartitionID& PartitionStatistics::my_partition_id_ = my_partition_id__;

int64_t PartitionStatistics::my_total_num_subchunks_ = -1;
int64_t PartitionStatistics::num_subchunks_per_edge_chunk_ = -1;

per_partition_elem<node_t> PartitionStatistics::per_partition_num_nodes_;
per_partition_elem<node_t> PartitionStatistics::per_machine_first_node_id_;
per_partition_elem<node_t> PartitionStatistics::per_machine_last_node_id_;

node_t PartitionStatistics::num_total_nodes_ = -1;
node_t PartitionStatistics::max_num_nodes_per_vector_chunk_ = -1;

//const int PartitionStatistics::NTimers = 32;
per_partition_array<double> PartitionStatistics::per_partition_timers_[NTimers];
per_partition_array<double> PartitionStatistics::per_partition_minor_timers_[NTimers];

std::vector<int64_t> PartitionStatistics::rget_amount_per_partition;
std::vector<int64_t> PartitionStatistics::raccum_amount_per_partition;
int64_t PartitionStatistics::num_target_vector_chunks_;
std::vector<Range<int64_t>> PartitionStatistics::per_machine_vector_chunks_;
Range<int64_t> PartitionStatistics::my_vector_chunks_;


std::vector<Range<node_t> > PartitionStatistics::vector_chunk_id_to_vid_range_;

/*
 * ################################################
 * Local Statistics
 * #################################################
 */

int64_t LocalStatistics::page_buffer_size_in_bytes_ = -1;


std::vector<double> LocalStatistics::timers_[NTimers];
double LocalStatistics::timers_prv[NTimers];
std::vector<double> LocalStatistics::minor_timers_[NTimers];
double LocalStatistics::minor_timers_prv[NTimers];

std::map<std::string, int64_t> LocalStatistics::mem_alloc_info;

void system_fprintf(int machine_id, FILE* stream, const char* format, ...) {
    if(PartitionStatistics::my_machine_id() == machine_id){
        va_list args;
        va_start (args, format);
        vfprintf(stream, format, args);
        va_end (args);
    }
}

void JsonLogger::OpenLogger(const char* log_file_name, bool is_local_) {
    is_local = is_local_;
    if (is_local) {
        log_file = std::fopen(log_file_name, "w");
        INVARIANT(log_file != NULL);
    } else {
        if (PartitionStatistics::my_machine_id() == 0){
            log_file = std::fopen(log_file_name, "w");
            if (log_file == NULL)
                fprintf(stdout, "[%ld] %s null\n", PartitionStatistics::my_machine_id(), log_file_name);
            INVARIANT(log_file != NULL);
        }
    }
}

void JsonLogger::WriteIntoDisk() {
    if (is_local) {
		json11::Json obj = json11::Json(json_map);
		std::string json_result = obj.dump();
		fprintf(log_file, "%s", json_result.c_str());
    } else {
        if (PartitionStatistics::my_machine_id() == 0){
            json11::Json obj = json11::Json(json_map);
            std::string json_result = obj.dump();
            fprintf(log_file, "%s", json_result.c_str());
        }
    }
}

void JsonLogger::InsertKeyValue(const char* key, json11::Json value){
    if (is_local) {
        json_map.insert(std::make_pair(key, value));
    } else {
        if (PartitionStatistics::my_machine_id() == 0){
            json_map.insert(std::make_pair(key, value));
        }
    }
}

void JsonLogger::Close() {
    if (is_local) {
        int rs = std::fclose(log_file);
        INVARIANT(rs == 0);
    } else {
        if(PartitionStatistics::my_machine_id() == 0){
            int rs = std::fclose(log_file);
            INVARIANT(rs == 0);
        }
    }
    is_local = false;
}


void MPI_Win_Info_Dump(MPI_Win& win) {
	int      i, j, rank;
	MPI_Info info_in, info_out;
	int      errors = 0, all_errors = 0;
	void    *base;
	char     invalid_key[] = "invalid_test_key";
	char     buf[MPI_MAX_INFO_VAL];
	int      flag;
	rank = 0;

	if (PartitionStatistics::my_machine_id() == 0) {
		MPI_Win_get_info(win, &info_out);

		MPI_Info_get(info_out, "no_locks", MPI_MAX_INFO_VAL, buf, &flag);
		if (flag) printf("%d: no_locks = %s\n", rank, buf);

		MPI_Info_get(info_out, "accumulate_ordering", MPI_MAX_INFO_VAL, buf, &flag);
		if (flag) printf("%d: accumulate_ordering = %s\n", rank, buf);

		MPI_Info_get(info_out, "accumulate_ops", MPI_MAX_INFO_VAL, buf, &flag);
		if (flag) printf("%d: accumulate_ops = %s\n", rank, buf);

		MPI_Info_get(info_out, "same_size", MPI_MAX_INFO_VAL, buf, &flag);
		if (flag) printf("%d: same_size = %s\n", rank, buf);

		MPI_Info_get(info_out, "alloc_shm", MPI_MAX_INFO_VAL, buf, &flag);
		if (flag) printf("%d: alloc_shm = %s\n", rank, buf);
	}
}

void PartitionStatistics::init() {
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);	/* get current process id */
	MPI_Comm_size(MPI_COMM_WORLD, &size);	/* get number of processes */
	my_partition_id__ = rank;

	printf("Shit.. %d\n", size);

	num_partitions__ = size;

        printf("Shit... %d\n", num_partitions__);
	PartitionStatistics::wait_for_all();
}

void  PartitionStatistics::close() {
	per_partition_num_nodes_.close();
	per_machine_first_node_id_.close();
	per_machine_last_node_id_.close();

	for (auto& timer: per_partition_timers_) {
		timer.close();
	}
	for (auto& timer: per_partition_minor_timers_) {
		timer.close();
	}
}

void PartitionStatistics::replicate() {
	wait_for_all();
	per_partition_num_nodes_.pull();
	num_total_nodes_ = 0;

	for (PartitionID pid = 0; pid < num_partitions_; pid++) {
		if (max_num_nodes_per_vector_chunk_ < per_partition_num_nodes_[pid]) {
			max_num_nodes_per_vector_chunk_ = per_partition_num_nodes_[pid];
		}
		num_total_nodes_ += per_partition_num_nodes_[pid];
	}

	num_target_vector_chunks() = UserArguments::VECTOR_PARTITIONS * num_partitions__;
	my_vector_chunks().Set(UserArguments::VECTOR_PARTITIONS * my_partition_id_,
	                       UserArguments::VECTOR_PARTITIONS * (my_partition_id_ + 1) - 1);

	per_machine_vector_chunks_.resize(num_partitions__);
	for (int i = 0; i < num_partitions__; i++) {
		per_machine_vector_chunks_[i].Set(UserArguments::VECTOR_PARTITIONS * i,
		                                  UserArguments::VECTOR_PARTITIONS * (i + 1) - 1);
	}

	per_machine_first_node_id_.pull();
	per_machine_last_node_id_.pull();
	wait_for_all();

	node_t prev_first_vid = -1;
	for(int64_t i = 0 ; i < num_machines(); i++) {
		ALWAYS_ASSERT (per_machine_first_node_id(i) > prev_first_vid);
		prev_first_vid = per_machine_first_node_id(i);
	}
	wait_for_all();

	vector_chunk_id_to_vid_range_.resize(num_target_vector_chunks());
	for(int64_t i = 0 ; i < num_target_vector_chunks() ; i++) {
		PageID pid = i /UserArguments::VECTOR_PARTITIONS;
		int64_t chunkID  = i % UserArguments::VECTOR_PARTITIONS;
		node_t start_vid = per_machine_first_node_id(pid);
		start_vid += per_partition_num_nodes(pid) * (chunkID)/UserArguments::VECTOR_PARTITIONS;
		vector_chunk_id_to_vid_range_.at(i).SetBegin(start_vid);
		node_t end_vid = per_machine_first_node_id(pid);
		end_vid += per_partition_num_nodes(pid) * (chunkID + 1)/UserArguments::VECTOR_PARTITIONS -1;
		vector_chunk_id_to_vid_range_.at(i).SetEnd(end_vid);
		if(chunkID == UserArguments::VECTOR_PARTITIONS -1)
			vector_chunk_id_to_vid_range_.at(i).SetEnd(per_machine_last_node_id(pid));
		ALWAYS_ASSERT(chunkID < UserArguments::VECTOR_PARTITIONS);
	}


	for(int64_t i = 0 ; i < num_machines() ; i++) {
		ALWAYS_ASSERT (vector_chunk_id_to_vid_range_.at(i * UserArguments::VECTOR_PARTITIONS).GetBegin() == per_machine_first_node_id(i));
		ALWAYS_ASSERT (vector_chunk_id_to_vid_range_.at(i * UserArguments::VECTOR_PARTITIONS + UserArguments::VECTOR_PARTITIONS - 1).GetEnd() == per_machine_last_node_id(i));
		for (int j = 0; j < UserArguments::VECTOR_PARTITIONS; j++) {
			int grid_id = i * UserArguments::VECTOR_PARTITIONS + j;
			if (j == 0) {
				ALWAYS_ASSERT (vector_chunk_id_to_vid_range_.at(grid_id).GetBegin() == per_machine_first_node_id(i));
			} else if (j == UserArguments::VECTOR_PARTITIONS - 1) {
				ALWAYS_ASSERT (vector_chunk_id_to_vid_range_.at(grid_id).GetEnd() == per_machine_last_node_id(i));
			} else {
				ALWAYS_ASSERT (vector_chunk_id_to_vid_range_.at(grid_id - 1).GetEnd()+1 == vector_chunk_id_to_vid_range_.at(grid_id).GetBegin());
				ALWAYS_ASSERT (vector_chunk_id_to_vid_range_.at(grid_id).GetEnd()+1 == vector_chunk_id_to_vid_range_.at(grid_id + 1).GetBegin());
			}
		}
	}
}


node_t PartitionStatistics::max_internal_nodes() {
	node_t return_val = 0;
	for(int i = 0 ; i < num_machines() ; i++)
		if(per_partition_num_nodes_[i] > return_val)
			return_val = per_partition_num_nodes_[i];
	return return_val;
}


Range<node_t> PartitionStatistics::my_chunkID_to_range(int64_t chunkID) {
	return machine_id_and_chunk_idx_to_vid_range(my_machine_id(), chunkID);
}

Range<node_t> PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(PartitionID pid, int64_t chunkID) {
	return vector_chunk_id_to_vid_range_[pid*UserArguments::VECTOR_PARTITIONS + chunkID];
}

Range<node_t> PartitionStatistics::per_machine_vid_range(int64_t machine_id) {
    Range<node_t> range(per_machine_first_node_id_[machine_id], per_machine_last_node_id_[machine_id]);
    return range;
}

Range<node_t> PartitionStatistics::per_edge_partition_vid_range(int64_t edge_partition_id) {
	int machine_id = edge_partition_id / UserArguments::VECTOR_PARTITIONS;
	int64_t chunk_idx = edge_partition_id % UserArguments::VECTOR_PARTITIONS;
	return machine_id_and_chunk_idx_to_vid_range(machine_id, chunk_idx);
}

PartitionID& PartitionStatistics::num_machines() {
	return num_partitions_;
}

PartitionID& PartitionStatistics::my_machine_id() {
	return my_partition_id_;
}
node_t& PartitionStatistics::my_num_internal_nodes() {
	return per_partition_num_nodes_[my_partition_id_];
}

node_t& PartitionStatistics::my_first_node_id() {
	return per_machine_first_node_id_[my_partition_id_];
}
node_t& PartitionStatistics::my_last_node_id() {
	return per_machine_last_node_id_[my_partition_id_];
}

void PartitionStatistics::wait_for_all() {
	MPI_Barrier(MPI_COMM_WORLD);
}

node_t& PartitionStatistics::num_total_nodes() {
	return num_total_nodes_;
}

node_t& PartitionStatistics::max_num_nodes_per_vector_chunk() {
	return max_num_nodes_per_vector_chunk_;
}

int64_t& PartitionStatistics::num_target_vector_chunks() {
	return num_target_vector_chunks_;
}

Range<int64_t>& PartitionStatistics::my_vector_chunks() {
	return my_vector_chunks_;
}

int64_t& PartitionStatistics::my_total_num_subchunks() {
	return my_total_num_subchunks_;
}
int64_t& PartitionStatistics::num_subchunks_per_edge_chunk() {
	return num_subchunks_per_edge_chunk_;
}

Range<int64_t> PartitionStatistics::vector_chunks_to_edge_subchunks(Range<int64_t> vector_chunks) {
	Range<int64_t> edge_subchunks;
	// TODO
	edge_subchunks.Set(vector_chunks.GetBegin() * PartitionStatistics::num_subchunks_per_edge_chunk(), (vector_chunks.GetEnd() + 1) * PartitionStatistics::num_subchunks_per_edge_chunk() - 1);
	return edge_subchunks;
}

void PartitionStatistics::print_partition_info(int64_t my_edge_db_file_size) {
	int64_t edge_file_size = my_edge_db_file_size;
	MPI_Allreduce(MPI_IN_PLACE, &edge_file_size, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

	LOG(INFO) << "[" << PartitionStatistics::my_machine_id() << "]"
	          << "\tnum_nodes: " << PartitionStatistics::my_num_internal_nodes()
	          << "\tedge_db_size: " << my_edge_db_file_size
	          << "\tVid = [" << PartitionStatistics::my_first_node_id()
	          << ", " << PartitionStatistics::my_last_node_id() << "]"
	          << "\ttotal_num_nodes: " << PartitionStatistics::num_total_nodes()
	          << "\ttotal_edge_db_size: " << edge_file_size;

	if (PartitionStatistics::my_machine_id() == 0) {
		fprintf(stdout, "[Data Graph Info] Total |V|=%lld\tTotal |E in MB|=%lld\n", PartitionStatistics::num_total_nodes(), edge_file_size);
		fprintf(stdout, "\t(# of machines) = %lld, (# of total vchunks) = %lld\n", PartitionStatistics::num_machines(), PartitionStatistics::num_target_vector_chunks());
	}
}

node_t PartitionStatistics::VidToDegreeOrder(node_t v1) {
	ALWAYS_ASSERT (v1 >= 0);
	ALWAYS_ASSERT (v1 < PartitionStatistics::num_total_nodes());
	PartitionID partition_id = v1/PartitionStatistics::per_partition_num_nodes(0);
	return VidToDegreeOrder(v1, partition_id);
}

node_t PartitionStatistics::VidToDegreeOrder(node_t v1, PartitionID pid) {
	ALWAYS_ASSERT (pid >= 0 && pid < PartitionStatistics::num_machines());
	ALWAYS_ASSERT (v1 >= PartitionStatistics::per_machine_first_node_id(pid) && v1 <= PartitionStatistics::per_machine_last_node_id(pid));
	node_t order = pid + num_partitions_ * (v1 - PartitionStatistics::per_machine_first_node_id(pid));
	ALWAYS_ASSERT (order >= 0 && order < PartitionStatistics::num_total_nodes());
	return order;
}

node_t PartitionStatistics::DegreeOrderToVid(node_t order) {
	ALWAYS_ASSERT (order >= 0 && order < PartitionStatistics::num_total_nodes());
	PartitionID pid = order % num_partitions_;
	return PartitionStatistics::DegreeOrderToVid(order, pid);
}

node_t PartitionStatistics::DegreeOrderToVid(node_t order, PartitionID pid) {
	ALWAYS_ASSERT (pid >= 0 && pid < PartitionStatistics::num_machines());
	ALWAYS_ASSERT (order >= 0 && order < PartitionStatistics::num_total_nodes());
	ALWAYS_ASSERT ((order - pid) % PartitionStatistics::num_machines() == 0);
	node_t vid = (order - pid) / num_partitions_ + PartitionStatistics::per_machine_first_node_id(pid);
	ALWAYS_ASSERT (vid >= 0 && vid < PartitionStatistics::num_total_nodes());
	return vid;
}

// Partition-aware APIs
bool LocalStatistics::IsInternalVertex(node_t node_id) {
	return PartitionStatistics::my_first_internal_vid() <= node_id && node_id <= PartitionStatistics::my_last_internal_vid();
}

int64_t& LocalStatistics::page_buffer_size_in_bytes() {
	return page_buffer_size_in_bytes_;
}


node_t LocalStatistics::first_local_internal_vid() {
	return 0;
}
node_t LocalStatistics::last_local_internal_vid() {
	return PartitionStatistics::my_num_internal_nodes() - 1;
}

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
        printProcessMemoryUsage(name, MBytes, get_total_mem_size_allocated());
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


void set_signal_handler() {
	struct sigaction sigact;
	sigact.sa_sigaction = crit_err_hdlr;
	sigact.sa_flags = SA_RESTART | SA_SIGINFO;

	if (sigaction(SIGINT, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGINT
		          << " (" << strsignal(SIGINT) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGILL, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGILL
		          << " (" << strsignal(SIGILL) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGTRAP, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGTRAP
		          << " (" << strsignal(SIGTRAP) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGSEGV
		          << " (" << strsignal(SIGSEGV) << ")\n";
		exit(EXIT_FAILURE);
	}

	if (sigaction(SIGFPE, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGFPE
		          << " (" << strsignal(SIGFPE) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGABRT, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGABRT
		          << " (" << strsignal(SIGABRT) << ")\n";
		exit(EXIT_FAILURE);
	}
}

void stack_dump() {
	static bool tried_throw = false;
	try {
		// try once to re-throw currently active exception
		if (!tried_throw++) throw;
	} catch (const std::exception &e) {
		std::cerr << __FUNCTION__ << " caught unhandled exception. what(): "
		          << e.what() << std::endl;
	} catch (...) {
		std::cerr << __FUNCTION__ << " caught unknown/unhandled exception."
		          << std::endl;
	}
	void * array[50];
	int size = backtrace(array, 50);

	std::cerr << __FUNCTION__ << " backtrace returned "
	          << size << " frames\n\n";

	char ** messages = backtrace_symbols(array, size);

	for (int i = 0; i < size && messages != NULL; ++i) {
        fprintf(stderr, "[bt][%ld]: (%d) %s\n", PartitionStatistics::my_machine_id(), i, messages[i]);
	}
	std::cerr << std::endl;

	free(messages);
}

void crit_err_hdlr(int sig_num, siginfo_t* info, void* ucontext) {
	sig_ucontext_t * uc = (sig_ucontext_t *)ucontext;

	std::cerr << "crit_err_hdlr() is called" << std::endl;

	// Get the address at the time the signal was raised from the EIP (x86)
	void* caller_address = (void *) uc->uc_mcontext.rip;

	std::cerr << "signal " << sig_num
	          << " (" << strsignal(sig_num)
	          << "), address is "
	          << info->si_addr
	          << " from " << caller_address
	          << std::endl;

	void* array[50];
	int size = backtrace(array, 50);

	std::cerr  << __FUNCTION__ << " backtrace returned "
	           << size << " frames\n\n";

	// overwrite sigaction with caller's address
	array[1] = caller_address;

	char** messages = backtrace_symbols(array, size);

	// skip first stack frame(points  here)
	for (int i = 1; i < size && messages != NULL; ++i) {
        fprintf(stderr, "[bt][%ld]: (%d) %s\n", PartitionStatistics::my_machine_id(), i, messages[i]);
		//std::cerr << "[bt]: (" << i << ") " << messages[i] << std::endl;
	}
	std::cerr << std::endl;

	free(messages);

	exit(EXIT_FAILURE);
}




