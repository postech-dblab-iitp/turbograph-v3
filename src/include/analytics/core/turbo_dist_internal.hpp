#ifndef TURBO_DIST_INTERNAL_H
#define TURBO_DIST_INTERNAL_H

#include <iostream>
#include <math.h>
#include <stdlib.h>
#include <iomanip>
#include <string>
#include <sstream>

#include "./hashmap/robin_hash.h"
#include "./hashmap/robin_map.h"
#include "./hashmap/hopscotch_map.h"
#include "./hashmap/hopscotch_sc_map.h"
#include <map>

#include "util.hpp"

void stack_dump();

namespace {
// invoke set_terminate as part of global constant initialization
 const bool SET_TERMINATE = std::set_terminate(stack_dump);
}


template<typename T>
struct _statistics {
	T sum;
	T max;
	T min;
	T avr;
	T stddev;
	T balance;

	int max_partition = 0;
	int min_partition = 0;
};

#define N_PERF_METRICS 14
typedef struct _statistics<double> Statistics;
extern Statistics* final_statistics_[N_PERF_METRICS];


enum PERF_METRICS {
	MPI_PROCESS = 0,
	SUPERSTEP = 1,
	MULTIPLY_PHASE = 2,
	UPDATE_PHASE = 3,
	DISK_READ_COUNT = 4,
	VECTOR_RGET_COUNT = 5,
	VECTOR_RACC_COUNT = 6,
};

enum EDGE_IN_OUT_MODE {
	UNDIRECTED,
	OUT_MODE,
	IN_MODE,
	//		IN_OUT_MODE
};

enum VECTOR_READ_WRITE_MODE {
	READ_ONLY,
	WRITE_ONLY,
	READ_WRITE,
};

static const int NTimers = 32;
extern PartitionID num_partitions__;
extern PartitionID my_partition_id__;

template <typename T>
class per_partition_elem {
  public:

	per_partition_elem() : win_(MPI_WIN_NULL) {
	}


	~per_partition_elem() {
		ALWAYS_ASSERT(win_ == MPI_WIN_NULL);
	}

	void close() {
		if (win_ != MPI_WIN_NULL) {
			MPI_Win_free(&win_);
			win_ = MPI_WIN_NULL;
		}
	}

	void push() {
		if (win_ == MPI_WIN_NULL) {
			MPI_Win_create(elems_, sizeof(T) * MAX_NUM_MACHINES, sizeof(T), MPI_INFO_NULL, MPI_COMM_WORLD, &win_);
		}
		wait_on();

		for (int i = 0; i < num_partitions__; i++) {
			if (i != my_partition_id__) {
				MPI_Put((char*)&elems_[my_partition_id__], sizeof(T), MPI_CHAR, i, my_partition_id__, sizeof(T), MPI_CHAR, win_);
			}
		}

		wait_on();
	}

	void pull() {
        //fprintf(stdout, "[%ld] try to pull\n", my_partition_id__);
		if (win_ == MPI_WIN_NULL) {
            //fprintf(stdout, "[%ld] create win\n", my_partition_id__);
			MPI_Win_create(elems_, sizeof(T) * MAX_NUM_MACHINES, sizeof(T), MPI_INFO_NULL, MPI_COMM_WORLD, &win_);
		}
        //fprintf(stdout, "[%ld] wait on 1\n", my_partition_id__);
		wait_on();

		for (int i = 0; i < num_partitions__; i++) {
			if (i != my_partition_id__) {
				MPI_Get((char *)&elems_[i], sizeof(T), MPI_CHAR, i, i, sizeof(T), MPI_CHAR, win_);
			}
		}
        //fprintf(stdout, "[%ld] wait on 2\n", my_partition_id__);
		wait_on();
	}


	inline T&		operator[] (std::size_t idx) {
		return elems_[idx];
	}
	inline const T&	operator[] (std::size_t idx) const {
		return elems_[idx];
	}

	inline T&		mine() {
		return elems_[my_partition_id__];
	}
	inline const T&	mine()	const {
		return elems_[my_partition_id__];
	}
	void	wait_on() {
		if(win_ != MPI_WIN_NULL) {
			MPI_Win_fence(0, win_);
		}
	}

  private:
	T elems_[MAX_NUM_MACHINES];
	MPI_Win win_;
};

template <typename T, typename T_size_t = int32_t>
class per_partition_array {
  public:

	per_partition_array() {
		total_size_ = 0;
		for (T_size_t i = 0; i < MAX_NUM_MACHINES; i++) {
			sizes_[i] = 0;
		}
		win_ = MPI_WIN_NULL;
	}

	~per_partition_array() {
		ALWAYS_ASSERT(pData_ == NULL);
		ALWAYS_ASSERT(win_ == MPI_WIN_NULL);
	}

	void close() {
		sizes_.close();
		if (win_ != MPI_WIN_NULL) {
			MPI_Win_free(&win_);
			win_ = MPI_WIN_NULL;
		}
		if (pData_ != NULL) {
			delete[] pData_;
			pData_ = NULL;
		}
	}

	void push_size() {
		sizes_.push();
		calculate_total_size();
	}
	void pull_size() {
		sizes_.pull();
		calculate_total_size();
	}

	void collective_init_from_array (T* ptr, T_size_t size) {
		if (size == 0) {
			return;
		}

		my_size() = size;
		//    sizes_.wait_on();
		pull_size();
		//    sizes().wait_on();
		make_array();
		data_copy(ptr);
		//    wait_on();
		pull_data();
		//    wait_on();
	}

	void make_array() {
		pData_ = new T[total_size_];
		std::size_t offset = 0;

		for (T_size_t idx = 0; idx < num_partitions__; idx++) {
			elems_[idx] = pData_ + offset;
			offset += sizes_[idx];
		}

		MPI_Win_create(pData_, sizeof(T) * total_size_, sizeof(T), MPI_INFO_NULL, MPI_COMM_WORLD, &win_);
	}

	void push_data() {
		T_size_t i;
		T_size_t disp;

		disp = 0;
		for (i = 0; i < my_partition_id__; i++) {
			disp += sizes_[i];
		}
		wait_on();
		for (T_size_t i = 0; i < num_partitions__; i++) {
			if (i != my_partition_id__) {
				MPI_Put((char*)&pData_[disp], sizes[my_partition_id__] * sizeof(T), MPI_CHAR, i, disp, sizes_[my_partition_id__] * sizeof(T), MPI_CHAR, win_);
			}
		}
		wait_on();
	}

	void pull_data() {
		T_size_t i, j;
		T_size_t disp;

		disp = 0;

		wait_on();
		for (i = 0; i < num_partitions__; i++) {
			if (i != my_partition_id__) {
				for (j = disp; j < disp + sizes_[i]; j++) {
					MPI_Get((char*)&pData_[disp], sizes_[i] * sizeof(T), MPI_CHAR, i, disp, sizes_[i] * sizeof(T), MPI_CHAR, win_);
				}
			}
			disp += sizes_[i];
		}
		wait_on();
	}

	void destory() {
		T_size_t i;

		for (i = 0; i < num_partitions__; i++) {
			delete elems_[i];
		}
	}

	void wait_on() {
		MPI_Win_fence(0, win_);
	}

	T_size_t& my_size() {
		return sizes_[my_partition_id__];
	}

	per_partition_elem<T_size_t>& sizes() {
		return sizes_;
	}
	const per_partition_elem<T_size_t>& sizes() const {
		return sizes_;
	}

	T* operator[] (std::size_t idx) {
		return elems_[idx];
	}
	const T* operator[] (std::size_t idx) const {
		return elems_[idx];
	}

	T* mine() {
		return elems_[my_partition_id__];
	}
	const T* mine() const {
		return elems_[my_partition_id__];
	}

	void data_copy(T* data) {
		memcpy(elems_[my_partition_id__], data, sizes_[my_partition_id__] * sizeof(T));
	}

	T elem(int32_t i, int32_t j) {
		return elems_[i][j];
	}

	void calculate_total_size() {
		total_size_ = 0;
		for (T_size_t i = 0; i < num_partitions__; i++) {
			total_size_ += sizes_[i];
		}
	}

  private:
	T* pData_;
	T* elems_[MAX_NUM_MACHINES];
	per_partition_elem<T_size_t> sizes_;
	MPI_Win win_;
	T_size_t total_size_;
};

class PartitionStatistics {
	friend class TurboDB;
	friend class LocalStatistics;
  public:
	PartitionStatistics() = default;

	static void init();
	static void replicate();
	static void close();

	inline static node_t& per_machine_first_node_id(PartitionID pid) {
		return per_machine_first_node_id_[pid];
	}
	inline static node_t& per_machine_last_node_id(PartitionID pid) {
		return per_machine_last_node_id_[pid];
	}
	inline static node_t& per_partition_num_nodes(PartitionID pid) {
		return per_partition_num_nodes_[pid];
	};
	static node_t max_internal_nodes();
	static PartitionID& num_machines();
	static PartitionID& my_machine_id();
	static node_t& my_num_internal_nodes();
	static node_t& my_first_node_id();
	static node_t& my_last_node_id();

	inline static node_t& my_first_internal_vid() {
		return per_machine_first_node_id_[my_partition_id_];
	}
	inline static node_t& my_last_internal_vid() {
		return per_machine_last_node_id_[my_partition_id_];
	}
    inline static Range<node_t> my_internal_vid_range() {
        return Range<node_t>(my_first_internal_vid(), my_last_internal_vid());
    }
	static Range<node_t> my_chunkID_to_range(int64_t chunkID);
	static Range<node_t> machine_id_and_chunk_idx_to_vid_range(PartitionID pid, int64_t chunkID);
	static Range<node_t> per_edge_partition_vid_range(int64_t edge_partition_id);
	static Range<node_t> per_machine_vid_range(int64_t machine_id);

	static void wait_for_all();

	static node_t& num_total_nodes();
	static node_t& max_num_nodes_per_vector_chunk();

	static int64_t& num_target_vector_chunks();
	static Range<int64_t>& my_vector_chunks();

	static int64_t my_total_num_subchunks_;
	static int64_t& my_total_num_subchunks();
	static int64_t num_subchunks_per_edge_chunk_;
	static int64_t& num_subchunks_per_edge_chunk();
	static Range<int64_t> vector_chunks_to_edge_subchunks(Range<int64_t> vector_chunks);

	static void print_partition_info(int64_t edge_db_file_size) ;

	//performance measurement
	static per_partition_array<double> per_partition_timers_[NTimers];
	static per_partition_array<double> per_partition_minor_timers_[NTimers];

  public:

    static node_t GetDstVidValue_(node_t vid) {
        return (vid) & ~(DELETE_MASKING_MSBS);
    }
    static node_t GetDeletedVersionOfAnEdge_(node_t vid) {
        node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
        return deleted_version == 0 ? std::numeric_limits<node_t>::max(): deleted_version;
    }
    static bool IsEdgePrevDeleted_(node_t vid) {
        node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
        if (deleted_version == 0) deleted_version = std::numeric_limits<node_t>::max();
        return deleted_version < UserArguments::UPDATE_VERSION;
    }
    static node_t GetPrevVisibleDstVid(node_t vid) {
        if (IsEdgePrevDeleted_(vid)) return -1;
        else return GetDstVidValue_(vid);
    }
    static bool IsEdgeDeleted_(node_t vid) {
        node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
        if (deleted_version == 0) deleted_version = std::numeric_limits<node_t>::max();
        return deleted_version <= UserArguments::UPDATE_VERSION;
    }
    static node_t GetVisibleDstVid(node_t vid) {
        if (IsEdgeDeleted_(vid)) return -1;
        else return GetDstVidValue_(vid);
    }
    static node_t VidToDegreeOrder(node_t vid);
	static node_t VidToDegreeOrder(node_t vid, PartitionID pid);
	static node_t DegreeOrderToVid(node_t vid);
	static node_t DegreeOrderToVid(node_t vid, PartitionID pid);

  private:

	static PartitionID& num_partitions_;
	static PartitionID& my_partition_id_;

	static node_t num_total_nodes_;
	static node_t max_num_nodes_per_vector_chunk_;

	static per_partition_elem<node_t> per_partition_num_nodes_;

	static per_partition_elem<node_t> per_machine_first_node_id_;
	static per_partition_elem<node_t> per_machine_last_node_id_;

	//These vector measure the amount of communication.
	//not thread-safe
	static std::vector<int64_t> rget_amount_per_partition; //byte
	static std::vector<int64_t> raccum_amount_per_partition; //byte

	static int64_t num_target_vector_chunks_;
	static std::vector<Range<int64_t>> per_machine_vector_chunks_;
	static Range<int64_t> my_vector_chunks_;

	static std::vector<Range<node_t>>  vector_chunk_id_to_vid_range_;
};


class LocalStatistics {
	friend class TurboDB;
  public:

	// Partition-aware APIs
	static bool IsInternalVertex(node_t node_id);
	static int64_t& page_buffer_size_in_bytes() ;

	static node_t first_local_internal_vid() ;
	static node_t last_local_internal_vid();

	// (Global) VertexID to (Local) VertexID
	inline static node_t Vid2Lid(node_t vid) {
        ALWAYS_ASSERT(vid >= PartitionStatistics::my_first_internal_vid() && vid <= PartitionStatistics::my_last_internal_vid());
        return vid - PartitionStatistics::my_first_internal_vid();
	}

	static void register_mem_alloc_info(const char* name, int64_t MBytes);
	static void print_mem_alloc_info();
	static int64_t get_total_mem_size_allocated();

	// performance measurement
	static std::vector<double> timers_[NTimers];
	static double timers_prv[NTimers];

	static std::vector<double> minor_timers_[NTimers];
	static double minor_timers_prv[NTimers];
  private:

	static int64_t page_buffer_size_in_bytes_;	// buffer size for data pages

	static std::map<std::string, int64_t> mem_alloc_info;
};

class JsonLogger{
	public:
		JsonLogger() {}
		~JsonLogger() {}
		
		void OpenLogger(const char* log_file_name, bool is_local_=false);
		void InsertKeyValue(const char* key,json11::Json value);
		void WriteIntoDisk();
		void Close();
		
		FILE* log_file;
		char* log_file_dir;
		std::map<std::string, json11::Json> json_map;
        bool is_local;
};

void system_fprintf(int machine_id, FILE* stream, const char* format, ...);

void MPI_Win_Info_Dump(MPI_Win& win);
//LONG WINAPI windows_exception_handler(EXCEPTION_POINTERS * ExceptionInfo);
void set_signal_handler();


template<typename T>
T ComputeSum(per_partition_array<T>& pa, int step) {
	T total = 0;
	for (int _partition = 0; _partition < num_partitions__; _partition++) {
		total += pa.elem(_partition, step);
	}
	return total;
}

template<typename T>
T ComputeMax(per_partition_array<T>& pa, int step, int& max_partition) {
	T max = pa.elem(0, step);
	max_partition = 0;
	for (int _partition = 0; _partition < num_partitions__; _partition++) {
		if (max < pa.elem(_partition, step)) {
			max = pa.elem(_partition, step);
			max_partition = _partition;
		}
	}
	return max;
}

template<typename T>
T ComputeMin(per_partition_array<T>& pa, int step, int& min_partition) {
	T min = pa.elem(0, step);
	min_partition = 0;
	for (int _partition = 0; _partition < num_partitions__; _partition++) {
		if (min > pa.elem(_partition, step)) {
			min = pa.elem(_partition, step);
			min_partition = _partition;
		}
	}
	return min;
}

template<typename T>
T ComputeAvr(per_partition_array<T>& pa, int step) {
	T total = 0;
	for (int _partition = 0; _partition < num_partitions__; _partition++) {
		total += pa.elem(_partition, step);
	}
	return (total / num_partitions__);
}

template<typename T>
T ComputeStddev(per_partition_array<T>& pa, int step) {
	T stddev = 0;
	T sqrSum = 0;
	T avr = ComputeAvr<T>(pa, step);

	for (int _partition = 0; _partition < num_partitions__; _partition++) {
		sqrSum += (pa.elem(_partition, step) * pa.elem(_partition, step));
	}
	stddev = sqrt((sqrSum / num_partitions__) - (avr * avr));
	return stddev;
}

template <typename T>
T ComputeBalance(T sum, T max) {
	T balance = 0;
	if(max == 0) {
		return balance;
	}
	balance = sum / (num_partitions__ * max);
	return balance;
}

template<typename T>
void ComputeStatistics(per_partition_array<T>& pa, int steps, int i) {

	final_statistics_[i] = (Statistics *) malloc(sizeof(Statistics) * steps);

	for (int _step = 0; _step < steps; _step++) {
		//std::cout << ComputeSum<T>(pa, _step) << std::endl;
		final_statistics_[i][_step].sum = ComputeSum<T>(pa, _step);
		final_statistics_[i][_step].max = ComputeMax<T>(pa, _step, final_statistics_[i][_step].max_partition);
		final_statistics_[i][_step].min = ComputeMin<T>(pa, _step, final_statistics_[i][_step].min_partition);
		final_statistics_[i][_step].avr = ComputeAvr<T>(pa, _step);
		final_statistics_[i][_step].stddev = ComputeStddev<T>(pa, _step);
		final_statistics_[i][_step].balance = ComputeBalance<T>(final_statistics_[i][_step].sum, final_statistics_[i][_step].max);
		//final_statistics_[i][_step].balance = ComputeBalance<T>(ComputeSum<T>(pa, _step), ComputeMax<T>(pa, _step, &final_statistics_[i][_step].max_partition));
	}
}



template<typename T>
void print_minor_result(per_partition_array<T> pa[], int size) {

    const int title_width = 20;
	const int value_width = 20;
	const char separator = ' ';

	LOG(INFO) << "=======================================================================================================================================";
	LOG(INFO) << "                                           <Minor Step Local Statistics>";
	std::ostringstream _title;
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "COMPUTE";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VECTOR_REQ_NEXT";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VECTOR_CNS_NEXT";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VECTOR_WAIT_WRB";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "PageIoReqs";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "BufferPoolUtil.";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "NotConsumedPages(%).";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "NumConsumedPages.";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "CurChunkSize.";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "TaskQueueUtil.";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "RGET_TIME";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "DISK_READ_TIME";
	//  _title << std::left << std::setw(title_width) << std::setfill(separator) << "DISK_WRITE_TIME";
	LOG(INFO) << _title.str().c_str();

	LOG(INFO) << "=======================================================================================================================================";
	for (int j = 0; j < size; j++) {
		std::ostringstream _value;
		for (int i = 0; i < 5; i++) {
			_value << std::left << std::setw(value_width) << std::setfill(separator) << LocalStatistics::minor_timers_[i][j];
		}
		LOG(INFO) << _value.str().c_str();
	}

	LOG(INFO) << "=======================================================================================================================================";
}

template<typename T>
void printResult_cout(int steps) {
	const int step_width = 6;
	const int title_width = 16;
	const char separator = ' ';

	const int label_width = 10;
	const int value_width = 16;

	std::cout << "==========================================================================================================================================================================" << std::endl;

	std::cout << "Order" << std::endl;
	std::cout << "  sum" << std::endl;
	std::cout << "  max" << std::endl;
	std::cout << "  min" << std::endl;
	std::cout << "  avg" << std::endl;
	std::cout << "  stddev" << std::endl;
	std::cout << "  balance" << std::endl;

	std::cout << "==========================================================================================================================================================================" << std::endl;
	std::cout << std::left << std::setw(step_width) << std::setfill(separator) << "step";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "SUPERSTEP";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "BEFORE_PAS";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "GROUP_NODE_LIST";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "COMPUTATION";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_IO_WAIT";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_SEND_REQ";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_SEND_UPD";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "MULTIPLY";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "UPDATE_BARRIER";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "UPDATE";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_IO_WAIT";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_CPU";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_MERGE";
	std::cout << std::left << std::setw(title_width) << std::setfill(separator) << "AFTER_PAS";
	std::cout << std::endl;
	std::cout << "==========================================================================================================================================================================" << std::endl;

	for (int _steps = 0; _steps < steps; _steps++) {
		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].sum;
		}
		std::cout << std::endl;

		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].max;
		}
		std::cout << std::endl;

		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].min;
		}
		std::cout << std::endl;

		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].avr;
		}
		std::cout << std::endl;

		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].stddev;
		}
		std::cout << std::endl;

		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].balance;
		}
		std::cout << std::endl;
	}
	std::cout << std::endl;
	std::cout << std::endl;
	std::cout << "==========================================================================================================================================================================" << std::endl;
	std::cout << "                                                    < (MAX, MIN) Information > " << std::endl;
	std::ostringstream _title;
	_title << std::left << std::setw(step_width) << std::setfill(separator) << "step";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "SUPERSTEP";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "BEFORE_PAS";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "GROUP_NODE_LIST";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "COMPUTATION";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_IO_WAIT";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_SEND_REQ";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_SEND_UPD";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "MULTIPLY";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPDATE_BARRIER";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPDATE";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_IO_WAIT";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_CPU";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_MERGE";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "AFTER_PAS";
	std::cout << std::endl;
	std::cout << "==========================================================================================================================================================================" << std::endl;
	for (int _steps = 0; _steps < steps; _steps++) {
		std::cout << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::ostringstream _p;
			_p << "(" <<  final_statistics_[i][_steps].max_partition << ", " << final_statistics_[i][_steps].min_partition << ")";
			std::cout << std::left << std::setw(value_width) << std::setfill(separator) << _p.str().c_str();
		}
		std::cout << std::endl;
	}
	std::cout << "==========================================================================================================================================================================" << std::endl;
	std::cout << std::flush;
	/*
	   for (int i = 0; i < N_PERF_METRICS; i++) {
	   free(final_statistics_[i]);
	   }
	   */
}

template<typename T>
void printResult(per_partition_array<T> pa[], int steps) {
	const int step_width = 6;
	const int title_width = 16;
	const char separator = ' ';

	const int label_width = 10;
	const int value_width = 16;

	for (int i = 0; i < N_PERF_METRICS; i++) {
		ComputeStatistics<T>(pa[i], steps, i);
	}

	LOG(INFO) << "==========================================================================================================================================================================";

	LOG(INFO) << "Order";
	LOG(INFO) << "  sum";
	LOG(INFO) << "  max";
	LOG(INFO) << "  min";
	LOG(INFO) << "  avg";
	LOG(INFO) << "  stddev";
	LOG(INFO) << "  balance";

	LOG(INFO) << "==========================================================================================================================================================================";
	std::ostringstream _title;
	LOG(INFO) << "                                           <Major Step Total Statistics>";
	_title << std::left << std::setw(step_width) << std::setfill(separator) << "step";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "SUPERSTEP";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "BEFORE_PAS";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "GROUP_NODE_LIST";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "COMPUTATION";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_IO_WAIT";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_SEND_REQ";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "VEC_SEND_UPD";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "MULTIPLY";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPDATE_BARRIER";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPDATE";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_IO_WAIT";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_CPU";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "UPD_MERGE";
	_title << std::left << std::setw(title_width) << std::setfill(separator) << "AFTER_PAS";
	LOG(INFO) << _title.str().c_str();
	LOG(INFO) << "==========================================================================================================================================================================";

	for (int _steps = 0; _steps < steps; _steps++) {
		std::ostringstream _line_sum, _line_max, _line_min, _line_avg, _line_balance, _line_stddev;

		_line_sum << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line_sum << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].sum;
		}
		LOG(INFO) << _line_sum.str().c_str();

		_line_max << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line_max << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].max;
		}
		LOG(INFO) << _line_max.str().c_str();

		_line_min << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line_min << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].min;
		}
		LOG(INFO) << _line_min.str().c_str();

		_line_avg << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line_avg << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].avr;
		}
		LOG(INFO) << _line_avg.str().c_str();

		_line_stddev << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line_stddev << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].stddev;
		}
		LOG(INFO) << _line_stddev.str().c_str();

		_line_balance << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line_balance << std::left << std::setw(value_width) << std::setfill(separator) << final_statistics_[i][_steps].balance;
		}
		LOG(INFO) << _line_balance.str().c_str();
	}
	LOG(INFO) << "==========================================================================================================================================================================";

	LOG(INFO) << "";
	LOG(INFO) << "";

	LOG(INFO) << "==========================================================================================================================================================================";
	LOG(INFO) << "                                                    < (MAX, MIN) Information > ";
	LOG(INFO) << _title.str().c_str();
	LOG(INFO) << "==========================================================================================================================================================================";
	for (int _steps = 0; _steps < steps; _steps++) {
		std::ostringstream _mm;
		_mm << std::left << std::setw(step_width) << std::setfill(separator) << _steps;
		for (int i = 0; i < N_PERF_METRICS; i++) {
			std::ostringstream _vv;
			_vv  <<  "(" <<  final_statistics_[i][_steps].max_partition << ", " << final_statistics_[i][_steps].min_partition << ")";
			_mm << std::left << std::setw(value_width) << std::setfill(separator) << _vv.str();
		}
		LOG(INFO) << _mm.str().c_str();
	}

	LOG(INFO) << "==========================================================================================================================================================================";

	LOG(INFO) << "";
	LOG(INFO) << "";

	LOG(INFO) << "==========================================================================================================================================================================";
	LOG(INFO) << "                                           <Major Step Local Statistics>";

	LOG(INFO) << _title.str().c_str();
	LOG(INFO) << "==========================================================================================================================================================================";
	for (int _steps = 0; _steps < steps; _steps++) {
		std::ostringstream _line;
		_line << std::left << std::setw(step_width) << std::setfill(separator) << _steps;

		for (int i = 0; i < N_PERF_METRICS; i++) {
			_line << std::left << std::setw(value_width) << std::setfill(separator) << LocalStatistics::timers_[i][_steps];
		}
		LOG(INFO) << _line.str().c_str();
	}
	LOG(INFO) << "==========================================================================================================================================================================";


	LOG(INFO) << "";
	LOG(INFO) << "";

	if(PartitionStatistics::my_machine_id() == 0) {
		printResult_cout<T>(steps);
	}

	for (int i = 0; i < N_PERF_METRICS; i++) {
		free(final_statistics_[i]);
	}

}



#endif
