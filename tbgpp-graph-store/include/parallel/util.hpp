#pragma once

#include <numa.h>
#include <execinfo.h>
#include <signal.h>
#include <string.h>
#include <atomic>
#include <iostream>
#include <cstdlib>
#include <stdexcept>
#include <assert.h>
#include <stdio.h>
#include <endian.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/mman.h>
#include <libaio.h>
#include <tbb/concurrent_queue.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "common/common.hpp"
#include "cache/disk_aio/util.hpp"

#define N_Timers 32

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHELINE_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define CACHE_PADOUT  \
  char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))

#define MAX_NUM_CPU_CORES 32 //TODO: is this correct?

typedef int64_t node_t;



struct UserArguments { //TODO: Need to set this up
	//	static std::string BASE_WORKSPACE_PATH;
	static std::string WORKSPACE_PATH;
	static std::string RAWDATA_PATH;

	static int64_t NUM_THREADS;
	static int64_t NUM_TOTAL_CPU_CORES;
	static int64_t NUM_CPU_SOCKETS;
	static int64_t NUM_DISK_AIO_THREADS;

	static int64_t VECTOR_PARTITIONS;
	static int64_t NUM_SUBCHUNKS_PER_EDGE_CHUNK;
	static int64_t NUM_ITERATIONS;
	static int64_t PHYSICAL_MEMORY;
	static int64_t SCHEDULE_TYPE;

	static int64_t MAX_LEVEL;
	static int64_t CURRENT_LEVEL;
	static bool USE_DEGREE_ORDER_REPRESENTATION;
	// static IteratorMode_t ITERATOR_MODE;
	static bool BUILD_DB;

	static int64_t DEGREE_THRESHOLD;

	static int64_t UPDATE_VERSION;
	static int64_t SUPERSTEP;

	static int64_t BEGIN_VERSION;
	static int64_t END_VERSION;
	static int64_t MAX_VERSION;

	static bool IS_GRAPH_DIRECTED_GRAPH_OR_NOT; // ..._OR_UNDIRECTED
	static bool INCREMENTAL_PROCESSING;
	static bool USE_INCREMENTAL_SCATTER;
	static bool USE_INCREMENTAL_APPLY;
	static bool USE_PULL;
	static bool RUN_SUBQUERIES;
	static bool RUN_PRUNING_SUBQUERIES;
	static bool OV_FLUSH;
	static bool GRAPH_IN_MEMORY;
    static bool QUERY_ONGOING;

	static int INC_STEP;
	static int PRUNING_BFS_LV;
	static bool INS_OR_DELETE;

	static bool LOAD_DELETEDB;
	static bool INSERTION_WORKLOAD;

	// static EdgeType EDGE_DB_TYPE;
	// static DynamicDBType DYNAMIC_DB_TYPE;

	static bool USE_DELTA_NWSM;
	static bool USE_FULLIST_DB;
    static bool ONLY_FIRST_SUPERSTEP;
	static int64_t tmp;

	static int LATENT_FACTOR;
	static int64_t LATENT_RANDOM_NUM_DECIMAL_PLACE;
	static int64_t LATENT_RANDOM_MAX_VALUE;

	static int NUM_TOTAL_SUBQUERIES;
	static int CURRENT_SUBQUERY_IDX;
};

class TG_ThreadContexts {
  public:
	static __thread int64_t thread_id;
	static __thread int64_t core_id;
	static __thread int64_t socket_id;

	static __thread bool per_thread_buffer_overflow;
	static __thread bool run_delta_nwsm;
    // static __thread PageID pid_being_processed;
	// static __thread TG_NWSMTaskContext* ctxt;
};

struct PtrAndSize {
	char* ptr;
	int64_t size;
};



struct SimpleContainer {
	SimpleContainer(char* p, std::size_t c, std::size_t s): data(p), capacity(c), size_used(s) {}
	SimpleContainer(): data(nullptr), capacity(0), size_used(0) {}

	std::size_t size_available() {
		return (capacity - size_used);
	}

	char* data;
	std::size_t capacity;
	std::size_t size_used;
};



inline double get_current_time() {
	timeval t;
	gettimeofday(&t, 0);
	double sec = (double)t.tv_sec + (double)t.tv_usec/1000000;
	return sec;
}

inline double get_cpu_time() {
	return (double)clock() / CLOCKS_PER_SEC;
}

class turbo_timer {
  public:
	turbo_timer() {
		clear_all_timers();
	}

	void start_timer(int i) {
#ifdef DISABLE_TIMER
    return;
#endif
		_timers[i] = get_current_time();
	}

	void reset_timer(int i) {
#ifdef DISABLE_TIMER
    return;
#endif
		_timers[i] = get_current_time();
		_acc_time[i] = 0;
	}

	double stop_timer(int i) {
#ifdef DISABLE_TIMER
    return 0.0;
#endif
		double t = get_current_time();
		double elapsed_time = t - _timers[i];
		_acc_time[i] += elapsed_time;
		return elapsed_time;
	}

	double get_timer_without_stop(int i) {
#ifdef DISABLE_TIMER
    return 0.0;
#endif
		double t = get_current_time();
		double elapsed_time = t - _timers[i];
		return elapsed_time;
	}

	double get_timer(int i) {
#ifdef DISABLE_TIMER
    return 0.0;
#endif
		return _acc_time[i];
	}

    void clear_all_timers() {
#ifdef DISABLE_TIMER
      return;
#endif
        for (int i = 0; i < N_Timers; i++) {
            _timers[i] = 0;
            _acc_time[i] = 0;
        }
    }

  private:
	double _timers[N_Timers]; // timer
	double _acc_time[N_Timers]; // accumulated time
};


template <typename T>
class Range {

  public:

	Range() : begin(-1), end(-1) {}
	Range(T b, T e) : begin(b), end(e) {}

	inline T length() const {
		return (end - begin + 1);
	}

	inline bool contains(T o) const {
		return (o <= end && begin <= o);
	}
	inline bool contains(Range<T> o) const {
		return (begin <= o.begin && o.end <= end);
	}

	inline void Set(Range<T> o) {
		begin = o.begin;
		end = o.end;
	}
	inline void Set(T b, T e) {
		begin = b;
		end = e;
	}
	inline void SetBegin(T b) {
		begin = b;
	}
	inline void SetEnd(T e) {
		end = e;
	}

	inline T GetBegin() const {
		return begin;
	}
	inline T GetEnd() const {
		return end;
	}

	inline Range<T> Union(Range left, Range right) {
		ALWAYS_ASSERT(left.end == right.begin);
		return Range(left.begin, right.end);
	}

	inline Range<T> Intersection(Range right) {
		Range<T> result(std::max(begin, right.begin), std::min(end, right.end));
		if (result.end < result.begin) {
			return Range<T>(0, -1);
		} else {
			return result;
		}
	}

	inline bool Overlapped(Range right) {
		if (begin <= right.begin) {
			return (end >= right.begin);
		} else {
			return (begin <= right.end);
		}
	}

	template <typename elem_t>
	friend bool operator> (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator>= (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator<= (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator< (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator== (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator!= (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator> (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator< (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator< (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator== (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator!= (Range<elem_t>& l, const Range<elem_t>& r);

  public:
	T begin;
	T end;
};

// struct PaddedIdx {
// 	int64_t idx;
// 	CACHE_PADOUT;
// };

