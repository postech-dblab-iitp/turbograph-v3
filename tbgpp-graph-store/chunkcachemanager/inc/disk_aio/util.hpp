#ifndef UTIL_H_
#define UTIL_H_

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
#include "TypeDef.hpp"

// padded, aligned primitives
template <typename T>
class aligned_padded_elem {
  public:

	template <class... Args>
	aligned_padded_elem(Args &&... args)
		: elem(std::forward<Args>(args)...) {
		assert(sizeof(aligned_padded_elem<T>) % CACHELINE_SIZE == 0);
	}

	T elem;
	CACHE_PADOUT;

	// syntactic sugar- can treat like a pointer
	inline T & operator*() {
		return elem;
	}
	inline const T & operator*() const {
		return elem;
	}
	inline T * operator->() {
		return &elem;
	}
	inline const T * operator->() const {
		return &elem;
	}

  private:
	inline void
	__cl_asserter() const {
		assert((sizeof(*this) % CACHELINE_SIZE) == 0);
	}
} CACHE_ALIGNED;

class NumaHelper {
  public:
	static int64_t sockets;
	static int64_t cores;
	static int64_t cores_per_socket;

	static int64_t numa_policy;

	static NumaHelper numa_helper;

	NumaHelper() {
		NumaHelper::sockets = numa_num_configured_nodes();
		NumaHelper::cores = numa_num_configured_cpus();
		NumaHelper::cores_per_socket = NumaHelper::cores / NumaHelper::sockets;

        //UserArguments::NUM_TOTAL_CPU_CORES = NumaHelper::cores;
    }

	static void print_numa_info() {
		fprintf(stdout, "(# of sockets) = %ld, (# of cores) = %ld\n", NumaHelper::sockets, NumaHelper::cores);
	}

	static int64_t num_sockets() {
		return sockets;
	}
	static int64_t num_cores() {
		return cores;
	}

	// TODO
	// Assuming 'compact'
	static int64_t assign_core_id_by_round_robin (int64_t thread_id, int64_t affinity_from, int64_t affinity_to) {
		return affinity_from + (thread_id % (affinity_to - affinity_from + 1));
	}

	static int64_t assign_core_id_scatter (int64_t thread_id, int64_t affinity_from, int64_t affinity_to) {
		int64_t socket_id = thread_id % NumaHelper::sockets;
		int64_t cores_per_socket = ((affinity_to - affinity_from + 1) / NumaHelper::sockets);
		int64_t core_idx_from = (affinity_from + NumaHelper::sockets -1) / NumaHelper::sockets;
		if (cores_per_socket == 0) cores_per_socket = 1;
		int64_t core_offset = (thread_id / NumaHelper::sockets) % cores_per_socket;
		int64_t core_id = (socket_id * NumaHelper::cores_per_socket) + core_idx_from + core_offset;

		assert (core_idx_from >= 0 && core_idx_from < NumaHelper::cores_per_socket);
		assert (socket_id >= 0 && socket_id < NumaHelper::sockets);
		assert (core_id >= 0 && core_id < NumaHelper::cores);
		return (core_id);
	}

	// Assuming 'scatter'
	static int64_t assign_core_id_by_round_robin (int64_t thread_id) {
		return (thread_id % NumaHelper::cores);
	}

	static int64_t get_socket_id_by_omp_thread_id(int thread_id) {
		return thread_id % NumaHelper::sockets;
	}
	static int64_t get_socket_id_by_core_id(int core_id) {
		return core_id / NumaHelper::cores_per_socket;
	}
	static int64_t get_socket_id(int thread_id) {
		return thread_id % NumaHelper::sockets;
	}

    // mmap with MAP_HUGETLB vs. madvise with MADV_HUGEPAGE
    // https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages
	static char* malloc_by_mmap_with_hugetlb (int64_t sz) {
        int64_t hugepage_sz = 2 * 1024 * 1024L;
        int64_t sz_hugetlb = (sz / hugepage_sz + 1) * hugepage_sz;
		char* buf = (char*) alloc_mmap(sz_hugetlb);
		if (buf == MAP_FAILED) {
			perror("mmap");
			exit(EXIT_FAILURE);
		}
        madvise(buf, sz_hugetlb, MADV_HUGEPAGE);
		return buf;
	}

	static char* alloc_mmap (int64_t sz) {
		assert (sz > 0);
		//fprintf(stdout, "mmap alloc %lld\n", (int64_t) sz);
		char * array = (char *)mmap(NULL, sz, PROT_READ | PROT_WRITE,
		                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (array == MAP_FAILED) {
			fprintf(stdout, "[NumaHelper] mmap failed; ErrorNo = %d\n", errno);
		}
		assert (array != NULL);
		return array;
	}

	static char* alloc_numa_interleaved_memory (int64_t sz) {
		assert (sz > 0);
		//char* tmp = (char*) numa_alloc_interleaved(sz);
        int64_t hugepage_sz = 2 * 1024 * 1024L;
        int64_t sz_hugetlb = (sz / hugepage_sz + 1) * hugepage_sz;
		char* tmp = (char*) malloc_by_mmap_with_hugetlb(sz_hugetlb);
        numa_interleave_memory(tmp, sz_hugetlb, numa_all_nodes_ptr);

		assert (tmp != NULL);
		if (tmp == NULL) {
			fprintf (stderr, "[Error] NumaHelper::alloc_num_interleaved_memory(%ld) failed; ErrorCode = %d\n", (int64_t) sz, errno);
			throw std::runtime_error("[Exception] NumaHelper::alloc_numa_interleaved_memory failed\n");
		}
		return tmp;
	}
	
	static char* alloc_numa_memory_two_part (int64_t sz) {
		char* tmp = alloc_numa_interleaved_memory(sz);
		int64_t pg_sz = numa_pagesize();
		int64_t sz_ = (((sz / pg_sz) * pg_sz) / 2) * 2;
		numa_tonode_memory(tmp, sz_/2, 0);
		numa_tonode_memory(tmp + sz_/2, sz_/2, 1);
		return tmp;
	}

	static char* alloc_numa_memory (int64_t sz) {
		assert (sz > 0);
		char* tmp;
		switch (NumaHelper::numa_policy) {
		case 0:
			tmp = (char*) numa_alloc_interleaved(sz);
			break;
		case 1:
			tmp = (char*) numa_alloc_onnode(sz, 0);
			break;
		case 2:
			tmp = (char*) numa_alloc_interleaved(sz);
			break;
		default:
			abort();
			break;
		}
		if (tmp == NULL) {
			fprintf (stderr, "[Error] NumaHelper::alloc_num_memory(%ld) failed; ErrorCode = %d\n", (int64_t) sz, errno);
			throw std::runtime_error("[Exception] NumaHelper::alloc_num_memory failed\n");
		}
		assert (tmp != NULL);
		return tmp;
	}

	static char* alloc_numa_local_memory(int64_t sz, int64_t socket_id) {
		char* tmp = (char*) numa_alloc_onnode(sz, socket_id);
		if (tmp == NULL) {
			fprintf(stdout, "[NumaHelper::alloc_numa_local_memory] Failed; errno = %d\n", errno);
			abort();
		}
		return tmp;
	}
	static char* alloc_numa_local_memory(int64_t sz) {
		exit(-1);
		//int64_t socket_id = TG_ThreadContexts::socket_id;
		//return alloc_numa_local_memory(sz, socket_id);
	}

    template <typename T>
	static void free_numa_local_memory (T* buf, int64_t sz) {
		assert (buf != NULL);
		assert (sz > 0);
		numa_free((char*)buf, sz);
	}
	
    template <typename T>
	static void free_numa_memory (T* buf, int64_t sz) {
		assert (buf != NULL);
		assert (sz > 0);
		numa_free((char*)buf, sz);
		//munmap(buf, PROT_READ | PROT_WRITE);
	}

	template <typename T>
	static void free_mmap_memory (T* buf) {
		assert (buf != NULL);
		munmap(buf, PROT_READ | PROT_WRITE);
	}
};

#endif
