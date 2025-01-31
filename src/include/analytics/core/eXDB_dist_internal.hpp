#ifndef EXDB_DIST_INTERNAL_H
#define EXDB_DIST_INTERNAL_H

/*
 * Design of the eXDB_dist_internal
 *
 * In this file, classes for managing information and data structures for each 
 * thread are implemented. The main classes are core_id and per_thread_lazy 
 * class. 
 *
 * The core_id class manages main computation threads. Each computation thread 
 * is given a unique core ID. The total number of computation threads is stored 
 * in the core_counts_ variable.
 *
 * The per_thread_lazy class manages per-thread data structures (mainly buffers).
 * We statically allocate memory space first. When a thread tries to access its 
 * data structure, we construct objects in the pre-allocated storage. Whether 
 * objects have been created is managed through a boolean array.
 * (Please refer to "placement new.")
 */

#include <algorithm>
#include <iostream>
#include <vector>
#include <atomic>
#include <mpi.h>
#include <omp.h>

#include "util.hpp"


//#define __thread __declspec( thread )

class core_id {
  public:
	static void set_core_ids(int);
	static int64_t my_core_id() {
		/*
		      if (my_core_id_ < 0 || my_core_id_ >= UserArguments::NUM_THREADS) {
			fprintf(stdout, "[Error] core_id GET my_core_id = %lld\n", core_id::my_core_id_);
		      }
		*/
		ALWAYS_ASSERT (my_core_id_ >= 0 && my_core_id_ < UserArguments::NUM_THREADS);
		return my_core_id_;
	}

	static void set_my_core_id(int64_t cid) {
		std::atomic_fetch_add((std::atomic<int64_t>*) &core_id::core_counts_, 1L);
		core_id::my_core_id_ = cid;
//      fprintf(stdout, "SET my_core_id = %lld\n", core_id::my_core_id_);
	}

	static int64_t core_counts() {
		return std::atomic_load((std::atomic<int64_t>*) &core_id::core_counts_);
		//return core_id::core_counts_;
	}

  private:
	// the core ID of 'this' core: -1 if not set
	static __thread int64_t my_core_id_;

	// contains a running count of all the cores
	static int64_t core_counts_;// CACHE PADDED & ALIGNED?;
};

template <typename T, bool CallDtor = false>
class per_thread {
  public:

	per_thread() {
		for (size_t i = 0; i < size(); i++) {
			new (&(elems()[i])) aligned_padded_elem<T>();
		}
		//memset(bytes_, 0, sizeof(aligned_padded_elem<T>) * MAX_NUM_PER_THREAD_DATASTRUCTURE);
	}

	~per_thread() {
		if (!CallDtor)
			return;
		for (size_t i = 0; i < size(); i++) {
			elems()[i].~aligned_padded_elem<T>();
		}
	}

	inline T & operator[](unsigned i) {
		ALWAYS_ASSERT (i >= 0);
		//ALWAYS_ASSERT (i < UserArguments::NUM_THREADS);
		return elems()[i].elem;
	}

	inline const T & operator[](unsigned i) const {
		ALWAYS_ASSERT (i >= 0);
		//ALWAYS_ASSERT (i < UserArguments::NUM_THREADS);
		return elems()[i].elem;
	}

	inline T & my() {
		return (*this)[core_id::my_core_id()];
	}

	inline const T & my() const {
		return (*this)[core_id::my_core_id()];
	}

	inline size_t size() const {
		return MAX_NUM_PER_THREAD_DATASTRUCTURE;
	}

	inline aligned_padded_elem<T>* elems() {
		return (aligned_padded_elem<T> *) &bytes_[0];
	}

	inline const aligned_padded_elem<T> * elems() const {
		return (const aligned_padded_elem<T> *) &bytes_[0];
	}
  protected:

	char bytes_[sizeof(aligned_padded_elem<T>) * MAX_NUM_PER_THREAD_DATASTRUCTURE];
};

template <typename T>
struct per_thread_buf {
	char bytes_[sizeof(T)];
	inline T * cast() {
		return (T *)&bytes_[0];
	}
	inline const T * cast() const {
		return (T *)&bytes_[0];
	}
};

template <typename T, bool CallDtor = false>
class per_thread_lazy : private per_thread<per_thread_buf<T>, CallDtor> {
	typedef per_thread_buf<T> buf_t;
  public:

	per_thread_lazy() {
		memset(&flags_[0], 0, sizeof(flags_));
	}

	template <class... Args>
	inline T & get(unsigned i, Args &&... args) {
		ALWAYS_ASSERT (i >= 0);
		buf_t &b = this->elems()[i].elem;
		if (!flags_[i]) {
			flags_[i] = true;
			T *px = new (&b.bytes_[0]) T(std::forward<Args>(args)...);
			return *px;
		}
		return *b.cast();
	}

	template <class... Args>
	inline T & my(Args &&... args) {
		return get(core_id::my_core_id(), std::forward<Args>(args)...);
	}


	inline T * view(unsigned i) {
		buf_t &b = this->elems()[i].elem;
		return flags_[i] ? b.cast() : nullptr;
	}

	inline const T * view(unsigned i) const {
		const buf_t &b = this->elems()[i].elem;
		return flags_[i] ? b.cast() : nullptr;
	}

	inline const T * myview() const {
		return view(core_id::my_core_id());
	}

  private:
	bool flags_[MAX_NUM_PER_THREAD_DATASTRUCTURE];
	CACHE_PADOUT;
};

#endif
