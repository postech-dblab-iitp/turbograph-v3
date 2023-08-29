#ifndef ATOM_H
#define ATOM_H

/*
 * Design of the atom
 *
 * This class provides several functions related to lock. The lock function
 * is implemented using atomic_compare_exchange_weak. If the value of the 
 * m_lock variable is LOCK, it loops and compares the value of m_lock within 
 * the lock() function until it is changed to UNLOCK. On the other hand, you 
 * can use trylock to do something else (e.g. waiting for other tasks that 
 * hold this lock to complete) in the loop waiting for unlock outside of this 
 * class.
 */

#include <assert.h>
#include <atomic>
#include <thread>
#include <sstream>
#include <immintrin.h>
#include "common/assert.hpp"

//#define OWNER_CHECK 0

#define LOCK 1
#define UNLOCK 0

class atom {
  public:
	atom() {
		m_lock=UNLOCK;
	};
	~atom() {};

	inline void lock() {
		int32_t lock = UNLOCK;
		while(!std::atomic_compare_exchange_weak((std::atomic<int32_t>*) &m_lock, &lock, LOCK)) {
			lock = UNLOCK;
			_mm_pause();
		};
#ifdef OWNER_CHECK
		set_owner();
#endif
	}

	inline bool trylock() {
		int32_t lock = UNLOCK;
		bool success = std::atomic_compare_exchange_weak((std::atomic<int32_t>*) &m_lock, &lock, LOCK);
#ifdef OWNER_CHECK
		if (success) {
			set_owner();
		}
#endif
		return success;
	}

	inline void unlock() {
		D_ASSERT (IsLocked());
#ifdef OWNER_CHECK
		check_owner();
#endif
		std::atomic_store((std::atomic<int32_t>*) &m_lock, UNLOCK);
	}

	inline bool IsLocked() {
		return std::atomic_load((std::atomic<int32_t>*) &m_lock);
	}

#ifdef OWNER_CHECK
	void set_owner() {
		m_thread_id = core_id::my_core_id();
	}

	void check_owner() {
		D_ASSERT (m_thread_id == core_id::my_core_id());
	}
#endif

  private:
	int32_t m_lock;
#ifdef OWNER_CHECK
	int m_thread_id;
#endif
};




#endif
