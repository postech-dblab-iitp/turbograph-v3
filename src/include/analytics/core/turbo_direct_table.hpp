#ifndef TURBO_DIRECT_TABLE_H
#define TURBO_DIRECT_TABLE_H

#include <iostream>
#include <fstream>
#include <vector>

#include "page.hpp"
#include "atom.hpp"
#include "turbo_dist_internal.hpp"
#include "Global.hpp"

#define INVALID_FRAME -1

struct turbo_direct_table_entry {

	turbo_direct_table_entry() : frameNo(INVALID_FRAME) { };

	inline void lock() {
		atom_lock.lock();
	}

	inline void unlock() {
		atom_lock.unlock();
	}

	inline bool trylock() {
		return atom_lock.trylock();
	}

	inline bool IsLocked() {
		return atom_lock.IsLocked();
	}
	
    inline void SetDirty(){
		dirty_bit = SETBIT;
	}
	
    inline void ResetDirty(){
		dirty_bit = RESETBIT;
	}
	
    inline bool IsDirty(){
		return dirty_bit == SETBIT;
	}

	FrameID frameNo;
    char dirty_bit;
	atom atom_lock;
};
//	CACHE_PADOUT;
//} CACHE_ALIGNED ;

class turbo_direct_table {
  public:

	turbo_direct_table(std::size_t size) : entries_ (NULL), num_entries_(0) {
		resize(size);
		#pragma omp parallel for
		for (PageID pid = 0; pid < size; pid++) {
			entries_[pid].frameNo = INVALID_FRAME;
			entries_[pid].ResetDirty();
		}
	}

	~turbo_direct_table() {
		#pragma omp parallel for
		for (PageID pid = 0; pid < num_entries_; pid++) {
			ALWAYS_ASSERT(!entries_[pid].IsLocked());
		}
		NumaHelper::free_numa_memory((char*)entries_, bytes_allocated_);
		entries_ = NULL;
		num_entries_ = 0;
		bytes_allocated_ = 0;
	}

	inline FrameID LookUp(PageID pid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		return entries_[pid].frameNo;
	}

	inline void Insert(PageID pid, FrameID frameid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		ALWAYS_ASSERT(frameid >= 0);
		entries_[pid].frameNo = frameid;
	}

	inline void Delete(PageID pid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		ALWAYS_ASSERT(entries_[pid].IsLocked());
		entries_[pid].frameNo = INVALID_FRAME;
	}

	inline void lock(PageID pid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		entries_[pid].lock();
	}

	//inline void unlock(PageID pid) {
	inline bool unlock(PageID pid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		if(!entries_[pid].IsLocked()) {
            fprintf(stdout, "pid %d not locked\n", pid);
            //INVARIANT(entries_[pid].IsLocked()); //XXX INVARIANT -> ALWAYS_ASSERT
            return false;
        }
		entries_[pid].unlock();
        return true;
	}

	inline bool trylock(PageID pid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		return entries_[pid].trylock();
	}

	inline bool IsLocked(PageID pid) {
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		return entries_[pid].IsLocked();
	}
	
    inline void SetDirty(PageID pid){ 
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		entries_[pid].SetDirty();
	}

	inline void ResetDirty(PageID pid){
		ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
		entries_[pid].ResetDirty();
	}

	inline bool IsDirty(PageID pid){
		if(pid == INVALID_PAGE){
			return false;
		}
		else{
			ALWAYS_ASSERT(pid >= 0 && pid < num_entries_);
			return entries_[pid].IsDirty();
		}
	}

	inline void InitTable() {
		#pragma omp parallel for
		for (PageID pid = 0; pid < num_entries_; pid++) {
			entries_[pid].frameNo = INVALID_FRAME;
		}
	}

	inline void ClearFrameReference(PageID pid) {
		entries_[pid].frameNo = INVALID_FRAME;
	}

	inline bool IsValid(PageID pid) {
		ALWAYS_ASSERT(entries_[pid].IsLocked());
		return (entries_[pid].frameNo != INVALID_FRAME);
	}
    
    inline PageID GetNumEntry(){
        return num_entries_;
    }

	inline void resize(PageID num_entries) {
		if (entries_ != NULL) {
			ALWAYS_ASSERT (num_entries_ > 0);
			NumaHelper::free_numa_memory((char*)entries_, sizeof(turbo_direct_table_entry) * num_entries_);
			entries_ = NULL;
			num_entries_ = 0;
		}
		ALWAYS_ASSERT (entries_ == NULL);

		int64_t m_SystemPageSize = 0;
		m_SystemPageSize = sysconf(_SC_PAGE_SIZE);
		bytes_allocated_ = (m_SystemPageSize) * ((sizeof(turbo_direct_table_entry) * num_entries + m_SystemPageSize - 1) / m_SystemPageSize);
		ALWAYS_ASSERT (bytes_allocated_ >= sizeof(turbo_direct_table_entry) * num_entries);
		entries_ = (turbo_direct_table_entry*) NumaHelper::alloc_numa_interleaved_memory(bytes_allocated_);
        LocalStatistics::register_mem_alloc_info("DirectTable", bytes_allocated_ / (1024 * 1024));
		num_entries_ = num_entries;
	}

  private:
	turbo_direct_table_entry* entries_;
	PageID num_entries_;
	int64_t bytes_allocated_;
};


#endif
