#pragma once

/*
 * Design of the BlockMemoryStackAllocator
 *
 * This class implements stack-based memory allocator. It can be used when memory
 * space is allocated and freed in a last-in-first-out (LIFO) manner. The core 
 * functions provided are as follows:
 *     - Initialize:
 *         It is used to allocate new memory space or set m_data_ pointer to the
 *         given memory space. We touch the allocated memory space using memset()
 *         to force page faults.
 *     - Allocate:
 *         It is used to allocate memory space according to the given size in 
 *         bytes as input. This process is done atomically. It repeatedly tries 
 *         to allocate memory until it succeeds, and uses a backoff algorithm to 
 *         reduce contention.
 *     - TryAllocate:
 *         Unlike Allocate(), it attempts to allocate memory space only once
 *         and returns nullptr if it fails.
 *     - FreeBytes:
 *         It is used to return the allocated memory space to the stack memory 
 *         allocator. This is done by just moving the stack pointer according 
 *         to the size of bytes being freed.
 */

#include "analytics/core/TypeDef.hpp"
#include "analytics/util/MemoryAllocator.hpp"
#include "analytics/util/util.hpp"

class BlockMemoryStackAllocator : public MemoryAllocator {

  public:

	/* Construct this allocator */
	BlockMemoryStackAllocator() : m_data_(NULL), m_stack_size_bytes_(0), m_stack_pointer_(0) {}

	/* Deconstruct this alocator */
	~BlockMemoryStackAllocator() {
		INVARIANT (m_stack_pointer_.load() == 0);
		INVARIANT (m_data_ == NULL);
		INVARIANT (m_stack_size_bytes_ == 0);
	}

	/* Close this allocator */
	virtual void Close(bool need_free = true) {
		INVARIANT (m_stack_pointer_.load() == 0);
		if (m_data_ != NULL && need_free) {
			NumaHelper::free_numa_memory(m_data_, m_stack_size_bytes_);
			m_data_ = NULL;
		}
		m_stack_size_bytes_ = 0;
		m_stack_pointer_.store(0);
	}

	/* Initialize this allocator for the given buffer */
	virtual void Initialize (char* ptr, int64_t capacity) {
		Close();
		ALWAYS_ASSERT (ptr != NULL);
        m_data_ = ptr;
		m_stack_size_bytes_ = capacity;
		MoveStackPointer(0);
        std::memset(m_data_, 0, m_stack_size_bytes_);
	}

	/* Initialize the allocator for the given buffer size */
	virtual void Initialize (int64_t block_memory_size) {
		Close();
		m_data_ = NumaHelper::alloc_numa_interleaved_memory (block_memory_size);
		INVARIANT (m_data_ != NULL);
		m_stack_size_bytes_ = block_memory_size;
		MoveStackPointer(0);
        std::memset(m_data_, 0, m_stack_size_bytes_);
	}

	/* Try to allocate the memroy space for the given size */
	virtual char* TryAllocate (int64_t size) {
        ALWAYS_ASSERT (m_data_ != NULL);
        ALWAYS_ASSERT (size >= 0 && size < m_stack_size_bytes_);
        int64_t size_to_alloc = size;
        int64_t offset = m_stack_pointer_.fetch_add(size_to_alloc);
        if (offset + size_to_alloc > m_stack_size_bytes_) {
            m_stack_pointer_.fetch_add (-size_to_alloc);
            return nullptr;
        } else {
            INVARIANT (offset >= 0 && offset + size_to_alloc <= m_stack_size_bytes_);
            //fprintf(stdout, "[%ld][BlockMemoryTryAllocate] (%ld) %ld %ld %ld\n", UserArguments::tmp, (int64_t) pthread_self(), size, offset + size, m_stack_size_bytes_);
            return &m_data_[offset];
        }
    }

	/* Allocate the memory space for the given size */
	virtual char* Allocate (int64_t size) {
		ALWAYS_ASSERT (m_data_ != NULL);
		ALWAYS_ASSERT (size >= 0 && size < m_stack_size_bytes_);
		int64_t size_to_alloc = size;
		int64_t offset = -1;
		int64_t backoff = 16;
		while (true) {
			offset = m_stack_pointer_.fetch_add(size_to_alloc);
			if (offset + size_to_alloc > m_stack_size_bytes_) {
				m_stack_pointer_.fetch_add (-size_to_alloc);
				/* It uses back-off algorithm to reduce contention */
				usleep(backoff * 1024);
				if (backoff < 1024) {
					backoff = 2 * backoff;
				}
			} else {
				break;
			}
		}
        //fprintf(stdout, "[%ld][BlockMemoryAllocate] (%ld) %ld %ld %ld\n", UserArguments::tmp, (int64_t) pthread_self(), size, offset + size, m_stack_size_bytes_);
		INVARIANT (offset >= 0 && offset + size_to_alloc <= m_stack_size_bytes_);
		return &m_data_[offset];
	}

	/* Free the allocated space */
	virtual void Free (char* ptr, int64_t size) {
        FreeBytes(size);
        return;
	}

	/* Free the allocated space */
	virtual void Free (char* ptr) {
		// DO Nothing
		LOG_ASSERT(false);
        return;
	}

	/* Free the all allocated spaces */
    void FreeAll () {
        MoveStackPointer(0L);
    }

	/* 
	* Free the allocated space for the given size
	* It assumes that it free the allocated space at the top of stack 
	*/
	void FreeBytes (int64_t size) {
        int64_t prev_sp = m_stack_pointer_.fetch_add(-size);
		INVARIANT (prev_sp >= 0 && prev_sp < m_stack_size_bytes_);
	}

	/* 
	* Move the stack pointer to the given offset
	*/
	void MoveStackPointer (int64_t offset) {
		ALWAYS_ASSERT (offset >= 0 && offset < m_stack_size_bytes_);
		ALWAYS_ASSERT (m_data_ != NULL);
        //fprintf(stdout, "[%ld][MoveStackPointer] (%ld) was %ld is %ld\n", UserArguments::tmp, (int64_t) pthread_self(), m_stack_pointer_.load(), offset);
		m_stack_pointer_.store(offset);
	}

	/*
	* Get the memory address for the given offset
	*/
	char* GetData(int64_t offset) {
		ALWAYS_ASSERT (offset >= 0 && offset < m_stack_size_bytes_);
		ALWAYS_ASSERT (m_data_ != NULL);
		return &m_data_[offset];
	}

	/*
	* Get the total size of the whole buffer space
	*/
	virtual int64_t GetSize() {
		return m_stack_size_bytes_;
	}

	/*
	* Get the stack pointer
	*/
	int64_t GetStackPointer() {
		return m_stack_pointer_.load();
	}

	/*'
	* Get the not allocated memory space
	*/
	virtual int64_t GetRemainingBytes() {
		return (m_stack_size_bytes_ - GetStackPointer());
	}

  private:
	char* m_data_; /* pointer to a buffer */
	int64_t m_stack_size_bytes_; /* the size of the buffer */
	std::atomic<int64_t> m_stack_pointer_; /* stack pointer that indicates the start of allocated memory space */
};
