#ifndef FIXED_SIZED_CONCURRENT_BUFFER_POOL_H
#define FIXED_SIZED_CONCURRENT_BUFFER_POOL_H

/*
 * Design of FixedSizeBufferPool
 *
 * This class implements a fixed size buffer pool. Allocates contiguous memory 
 * space for a given total memory budget, then splits it down into the size of 
 * a buffer chunk given as a parameter. The buffer chunks are inserted into the 
 * concurrent queue and managed through it.
 *
 * There may be cases where many threads request memory allocation at the same 
 * time, but there are no buffers left in the buffer pool. In order to prevent 
 * threads from wasting cpu, we use the backoff algorithm.
 */

#include <cstring>

#include "TypeDef.hpp"
#include "concurrentqueue/oncurrentqueue.h"

class FixedSizeConcurrentBufferPool {

  public:

	/* Construct this fixed size concurrent buffer pool */
	FixedSizeConcurrentBufferPool() : data_(NULL), total_buffer_size_(-1), chunk_buffer_size_(-1) {}

	/* Deconstruct this fixed size concurrent buffer pool */
	~FixedSizeConcurrentBufferPool() {
		if (data_ != NULL) {
			delete[] data_;
			data_ = NULL;
		}
	}

	/* 
	* Initialize this fixed size concurrent buffer pool for a given total buffer size and chunk size 
	* For example, it makes four 64 bytes chunks if you configure the total buffser size as 256 bytes 
	* and the chunk size as 64 bytes   
	*/
	void Initialize(int64_t total_buffer_size_in_bytes, int64_t chunk_buffer_size_in_bytes) {
		if (data_ != NULL) {
			delete[] data_;
			data_ = NULL;
		}

		total_buffer_size_ = total_buffer_size_in_bytes;
		chunk_buffer_size_ = chunk_buffer_size_in_bytes;
		/* Allocate a cache-aligned memory space for the given total buffer size */ 
		posix_memalign((void**)&data_, 512, total_buffer_size_);
		//data_ = new char[total_buffer_size_];
		std::memset ((void*) data_, 0, total_buffer_size_);

		int64_t num_chunks = total_buffer_size_ / chunk_buffer_size_;
		/* 
		* Initialize the queue that contains the information of chunks
		* The information of a chunk consists of a pointer to a chunk, the capacity of a chunk 
		* and used size of a chunk 
		*/
		#pragma omp parallel for
		for (int64_t i = 0; i < num_chunks; i++) {
			char* tmp = data_ + chunk_buffer_size_ * i;
			SimpleContainer cont;
			cont.data = tmp;
			cont.capacity = chunk_buffer_size_;
			cont.size_used = 0;
			queue_.enqueue(cont);
		}
	}

	/* Close this fixed size concurrent buffer pool */
	void Close() {
		if (data_ != NULL) {
			delete[] data_;
			data_ = NULL;
		}
	}

	/* Get a free chunk from this buffer pool */
	SimpleContainer Alloc() {
		int backoff = 1;
		SimpleContainer cont;
		while (!queue_.try_dequeue(cont)) {
			/* It uses back-off algorithm */
			usleep(backoff * 32);
			backoff= 2 * backoff;
			if (backoff >= 4096) backoff = 4096;
		}
		ALWAYS_ASSERT (cont.capacity == chunk_buffer_size_);
		return cont;
	}

	/* Give the chunk back to this buffer pool */
	void Dealloc(SimpleContainer cont) {
		ALWAYS_ASSERT (cont.capacity == chunk_buffer_size_);
		queue_.enqueue(cont);
	}

  private:
	char* data_; /* pointer to a buffer space */
	int64_t total_buffer_size_; /* the size of the buffer */
	int64_t chunk_buffer_size_; /* the size of a chunk */
	moodycamel::ConcurrentQueue<SimpleContainer> queue_; /* Queue that contains the information of chunks */
};


#endif

