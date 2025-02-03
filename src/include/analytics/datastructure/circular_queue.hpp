#ifndef PER_THREAD_TASK_QUEUE_H
#define PER_THREAD_TASK_QUEUE_H

#include <vector>
#include <complex>

#include "analytics/core/TypeDef.hpp"
#include "analytics/core/Global.hpp"

template <typename task_t>
class circular_queue {
  public:

	circular_queue() {
		pData_ = NULL;
		buffer_alloc_size_ = 0;
		enqueue_idx_ = 0;
		dequeue_idx_ = 0;
		fix_end = -1;
	}

	~circular_queue() {
		if (pData_ != NULL) {
			//delete[] pData_;
			numa_free(pData_, buffer_alloc_size_);
			pData_ = NULL;
			buffer_alloc_size_ = 0;
			enqueue_idx_ = 0;
			dequeue_idx_ = 0;
			fix_end = -1;
		}
	}

	void reset() {
		enqueue_idx_ = 0;
		dequeue_idx_ = 0;
		fix_end = -1;
	}
	void reserve(std::size_t sz) {
		if (pData_ != NULL) {
			//delete[] pData_;
			numa_free(pData_, buffer_alloc_size_);
			pData_ = NULL;
			buffer_alloc_size_ = 0;
			enqueue_idx_ = 0;
			dequeue_idx_ = 0;
			fix_end = -1;
		}
		if (sz <= 256*1024) {
			sz = 256 * 1024;
		}
		buffer_length_ = sz;
		//pData_ = new task_t[buffer_length_];
		buffer_alloc_size_ = sizeof(task_t) * buffer_length_;
		pData_ = (task_t*) numa_alloc_local(buffer_alloc_size_);
		memset(pData_, 0, buffer_alloc_size_);
	}

	//return ReturnStatus::OK; If successfully inserted
	//return ReturnStatus::FAIL; If buffer is full
	inline ReturnStatus enqueue(task_t& item) {
		if ((enqueue_idx_ + 1) % buffer_length_ == dequeue_idx_)
			return ReturnStatus::FAIL;
		pData_[enqueue_idx_] = item;
		enqueue_idx_ = (enqueue_idx_ + 1) % buffer_length_;
		return ReturnStatus::OK;
	}

	// return ReturnStatus::OK; if it returns any item
	// return ReturnStatus::FAIL; it it's empty.
	inline ReturnStatus dequeue(task_t& item) {
		if (enqueue_idx_ == dequeue_idx_)
			return ReturnStatus::FAIL;
		if (fix_end == dequeue_idx_)
			return ReturnStatus::FAIL;
		item = pData_[dequeue_idx_];
		dequeue_idx_ = (dequeue_idx_ + 1) % buffer_length_;
		return ReturnStatus::OK;
	}

	inline void set_fix_end() {
		fix_end = enqueue_idx_;
	}
	inline void clear_fix_end() {
		fix_end = -1;
	}

	inline bool hasCapacityFor(std::size_t capacity) {
		return (buffer_length_ - size()) <= capacity;
	}

	inline bool hasCapacityAbout(double percent) {
		double current_avail_space_ratio = (double) (buffer_length_ - size()) / buffer_length_;
		return current_avail_space_ratio >= percent ;
	}

	// The number of 'task_t' currently in the queue
	inline std::size_t size() {
		return (enqueue_idx_ - dequeue_idx_ + buffer_length_) % buffer_length_;
	}

	inline double getUtilization() {
		return 100 * ((double) size() / buffer_length_);
	}

  private:
	task_t*		pData_;
	std::size_t buffer_alloc_size_;
	std::size_t buffer_length_;
	std::size_t enqueue_idx_;
	std::size_t dequeue_idx_;
	std::size_t fix_end;
};

#endif
