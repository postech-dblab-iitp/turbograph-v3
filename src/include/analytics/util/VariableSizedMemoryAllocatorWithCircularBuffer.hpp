#pragma once

#include <list>
#include <map>
#include <atom.hpp>

#include "Global.hpp"
#include "MemoryAllocator.hpp"

class VariableSizedMemoryAllocatorWithCircularBuffer: public MemoryAllocator {
  public:
	VariableSizedMemoryAllocatorWithCircularBuffer() : m_data_ (NULL) {}
	VariableSizedMemoryAllocatorWithCircularBuffer(int64_t init_size) : m_data_ (NULL) {
		Initialize(init_size);
	}

	virtual void Close(bool need_free = true) {
		if (m_data_ != NULL && need_free) {
			NumaHelper::free_numa_memory(m_data_, m_size_bytes_);
			m_data_ = NULL;
		}
	}

	virtual void Initialize (char* ptr, int64_t capacity) {
        m_data_ = ptr;
		start_ptr_ = m_data_;
		last_ptr_ = start_ptr_ + capacity;
		m_size_bytes_ = capacity;
        std::memset(m_data_, 0, m_size_bytes_);

		cur_low_water_mark_ptr_ = start_ptr_;
		cur_high_water_mark_ptr_ = start_ptr_;
		syko = start_ptr_;
		allocated_size = 0;
	}

	virtual void Initialize(int64_t init_size) {
		m_data_ = NumaHelper::alloc_numa_interleaved_memory(init_size);;
		start_ptr_ = m_data_;
		last_ptr_ = start_ptr_ + init_size;
		m_size_bytes_ = init_size;
        std::memset(m_data_, 0, m_size_bytes_);

		cur_low_water_mark_ptr_ = start_ptr_;
		cur_high_water_mark_ptr_ = start_ptr_;
		syko = start_ptr_;
		allocated_size = 0;
	}

	// It tries to allocate a contiguous memory chunk of size 'size'
	// if succeed, return the pointer to the allocated buffer,
	// othrewise, return NULL (i.e. there is no avilable space)
	// so that the caller thread can wait/spin till it's available
	virtual char* Allocate (int64_t size) {
		lock.lock();
		INVARIANT(syko == cur_high_water_mark_ptr_);
		INVARIANT(allocated_size >= 0 && allocated_size <= m_size_bytes_);
		char* return_buffer = Allocate_(size);
		if(return_buffer != NULL) {
			// fprintf(stdout, "Alloc\t%lld\t%lld\n", cur_high_water_mark_ptr_ - size - start_ptr_, cur_high_water_mark_ptr_ - start_ptr_);
			allocated_size += size;
		}
		INVARIANT(allocated_size >= 0 && allocated_size <= m_size_bytes_);
		syko = cur_high_water_mark_ptr_;


		lock.unlock();
		return return_buffer;
	}

	char* Allocate_ (int64_t size) {

		if ( size <= 0 || size > m_size_bytes_) { //input size is negative or larger than Circular buffer
			fprintf(stdout, "Input size is negative or larger than buffer size, %lld / %lld\n", size, m_size_bytes_);
			return NULL;
		}

		if (cur_high_water_mark_ptr_ == cur_low_water_mark_ptr_ && lists_.size() == 0) {//Init state

			//fprintf(stdout, "Alloc back in init state\n");

			cur_high_water_mark_ptr_ = cur_high_water_mark_ptr_ + size;
			//fprintf(stdout, "Allocationed range = [%lld, %lld), size = %lld\n", cur_high_water_mark_ptr_ - size - start_ptr_, cur_high_water_mark_ptr_ - start_ptr_, size);

			PtrAndSize ptr_and_size_;
			ptr_and_size_.ptr = cur_high_water_mark_ptr_ - size;
			ptr_and_size_.size = size;

			lists_.push_back(ptr_and_size_);
			map_.insert(std::map<char*, std::list<PtrAndSize>::iterator>::value_type(cur_high_water_mark_ptr_ - size, --lists_.end() ));

			return cur_high_water_mark_ptr_ - size;
		}

		if(cur_high_water_mark_ptr_ > cur_low_water_mark_ptr_ ) { // high_water_mark_ptr_ is right of low_water_mark_ptr_
			if(cur_high_water_mark_ptr_ + size <= last_ptr_) { // high_water_mark_ptr_ is right of low_water_mark_ptr_ and exist space
				//fprintf(stdout, "Alloc back\n");

				cur_high_water_mark_ptr_ = cur_high_water_mark_ptr_ + size;
				//fprintf(stdout, "Allocationed range = [%lld, %lld), size = %lld\n", cur_high_water_mark_ptr_ - size - start_ptr_, cur_high_water_mark_ptr_ - start_ptr_, size);
				//cout << static_cast<void*>(cur_high_water_mark_ptr_ - size) << endl;

				PtrAndSize ptr_and_size_;
				ptr_and_size_.ptr = cur_high_water_mark_ptr_ - size;
				ptr_and_size_.size = size;

				lists_.push_back(ptr_and_size_);
				map_.insert(std::map<char*, std::list<PtrAndSize>::iterator>::value_type(cur_high_water_mark_ptr_ - size, --lists_.end() ));

				return cur_high_water_mark_ptr_ - size;
			} else { // high_water_mark_ptr_ is right of low_water_mark_ptr_ but no space
				if(cur_low_water_mark_ptr_ - size >= start_ptr_) { // exist space in [0 ~ low]
					//fprintf(stdout, "Back to front\n");

					cur_high_water_mark_ptr_ = start_ptr_ + size;
					//fprintf(stdout, "Allocationed range = [%lld, %lld), size = %lld\n", cur_high_water_mark_ptr_ - size - start_ptr_, cur_high_water_mark_ptr_ - start_ptr_, size);

					PtrAndSize ptr_and_size_;
					ptr_and_size_.ptr = cur_high_water_mark_ptr_ - size;
					ptr_and_size_.size = size;

					lists_.push_back(ptr_and_size_);
					map_.insert(std::map<char*, std::list<PtrAndSize>::iterator>::value_type(cur_high_water_mark_ptr_ - size, --lists_.end() ));

					return cur_high_water_mark_ptr_ - size;
				}
				// no space in [0 ~low]
				//fprintf(stdout, "No space in buffer pool\n");
				return NULL;
			}
		} else { // low_water_mark_ptr_ is right of high_water_mark_ptr_
			if(cur_low_water_mark_ptr_ - cur_high_water_mark_ptr_ >= size) { //exist space

				//fprintf(stdout, "Alloc back in cycle\n");

				cur_high_water_mark_ptr_ = cur_high_water_mark_ptr_ + size;
				//fprintf(stdout, "Allocationed range = [%lld, %lld), size = %lld\n", cur_high_water_mark_ptr_ - size - start_ptr_, cur_high_water_mark_ptr_ - start_ptr_, size);

				PtrAndSize ptr_and_size_;
				ptr_and_size_.ptr = cur_high_water_mark_ptr_ - size;
				ptr_and_size_.size = size;

				lists_.push_back(ptr_and_size_);
				map_.insert(std::map<char*, std::list<PtrAndSize>::iterator>::value_type(cur_high_water_mark_ptr_ - size, --lists_.end() ));

				return cur_high_water_mark_ptr_ - size;
			} else { //no space
				//fprintf(stdout, "No space in buffer pool with cycle allocation\n");
				return NULL;
			}
		}
	}

	virtual void Free (char* ptr) {
	    LOG_ASSERT(false);	
	}

	virtual void Free (char* ptr, int64_t size) {
		lock.lock();
		INVARIANT(syko == cur_high_water_mark_ptr_);
		INVARIANT(allocated_size >= 0 && allocated_size <= m_size_bytes_);
		Free_(ptr, size);

		//fprintf(stdout, "Free\t%lld\t%lld\n", (ptr - m_data_), (ptr - m_data_ + size));
		allocated_size -= size;
		INVARIANT(allocated_size >= 0 && allocated_size <= m_size_bytes_);
		syko = cur_high_water_mark_ptr_;
		lock.unlock();
		return;
	}

	void Free_ (char* ptr, int64_t size) {
		//fprintf(stdout, "Try to free buffer\n");
		std::map<char*, std::list<PtrAndSize>::iterator>::iterator map_iter_ = map_.find(ptr);
		if(map_iter_ != map_.end()) { // exist
			if( map_.find(ptr)->second->size != size) { //input size is different from origina ptr alloc size
				fprintf(stdout, "Size is not matched, size = %lld, original size = %lld\n", size, map_.find(ptr)->second->size);
			}
			INVARIANT(map_.find(ptr)->second->size == size);
			lists_.erase(map_[ptr]);
			map_.erase(map_.find(ptr));
			if(cur_high_water_mark_ptr_ >= cur_low_water_mark_ptr_) { // high_water_mark_ptr_ is right of low_water_mark_ptr_
				if(cur_high_water_mark_ptr_ - size == cur_low_water_mark_ptr_ && cur_low_water_mark_ptr_ == ptr) { //마지막 하나 남은거 Free
					//fprintf(stdout, "Free high and low\n");
					cur_high_water_mark_ptr_ = start_ptr_;
					cur_low_water_mark_ptr_ = start_ptr_;
				} else {
					if(cur_high_water_mark_ptr_ - size == ptr) {
						//fprintf(stdout, "Free high\n");
						cur_high_water_mark_ptr_ = lists_.back().ptr + lists_.back().size;
					} else if(cur_low_water_mark_ptr_ == ptr) {
						//fprintf(stdout, "Free low\n");
						cur_low_water_mark_ptr_ = lists_.front().ptr;
					}
					//else
					//fprintf(stdout, "Free middle\n");
				}
			} else { //low_water_mark_ptr_ is right of high_water_mark_ptr_
				if(cur_high_water_mark_ptr_ - size == ptr) {
					//fprintf(stdout, "Free high in cycle\n");
					cur_high_water_mark_ptr_ = lists_.back().ptr + lists_.back().size;
				} else if(cur_low_water_mark_ptr_ == ptr) {
					//fprintf(stdout, "Free low in cycle\n");
					cur_low_water_mark_ptr_ = lists_.front().ptr;
				}
				//else
				//fprintf(stdout, "Free middle\n");
			}
			ptr = NULL;
			return;
		} else { //not in map
			fprintf(stdout, "Not in map\n");
			//throw unsupported_exception();
			abort();
		}
		return;
	}

	virtual int64_t GetSize() {
		return m_size_bytes_;
	}

	virtual int64_t GetRemainingBytes() {
		lock.lock();
		std::map<char*, std::list<PtrAndSize>::iterator>::iterator it;

        int64_t bytes = 0;
        for (it = map_.begin(); it != map_.end(); it++) {
            bytes += it->second->size;
        }
        
		lock.unlock();
        return (m_size_bytes_ - bytes);
	}

  private:
	char* m_data_;
	int64_t m_size_bytes_;
	int64_t allocated_size;

	char* cur_low_water_mark_ptr_;// low_water_mark_ 주소
	char* cur_high_water_mark_ptr_;// high_water_mark_ 주소
	char* start_ptr_;// 버퍼 풀의 시작 주소
	char* last_ptr_;// 버퍼 풀의 마지막 주소
	char* syko;


	atom lock;

	std::list<PtrAndSize> lists_;
	std::map<char*, std::list<PtrAndSize>::iterator> map_;
};

