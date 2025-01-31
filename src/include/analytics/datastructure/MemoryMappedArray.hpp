#ifndef MEMORY_MAPPED_ARRAY_H
#define MEMORY_MAPPED_ARRAY_H

/* 
 * Design of the MemoryMappedArray
 *
 * This class provides functions to map binary files into memory and then 
 * access them like as an array. For performance purposes, it also provides 
 * a function to lock (and also, unlock) the memory to prevent that memory 
 * from being paged to the swap area. 
 */

#include "Turbo_bin_mmapper.hpp"
#include "TypeDef.hpp"

template <typename T>
class MemoryMappedArray {
  public:
	MemoryMappedArray() : array_ (NULL), is_mlocked_(false) {}
	MemoryMappedArray(const char* file_name): is_mlocked_(false) {
		Open(file_name);
	}

	
	MemoryMappedArray(const char* file_name, std::size_t file_size) :is_mlocked_(false){
		Create(file_name, file_size);
	}

    ~MemoryMappedArray() {
		Close();
	}
	
    void Close(bool rm = false) {
		Munlock();
		bin_reader_.Close(rm);
		array_ = NULL;
	}

	/* 
	* Open the given memory-mapped file
	*/
	ReturnStatus Open(const char* file_name, bool write_enabled = false) {
        file_name_ = std::string(file_name);
		ReturnStatus st;
		st = bin_reader_.OpenFileAndMemoryMap(file_name, write_enabled);
		is_mlocked_ = false;
		if (st != OK) {
			fprintf(stderr, "[MemoryMappedArray] Open (%s) failed\n", file_name);
			return st;
		}
		array_ = (T*)bin_reader_.data();
		return st;
	}

	/*
	* Create a memory-mapped file for the given file name and the size of the array
	*/
	ReturnStatus Create(const char* file_name, std::size_t num_entries) {
		ReturnStatus st;
		st = bin_reader_.CreateFileAndMemoryMap(file_name, sizeof(T) * num_entries);
		is_mlocked_ = false;
        if (st != OK) {
            perror("[MemoryMappedArray] CreateFileAndMemoryMap failed");
            LOG_ASSERT(false);
        }
		array_ = (T*)bin_reader_.data();
		return st;
	}

	/*
	* Return the entry in the given location
	*/
	T& operator[](std::size_t idx) {
		ALWAYS_ASSERT(idx >= 0 && idx < bin_reader_.file_size() / sizeof(T));
		return array_[idx];
	}

	/*
	* Return the entry in the given location
	*/
	T& operator[](std::size_t idx) const {
		//ALWAYS_ASSERT(idx >= 0 && idx < bin_reader_.file_size() / sizeof(T));
		return array_[idx];
	}

	/*
	* It locks the whole memory space for the file using the mlock in https://linux.die.net/man/2/mlock
	* The mlock() respectively lock part or all of the calling process's virtual address space into RAM, 
	* preventing that memory from being paged to the swap area.
	*/
	void Mlock(bool lock_on_fault) {
		Range<std::size_t> range (0, length() - 1);
		return MlockRange(range, lock_on_fault);
	}

	/*
	* It locks the memory space in the given range using the mlock in https://linux.die.net/man/2/mlock
	* The mlock() respectively lock part or all of the calling process's virtual address space into RAM, 
	* preventing that memory from being paged to the swap area.
	*/
	// 'range' must be inclusive; [begin, end]
	void MlockRange(Range<std::size_t> range, bool lock_on_fault) {
		std::size_t size = range.length();
		int error;
		if (lock_on_fault) {
			// XXX - syko: mlock2 is avilable after Linux 4.4.... T_T
			//error = mlock2( (void*) (array_ + range.GetBegin()), sizeof(T) * size, MLOCK_ONFAULT);
			error = madvise( (void*) (array_ + range.GetBegin()), sizeof(T) * size, MADV_WILLNEED);
		} else {
			error = mlock( (void*) (array_ + range.GetBegin()), sizeof(T) * size);
		}
		if (error != 0) {
			perror("[MemoryMappedArray] mlock has failed");
//                fprintf(stderr, "[MemoryMappedArray] mlock has failed, error code = %d\n", error);
			abort();
		}
		mlocked_range_ = range;
		is_mlocked_ = true;
		return;
	}

	/*
	* It unlocks the locked memory space by the mlock
	*/
	void Munlock() {
		if (!is_mlocked_) {
			return;
		}
		std::size_t size = mlocked_range_.length();
		int error = munlock((void*) (array_ + mlocked_range_.GetBegin()), sizeof(T) * size);
		if (error != 0) {
			fprintf(stderr, "[MemoryMappedArray] munlock has failed\n");
			abort();
		}
		is_mlocked_ = false;
		mlocked_range_.Set(0, 0);
		return;
	}

	/*
	* Copy the entries of the given memory mapped array to this in the given range
	*/
    void CopyFrom(MemoryMappedArray<T>* src, Range<int64_t> range) {
        INVARIANT(src->length() == length());
        memcpy((void*) (data() + range.GetBegin()), (void*) (src->data() + range.GetBegin()), sizeof(T) * range.length());
    }

 	/*
	* Copy the entries of the given memory mapped array to this
	* It assumes that the size of this array is larger than the size of the array that we want to copy
	*/
   void CopyFrom(MemoryMappedArray<T>* src) {
        INVARIANT(src->length() == length());
        memcpy((void*)data(), (void*)src->data(), sizeof(T) * length());
    }
    
	/*
	* Copy the entries of this array to the given memory mapped array
	* It assumes that the size of the given array is larger than the size of this array
	*/
    void CopyTo(T* dst) {
        INVARIANT(dst != NULL);
        memcpy((void*)dst, (void*)data(), sizeof(T) * length());
    }
 
	/*
	* Copy the entries of this array A to the given array B
	* For each entry at location l, it copies the A[l] to B[l] if A[l] is different with B[l]
	*/
    void CopyToIfDirty(T* dst, Range<int64_t> idx_range) {
        INVARIANT(dst != NULL);
        for (int64_t idx = idx_range.GetBegin(); idx <= idx_range.GetEnd(); idx++) {
            if (array_[idx] != dst[idx]) dst[idx] = array_[idx];
        }
    }  

	/*
	* Copy the entries in the given array to this array
	*/ 
    void CopyFrom(T* src) {
        INVARIANT(src != NULL);
        memcpy((void*)data(), (void*)src, sizeof(T) * length());
    }

	/*
	* Copy the entries in the given range of the given array A to the this array B
	* For each entry at location l, it copies the A[l] to B[l] if A[l] is different with B[l]
	*/
    void CopyFromIfDirty(T* src, Range<int64_t> idx_range) {
        INVARIANT(src != NULL);
        for (int64_t idx = idx_range.GetBegin(); idx <= idx_range.GetEnd(); idx++) {
            if (array_[idx] != src[idx]) array_[idx] = src[idx];
        }
    }
    
	/*
	* Copy the entries of the given array A to the this array B
	* For each entry at location l, it copies the A[l] to B[l] if A[l] is different with B[l]
	*/
    void CopyFromIfDirty(T* src) {
        INVARIANT(src != NULL);
        for (int64_t idx = 0; idx < length(); idx++) {
            if (array_[idx] != src[idx]) array_[idx] = src[idx];
        }
    }

	/*
	* Copy the entries of the given array A to the this array B
	* For each entry at location l, it copies the A[l] to B[l] if A[l] is different with B[l]
	*/
    void CopyFromIfDirty(MemoryMappedArray<T>* src_mmap) {
        INVARIANT(src_mmap != NULL);
        T* src = src_mmap->data();
        CopyFromIfDirty(src);
    }

	/*
	* Copy the entries of the given array A to the this array B
	* For each entry at location l, it copies the A[l] to B[l] if A[l] is different with B[l] and copied[l] is true
	*/
    void CopyFromIfDirtyAndMark(T* src, bool* copied) {
        INVARIANT(src != NULL);
        for (int64_t idx = 0; idx < length(); idx++) {
            if (array_[idx] != src[idx]) {
                array_[idx] = src[idx];
                copied[idx] = true;
            }
        }
    }
    
	/*
	* return the memory address of this array
	*/
    T* data() {
        return array_;
    }

	/*
	* return the size of this array
	*/
	std::size_t length() {
		return bin_reader_.file_size() / sizeof(T);
	}
	
  private:
	T* array_; /* the pointer to a memory space for this array */
	Turbo_bin_mmapper bin_reader_;

	bool is_mlocked_; /* a boolean value that indicates that the memory space of this array is locked */
	Range<std::size_t> mlocked_range_; /* the range of locked memory space */
    std::string file_name_; /* the file name of this memory-mapped array */
};

#endif
