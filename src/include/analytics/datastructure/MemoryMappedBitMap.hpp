#ifndef MEMORYMAPPEDBITMAP_H
#define MEMORYMAPPEDBITMAP_H

/*
 * Design of the MemoryMappedBitMap
 *
 * This class provides same APIs as the BitMap class. The difference is that 
 * the bitmap is stored in a binary file, not the allocated space in memory, 
 * and it is memory mapped.
 */

#include <algorithm>
#include <atomic>
#include <vector>
#include <stdexcept>

#include "analytics/core/TypeDef.hpp"
#include "analytics/util/util.hpp"

#define SizeInBits(X) ((8*sizeof(X)))


template <typename IdxTmp_t>
class MemoryMappedBitMap {
  public:

    typedef int64_t Idx_t;
	typedef int64_t bitmap_entry_t;

    /*
    * Construct the memory-mapped bitmap
    */
	MemoryMappedBitMap() : entries_ (NULL) {
	}

    /*
    * Deconstruct the memroy-mapped array: 1) Unlock the memory space, 2) close the memory-mapped file 
    */
	~MemoryMappedBitMap() {
		if (entries_ != NULL) {
		    NumaHelper::free_numa_memory(entries_, sizeof(bitmap_entry_t) * array_size);
			//delete entries_;
		    entries_ = NULL;
		}
	}

    /* 
    * Open the given memory-mapped file for this bitmap
    */

    void Open(const char* file_name, bool write_enabled = false) {
        entries_mmap_.Open(file_name, write_enabled);
        entries_ = (bitmap_entry_t *) entries_mmap_.data();
        num_entries_ = entries_mmap_.length() * SizeInBits(bitmap_entry_t); // XXX maybe slightly larger than real num_entries
    }


    /*
    * For the given file name and the size of bitmap, create the memory-mapped file
    */
    void Create(const char* file_name, Idx_t num_entries) {
        ALWAYS_ASSERT (num_entries > 0);
        ALWAYS_ASSERT ( (sizeof(bitmap_entry_t) * ((num_entries + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t))) >= (num_entries + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t));
        num_entries_ = num_entries;
        array_size = ((num_entries + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t));
        entries_mmap_.Create(file_name, array_size);
        entries_ = (bitmap_entry_t *) entries_mmap_.data();
        //entries_ = (bitmap_entry_t *) NumaHelper::alloc_numa_memory(sizeof(bitmap_entry_t) * array_size);
        //entries_ = (bitmap_entry_t *) malloc(sizeof(bitmap_entry_t) * array_size);
        //memset(entries_, 0, array_size * sizeof(bitmap_entry_t));
    }

    /*
    * Close the memory-mapped file for this bitmap
    */
    void Close(bool rm = false) {
        entries_mmap_.Close(rm);
    }

    /*
    * Return the memory address to the memory-mapped file
    */
    int64_t* GetBitMap() {
        return entries_;
    }
	

    /*
    * For the given range, union the given bitmaps rs and write ther results to r
    */
    // assuming that l == o
    /*
    * Union the parts of given bitmaps rs and write the results to o
    *
    * Input bitmaps (o, rs)
    *                       [ from ... to ]
    *    o[k] : [ 1, 0, ...,| 0, 0, 1, 0, | ... ]
    *   rs[0] : [ 0, 1, ...,| 0, 1, 0, 0, | ... ]
    *   rs[1] : [ 1, 1, ...,| 0, 1, 0, 1, | ... ]
    * 
    * Output bitmap: (o)
    *    o[k] : [ 1, 0, ...,| 0, 1, 1, 1, | ... ]   
    *
    */
    static void Union(MemoryMappedBitMap<Idx_t>& l, std::vector<MemoryMappedBitMap<Idx_t>*>& rs, MemoryMappedBitMap<Idx_t>& o, Idx_t from, Idx_t to, bool single_thread) {
		ALWAYS_ASSERT (l.num_entries() == o.num_entries());
       
        Range<Idx_t> idx_range(from, to);

        if (idx_range.length() <= 63) {
            /* Union small bitmaps */
            //if (true) {
            if (single_thread) {
                for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                    for (auto& r: rs) {
                        if (r->Get(i)) o.Set(i);
                    }
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                    for (auto& r: rs) {
                        if (r->Get(i)) o.Set_Atomic(i);
                    }
                }
            }
        } else {
            /* Union large bitmaps
            * 
            *      64 bits        64 bits              64 bits
            * | 1, 0, ..., 1 | 1, 0, ..., 1 | ... | 1, 0, ..., 1 | 
            *          ----------------------------------
            *              A |         B          |  C
            *            the part of a bitmap to union
            *
            * It process the union operation using 64 bit OR operations for the middle bits in B
            * It process the union operation using if-else estatements for the bits in A and C
            */
            Idx_t from = idx_range.GetBegin();
            Idx_t to = idx_range.GetEnd();
            Idx_t bytefrom = from / SizeInBits(bitmap_entry_t);
            Idx_t byteto =  to / SizeInBits(bitmap_entry_t);

            if (from % SizeInBits(bitmap_entry_t) != 0) {
                /* Process the union operation for the bits in A */
                Idx_t upto = from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t) - 1;
                upto = std::min(upto, to);
                if (single_thread) {
                    for (Idx_t i = from; i <= upto; i++) {
                        for (auto& r: rs) {
                            if (r->Get(i)) o.Set(i);
                        }
                    }
                } else {
#pragma omp parallel for
                    for (Idx_t i = from; i <= upto; i++) {
                        for (auto& r: rs) {
                            if (r->Get(i)) o.Set_Atomic(i);
                        }
                    }
                }
                bytefrom = (upto + 1) / SizeInBits(bitmap_entry_t);
            }
            
            if (byteto - bytefrom > 0) {
                /* Process the union operation for the bits in B */
                if (single_thread) {
                    for (int64_t i = bytefrom; i < byteto; i++) {
                        for (auto& r: rs) {
                            if (r->entries_[i] == 0) continue;
                            o.entries_[i] = l.entries_[i] | r->entries_[i];
                        }
                    }
                } else {
#pragma omp parallel for
                    for (int64_t i = bytefrom; i < byteto; i++) {
                        for (auto& r: rs) {
                            if (r->entries_[i] == 0) continue;
                            o.entries_[i] = l.entries_[i] | r->entries_[i];
                        }
                    }
                }
            }

            /* Process the union operation for the bits in C */
            if (single_thread) {
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    for (auto& r: rs) {
                        if (r->Get(i)) o.Set(i);
                    }
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    for (auto& r: rs) {
                        if (r->Get(i)) o.Set_Atomic(i);
                    }
                }
            }

        }
    }

    /*
    * Union the two bitmaps l and r and write the result to the output bitmap o
    */
    static void Union(MemoryMappedBitMap<Idx_t>& l, MemoryMappedBitMap<Idx_t>& r, MemoryMappedBitMap<Idx_t>& o) {
		ALWAYS_ASSERT (l.num_entries() == o.num_entries());
		ALWAYS_ASSERT (l.num_entries() == r.num_entries());
		#pragma omp parallel for
		for (Idx_t i = 0; i < o.array_size; i++) {
			o.entries_[i] = l.entries_[i] | r.entries_[i];
		}
	}

    /*
    * Check whether the result of the intersection of two bitmaps l and r is empty 
    */
	static bool IsIntersectionEmpty(MemoryMappedBitMap<Idx_t>& l, MemoryMappedBitMap<Idx_t>& r) {
        //ALWAYS_ASSERT (l.num_entries() == o.num_entries());
        ALWAYS_ASSERT (l.num_entries() == r.num_entries());
        for (Idx_t i = 0; i < l.array_size; i++) {
            bitmap_entry_t tmp = l.entries_[i] & r.entries_[i];
		    if (tmp != 0) return false;
        }
        return true;
    }

    /*
    * Intersect the two bitmaps l and r and write the result to the output bitmap o
    */
	static void Intersection(MemoryMappedBitMap<Idx_t>& l, MemoryMappedBitMap<Idx_t>& r, MemoryMappedBitMap<Idx_t>& o) {
		ALWAYS_ASSERT (l.num_entries() == o.num_entries());
		ALWAYS_ASSERT (l.num_entries() == r.num_entries());
		#pragma omp parallel for
		for (Idx_t i = 0; i < o.array_size; i++) {
			o.entries_[i] = l.entries_[i] & r.entries_[i];
		}
	}

    /*
    * Swap the two bitmaps
    */
    static void SwapBitMap(MemoryMappedBitMap<Idx_t>& l, MemoryMappedBitMap<Idx_t>& r) {
        l.SwapBitMap(r);
    }

    /*
    * For the given range, copy the bits from the given bitmap to the given offset
    * For each l in the given range, copy the value of A[l] to B[to_offset + l]
    */
	void CopyFromWithRange(MemoryMappedBitMap<Idx_t>& from, Range<Idx_t> from_range, Idx_t to_offset) {
		CopyFromWithRange(from, from_range.GetBegin(), from_range.GetEnd(), to_offset);
	}

    /*
    * For the given range, copy the bits from the given bitmap to the given offset
    * For each l in the given range, copy the value of A[l] to B[to_offset + l]
    */
	void CopyFromWithRange(MemoryMappedBitMap<Idx_t>& from, Idx_t range_begin_idx, Idx_t range_end_idx, Idx_t to_offset) {
		#pragma omp parallel for
		for (Idx_t i = range_begin_idx; i <= range_end_idx; i++) {
			if (from.Get(i)) {
				this->Set_Atomic(i + to_offset);
			} else {
				this->Clear_Atomic(i + to_offset);
			}
		}
	}

    /*
    * For the given range, copy the bits from the given bitmap A to the this bitmap B
    * For each l in the given range, copy the value of A[l] to B[l]
    */
	void CopyFromWithRange(MemoryMappedBitMap<Idx_t>& from, Range<Idx_t> idx) {
		CopyFromWithRange(from, idx.GetBegin(), idx.GetEnd());
	}

    /*
    * For the given range, copy the bits from the given bitmap to this bitmap
    * For each l in the given range, copy the value of A[l] to B[l]
    */
	void CopyFromWithRange(MemoryMappedBitMap<Idx_t>& from, Idx_t from_idx, Idx_t to_idx) {
		ALWAYS_ASSERT (array_size == from.array_size);
		#pragma omp parallel for
		for (Idx_t i = from_idx; i <= to_idx; i++) {
			if (from.Get(i)) {
				this->Set_Atomic(i);
			} else {
				this->Clear_Atomic(i);
			}
		}
	}

    /*
    * Copy the bits from the given bitmap to the this bitmap
    */
	void CopyFrom(MemoryMappedBitMap<Idx_t>& from) {
		ALWAYS_ASSERT (array_size == from.array_size);
		#pragma omp parallel for
		for (Idx_t i = 0; i < array_size; i++) {
			entries_[i] = from.entries_[i];
		}
	}

    /*
    * Sum the bits in the given range [from, to] using multiple threads
    */
	int64_t GetTotalInRange(Idx_t from, Idx_t to) const {
		ALWAYS_ASSERT(from >= 0 && from / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(to >= 0 && to / SizeInBits(bitmap_entry_t) < array_size);
		int64_t total_cnts = 0;
		#pragma omp parallel
		{
			int64_t cnt = 0;
			#pragma omp for
			for (Idx_t i = from; i <= to; i++) {
				cnt += Get(i);
			}
			std::atomic_fetch_add((std::atomic<int64_t>*) &total_cnts, cnt);
		}
		return total_cnts;
	}
    /*
    * Sum the bits in the given range [from, to] using a single thread
    */
	int64_t GetTotalInRangeSingleThread(Idx_t from, Idx_t to) const {
		ALWAYS_ASSERT(0 <= from && from / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(0 <= to && to / SizeInBits(bitmap_entry_t) < array_size);
		int64_t total_cnts = 0;
		for (Idx_t i = from; i <= to; i++) {
			total_cnts += Get(i);
		}
		return total_cnts;
	}

    /*
    * Sum the bits in the given range using a single thread
    */
	int64_t GetTotalInRangeSingleThread(Range<Idx_t> range) {
		return GetTotalInRangeSingleThread(range.GetBegin(), range.GetEnd());
	}

    /*
    * Sum the bits in this array using multiple threads
    */
	int64_t GetTotal() {
		return GetTotalInRange(0, num_entries_ - 1);
	}

    /*
    * Sum the bits in this array using a single thread
    */
	int64_t GetTotalSingleThread() {
		return GetTotalInRangeSingleThread(0, num_entries_ - 1);
	}

    /*
    * Set the all bits to 1 using multi threads
    */
	void SetAll() {
		bitmap_entry_t bittemp = 0;
		bittemp = ~bittemp;
		#pragma omp parallel for
		for (std::size_t i = 0; i < array_size; i++) {
			entries_[i] = bittemp;
		}
	}
    /* Set the all bits to zero */
	void ClearAll() {
		ALWAYS_ASSERT (entries_ != NULL);
        memset(entries_, 0, array_size * sizeof(bitmap_entry_t));
	}
    /* Set the bit at the given location to 1 */
	inline void Set(Idx_t idx) {
		ALWAYS_ASSERT(idx >= 0 &&(idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t or_val = one << bit_loc;
		entries_[bit_pos] |= or_val;
	}
    /* Set the bit at the given location to 0 */
	inline void Clear(Idx_t idx) {
		ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t and_val = ~(one << bit_loc);
		entries_[bit_pos] &= and_val;
	}
    /* Get the value of the bit at the given location */
	inline bool Get(Idx_t idx) const {
		ALWAYS_ASSERT(0 <= idx);
		ALWAYS_ASSERT((idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
		return (bool)val;
	}
    /* Get the value of the bit at the given location */
    inline bool operator[](int64_t idx) {
        ALWAYS_ASSERT(0 <= idx);
        ALWAYS_ASSERT((idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
		return (bool)val;
    }
    /* Get the value of the bit at the given location atomically */
	inline bool Get_Atomic(Idx_t idx) const {
		ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t val = std::atomic_load((std::atomic<bitmap_entry_t>*)&entries_[bit_pos]);
		return (val >> bit_loc) & one;
	}

    /* 
    *  1) Set the bit at the given location to 1 if the bit is 0 
    *  2) Return true if the bit was 0, else return false
    */
    inline bool Set_IfNotSet(Idx_t idx) {
        ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
        Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
        if (!val) {
            bitmap_entry_t or_val = one << bit_loc;
            entries_[bit_pos] |= or_val;
        }
    }

    /* 
    *  1) Set the bit at the given location to 1 atomically if the bit is 0 
    *  2) Return true if the bit was 0, else return false
    */
    inline bool Set_Atomic_IfNotSet(Idx_t idx) {
        ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
        Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
        if (!val) return Set_Atomic(idx);  
        return false;
    }

    /* 
    *  1) Set the bit at the given location to 1 if the bit is 0 
    *  2) Return true if the bit was 0, else return false
    */
	inline bool Set_Atomic(Idx_t idx) {
		ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t or_val = one << bit_loc;
		bitmap_entry_t old_val = std::atomic_fetch_or((std::atomic<bitmap_entry_t>*) &entries_[bit_pos], or_val);
		if (((old_val >> bit_loc) & one) == 0)
			return true; // Am I the one who set the bit?
		return false;
	}

    /* 
    *  1) Set the bit at the given location to 0 if the bit is 1 
    *  2) Return true if the bit was 1, else return false
    */
	inline bool Clear_Atomic(Idx_t idx) {
		ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t and_val = ~(one << bit_loc);
		bitmap_entry_t old_val = std::atomic_fetch_and((std::atomic<bitmap_entry_t>*) &entries_[bit_pos], and_val);
		if (((old_val >> bit_loc) & one) == 1)
			return true; // Am I the one who cleared the bit?
		return false;
	}

    /*
    * Set the bits in the given range [offset + range.GetBegin(), offset + range.GetEnd()] to 1
    */
	inline void SetRangeWithOffset(Idx_t offset, Range<Idx_t> range) {
		SetRange(offset + range.GetBegin(), offset + range.GetEnd());
	}

    /*
    * Set the bits in the given range [offset + range.GetBegin(), offset + range.GetEnd()] to 0
    */
	inline void ClearRangeWithOffset(Idx_t offset, Range<Idx_t> range) {
		ClearRange(offset + range.GetBegin(), offset + range.GetEnd());
	}

    /*
    * Set the bits in the given range to 1
    */
	inline void SetRange(Range<Idx_t> range) {
		SetRange(range.GetBegin(), range.GetEnd());
	}
    /*
    * Set the bits in the given range to 0
    */
	inline void ClearRange(Range<Idx_t> range) {
		ClearRange(range.GetBegin(), range.GetEnd());
	}

    /*
    * Set the bits in the given range [from, to] to 1 
    */
	inline void SetRange(Idx_t from, Idx_t to) {
		ALWAYS_ASSERT(0 <= from && from / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(0 <= to && to / SizeInBits(bitmap_entry_t) < array_size);
		for (Idx_t i = from; i <=to; i++) {
			Set(i);
		}
	}

    /*
    * Set the bits in the given range [from, to] to 0
    */
	inline void ClearRange(Idx_t from, Idx_t to) {
		ALWAYS_ASSERT(0 <= from && from / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(0 <= to && to / SizeInBits(bitmap_entry_t) < array_size);
        
        if (to - from <= 63) {
            /* Clear the small bitmap */
            for (Idx_t i = from; i <=to; i++) {
                Clear(i);
            }
        } else {
            /* Clear the large bitmap 
            * 
            *      64 bits        64 bits              64 bits
            * | 1, 0, ..., 1 | 1, 0, ..., 1 | ... | 1, 0, ..., 1 | 
            *          ----------------------------------
            *              A |         B          |  C
            *            the part of a bitmap to clear
            *
            * It sets bits to 0 in the part B using memcpy 
            * It sets bits to 0 in A and C in 1 bit unit
            */
            Idx_t bytefrom = from / SizeInBits(bitmap_entry_t);
            Idx_t byteto =  to / SizeInBits(bitmap_entry_t);

            if (from % SizeInBits(bitmap_entry_t) != 0) {
                 /* Set bits to 0 in part A */
                Idx_t upto = (from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t));
                for (Idx_t i = from; i < upto; i++) {
                    Clear(i);
                }
                bytefrom = upto / SizeInBits(bitmap_entry_t);
            }

            if (byteto - bytefrom > 0) {
                /* Set bits to 0 in part B */
                memset(&entries_[bytefrom], 0, (byteto - bytefrom) * sizeof(bitmap_entry_t));
            }

            /* Set bits to 0 in part C */
            for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                Clear(i);
            }
        }
	}
/*
    inline bool IsSetInRange(Idx_t from, Idx_t to) const {
        ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
        ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);
        for (int64_t idx = from; idx <= to; idx++) {
            if (Get(idx)) return true;
        }
        return false;
    }
    */

    /* 
    * Check whether there is at least one bit with a value of 1 in the range
    */
    inline bool IsSetInRange(Idx_t from, Idx_t to) const {
        ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
        ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);
        ALWAYS_ASSERT(from <= to);
        Idx_t from_idx = from / SizeInBits(bitmap_entry_t);
        Idx_t to_idx = to / SizeInBits(bitmap_entry_t);
        Idx_t found = -1;

        /*  
        *      64 bits        64 bits              64 bits
        * | 1, 0, ..., 1 | 1, 0, ..., 1 | ... | 1, 0, ..., 1 | 
        *          ----------------------------------
        *              A |         B          |  C
        *            the part of a bitmap in the given range
        */
        if (from % SizeInBits(bitmap_entry_t) != 0) {
            /* Check whether there is at least one bit with a value of 1 in the part A */
            Idx_t upto = from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t) - 1;
            //ALWAYS_ASSERT((from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t)) % SizeInBits(bitmap_entry_t) == 0);
            upto = std::min(upto, to);
            if (GetSetBits(from_idx, from % SizeInBits(bitmap_entry_t), upto % SizeInBits(bitmap_entry_t)) != 0) {
                return true;
            }
            from_idx += 1;
        }

        for (Idx_t idx = from_idx; idx < to_idx; idx++) {
            /* Check whether there is at least one bit with a value of 1 in the part B */
            if (entries_[idx] != 0) {
                return true;
            }
        }

        if (to_idx >= from_idx) {
            /* Check whether there is at least one bit with a value of 1 in the part C */
            if (GetSetBits(to_idx, 0, to % SizeInBits(bitmap_entry_t)) != 0) {
                return true;
            }
        }
        return false;
	}

/*
	inline bool IsSetInRange(Idx_t from, Idx_t to) {
		ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);

		Idx_t from_idx = from/SizeInBits(bitmap_entry_t);
		Idx_t to_idx = to/SizeInBits(bitmap_entry_t);
		Idx_t fromlast = from%SizeInBits(bitmap_entry_t);
		Idx_t tolast = to%SizeInBits(bitmap_entry_t);
		if(from_idx== to_idx) {
			if (entries_[from_idx] & (18446744073709551615UL - firstbitmask[fromlast] - lastbitmask[7-tolast])) return true;
			//if (entries_[from_idx] & (18446744073709551615UL - firstbitmask[fromlast] - lastbitmask[63-tolast])) return true;
			return false;
		}
		if(entries_[from_idx] & lastbitmask[SizeInBits(bitmap_entry_t) - fromlast]) return true;
		from_idx++;

		while(from_idx < to_idx) {
			if(entries_[from_idx] != 0) return true;
			from_idx++;
		}
		if (entries_[to_idx] & firstbitmask[tolast+1]) return true;
		return false;
	}
*/

/*
	inline Idx_t FindLastMarkedEntry(Idx_t from, Idx_t to) {
		ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t from_idx = from / SizeInBits(bitmap_entry_t);
		Idx_t to_idx = to / SizeInBits(bitmap_entry_t);
		Idx_t found = -1;
		for (Idx_t idx = to_idx; idx >= from_idx; idx--) {
			if (entries_[idx] != 0) {
				found = SizeInBits(bitmap_entry_t) * idx;
				break;
			}
		}
		if (found == -1) return -1;
		to = (to > found) ? to : found;
		for (Idx_t idx = to; idx >= from; idx--) {
			Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
			Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
			bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
			if (val) {
				return idx;
			}
		}
		return -1;
	}
*/

    /*
    * Get the bits in the given range: [64 * idx_at + from_bit, 64 * idx_at + to_bit ]
    */
	// Check if any bit of range [from_bit, to_bit] in entries_[idx_at] is set
	inline bitmap_entry_t GetSetBits(Idx_t idx_at, Idx_t from_bit, Idx_t to_bit) const {
        ALWAYS_ASSERT(from_bit >= 0 && from_bit < SizeInBits(bitmap_entry_t));
        ALWAYS_ASSERT(to_bit >= 0 && to_bit < SizeInBits(bitmap_entry_t));
        ALWAYS_ASSERT(from_bit <= to_bit);
        int64_t mask, submask_1, submask_2;
        submask_1   = submask_2 = one;
        submask_1 <<= from_bit;         // set the ath bit from the left
        submask_1 = (submask_1 - 1);    // make 'a' an inclusive index and fill all bits after ath bit
        submask_2 <<= to_bit;           // set the bth bit from the left
        submask_2 |= (submask_2 - 1);   // make 'b' an inclusive index and fill all bits after bth bit
        mask = submask_1 ^ submask_2;
        return entries_[idx_at] & mask; // 'v' now only has set bits in specified range
	}

    /*
    * Find the location of the first bit with a value of 1
    */
	inline Idx_t FindFirstMarkedEntry() const {
        return FindFirstMarkedEntry(0, num_entries_ - 1);
    }

    /*inline Idx_t FindFirstMarkedEntry(Idx_t from, Idx_t to) const {
        ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
        ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);
        for (int64_t idx = from; idx <= to; idx++) {
            if (Get(idx)) return idx;
        }
        return -1;
    }*/

    /*
    * Find the location of the first bit with a value of 1 in the given range
    */    
    inline Idx_t FindFirstMarkedEntry(Idx_t from, Idx_t to) const {
        ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
        ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);
        Idx_t from_idx = from / SizeInBits(bitmap_entry_t);
        Idx_t to_idx = to / SizeInBits(bitmap_entry_t);
        Idx_t found = -1;

        /* Find the bit with a value of 1 using 64 bit operations first */
        if (from % SizeInBits(bitmap_entry_t) != 0) {
            Idx_t upto = from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t) - 1;
            upto = std::min(upto, to);
            for (Idx_t idx = from; idx <= upto; idx++) {
                if (Get(idx)) {
                    return idx;
                }
            }
            //from_idx += 1;
        }
        /* Find the bit with a value of 1 */
        for (Idx_t idx = from_idx; idx <= to_idx; idx++) {
            if (entries_[idx] != 0) {
                found = SizeInBits(bitmap_entry_t) * idx;
                break;
            }
        }
        if (found == -1) return -1;
        from = (from > found) ? from : found;
        for (Idx_t idx = from; idx <= to; idx++) {
            if (Get(idx)) {
                return idx;
            }
        }
        return -1;
    }

    /*
    * For each bit in the bitmap, invoke the function f(location of the bit) if the bit is 0
    */
    void InvokeIfNotMarked(const std::function<void(node_t)>& f) {
        Range<Idx_t> total_range(0, num_entries_ - 1);
        InvokeIfNotMarked(f, total_range);
    }

    /*
    * For each bit in the given range, invoke the function f(location of the bit) if the bit is 0
    */
    void InvokeIfNotMarked(const std::function<void(node_t)>& f, Range<Idx_t> idx_range) {
        if (idx_range.length() <= 63) {
            /* Invoke the function for the bits with a value of 0 in the short range */
#pragma omp parallel for
            for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                if (!Get(i)) f(i);
            }
        } else {
            /* Invoke the function for the wide range */
            Idx_t from = idx_range.GetBegin();
            Idx_t to = idx_range.GetEnd();
            Idx_t bytefrom = from / SizeInBits(bitmap_entry_t);
            Idx_t byteto =  to / SizeInBits(bitmap_entry_t);

            /*  
            *      64 bits        64 bits              64 bits
            * | 1, 0, ..., 1 | 1, 0, ..., 1 | ... | 1, 0, ..., 1 | 
            *          ----------------------------------
            *              A |         B          |  C
            *            the part of a bitmap in the given range
            */

            if (from % SizeInBits(bitmap_entry_t) != 0) {
                /* Invoke the function f for the bits with a value of 1 in the part A */
                Idx_t upto = (from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t));
                for (Idx_t i = from; i < upto; i++) {
                    if (!Get(i)) f(i);
               }
                bytefrom = upto / SizeInBits(bitmap_entry_t);
            }
            
            if (byteto - bytefrom > 0) {
                bitmap_entry_t mask = 0;
                mask = ~mask;
                /* Invoke the function f for the bits with a value of 1 in the part B */
#pragma omp parallel for
                for (int64_t i = bytefrom; i < byteto; i++) {
                    if (entries_[i] == mask) continue;
                    for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                        bool val = (bool) ((entries_[i] >> j) & one);
                        if (!val) { 
                            f(i * SizeInBits(bitmap_entry_t) + j);
                        }
                    }
                }
            }
            /* Invoke the function f for the bits with a value of 1 in the part C */
            for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                if (!Get(i)) f(i);
            }
        }  
    }

    /*
    * For each bit in the given range, invoke the function f(location of the bit) if the bit is 1
    */
    void InvokeIfMarked(const std::function<void(node_t)>& f, Range<Idx_t> idx_range, bool single_thread = false, bool clear_after_invoke = false) {
        if (idx_range.length() <= 63) {
            /* Invoke the function for the bits with a value of 1 in the short range */
        //if (true) {
            if (single_thread) {
                for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                    if (Get(i)) f(i);
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                    if (Get(i)) f(i);
                }
            }
        } else {
            /*  
            *      64 bits        64 bits              64 bits
            * | 1, 0, ..., 1 | 1, 0, ..., 1 | ... | 1, 0, ..., 1 | 
            *          ----------------------------------
            *              A |         B          |  C
            *            the part of a bitmap in the given range
            */
            Idx_t from = idx_range.GetBegin();
            Idx_t to = idx_range.GetEnd();
            Idx_t bytefrom = from / SizeInBits(bitmap_entry_t);
            Idx_t byteto =  to / SizeInBits(bitmap_entry_t);

            if (from % SizeInBits(bitmap_entry_t) != 0) {
                /* Invoke the function f for the bits with a value of 1 in the part A */
                Idx_t upto = from + SizeInBits(bitmap_entry_t) - from % SizeInBits(bitmap_entry_t) - 1;
                upto = std::min(upto, to);
                if (single_thread) {
                    for (Idx_t i = from; i <= upto; i++) {
                        if (Get(i)) f(i);
                    }
                } else {
#pragma omp parallel for
                    for (Idx_t i = from; i <= upto; i++) {
                        if (Get(i)) f(i);
                    }
                }
                bytefrom = (upto + 1) / SizeInBits(bitmap_entry_t);
            }
            
            if (byteto - bytefrom > 0) {
                /* Invoke the function f for the bits with a value of 1 in the part B */
                if (single_thread) {
                    for (int64_t i = bytefrom; i < byteto; i++) {
                        if (entries_[i] == 0) continue;
                        for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                            bool val = (bool) ((entries_[i] >> j) & one);
                            if (val) { 
                                f(i * SizeInBits(bitmap_entry_t) + j);
                            }
                        }
                    }
                } else {
#pragma omp parallel for
                    for (int64_t i = bytefrom; i < byteto; i++) {
                        if (entries_[i] == 0) continue;
                        for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                            bool val = (bool) ((entries_[i] >> j) & one);
                            if (val) { 
                                f(i * SizeInBits(bitmap_entry_t) + j);
                            }
                        }
                    }
                }
            }
            /* Invoke the function f for the bits with a value of 1 in the part C */
            if (single_thread) {
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    if (Get(i)) f(i);
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    if (Get(i)) f(i);
                }
            }

        }
    }
    /*
    * For each bit in the given range, invoke the function f(location of the bit) if the bit is 1
    */
    void InvokeIfMarked(const std::function<void(node_t)>& f, bool single_thread = false) {
        if (single_thread) {
            for (int64_t i = 0; i < array_size; i++) {
                if (entries_[i] == 0) continue;
                for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                    bool val = (bool) ((entries_[i] >> j) & one);
                    if (val) { 
                        f(i * SizeInBits(bitmap_entry_t) + j);
                    }
                }
            }
        } else {
#pragma omp parallel for
            for (int64_t i = 0; i < array_size; i++) {
                if (entries_[i] == 0) continue;
                for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                    bool val = (bool) ((entries_[i] >> j) & one);
                    if (val) { 
                        f(i * SizeInBits(bitmap_entry_t) + j);
                    }
                }
            }
        }
    }

    /* @tslee */
    /* Deprecated
    void SetOffset(Idx_t offset) {
        Idx_t bit_pos = offset / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = offset % SizeInBits(bitmap_entry_t);
        entries_offset_indexible_ = entries_ + offset;
    }
    */

    /* return the address of the allocated memory space for the bitmap */
	inline char *data() {
		return (char *)entries_;
	}

    /* return the size of the allocated memory space for the bitmap */
	inline std::size_t container_size() const {
		return array_size * sizeof(bitmap_entry_t);
	}

    /* return the number of entries */
	Idx_t num_entries() const {
		return num_entries_;
	}
	
    /* return the (the number of entries in this bitmap + 63) / 64 */
    std::size_t length() {
		return entries_mmap_.length();
	}

    /* Swap this bitmap with the given bitmap */
    void SwapBitMap(MemoryMappedBitMap<Idx_t>& another) {
        ALWAYS_ASSERT (num_entries_ == another.num_entries_);
		bitmap_entry_t* tmp = entries_;
        this->entries_ = another.entries_;
        another.entries_ = tmp;
    }

    /* Do allreduce for the bitmap */
    void Allreduce() {
        MPI_Allreduce(MPI_IN_PLACE, (char*) entries_, sizeof(bitmap_entry_t) * array_size, MPI_CHAR, MPI_BOR, MPI_COMM_WORLD);
    }

  protected:
	bitmap_entry_t *entries_; /* a pointer to the allocated memory space for the bitmap */
    MemoryMappedArray<bitmap_entry_t> entries_mmap_; 
	Idx_t num_entries_; /* the number of entries in the bitmap */
	Idx_t array_size; /* (the size of allocated memory space + 63) / 64 */
    bitmap_entry_t one = 0x1;
};

#endif
