#pragma once
#ifndef BITMAP_H
#define BITMAP_H

/*
 * Design of the BitMap
 * 
 * This class implements various bitmap operations required by various bitmaps
 * (e.g. voi: vertices of interest) used in the system. Basically, operations 
 * to set and clear bits are provided (atomic set/clear operations are also 
 * provided). Additionally, operations such as intersection and union, which 
 * are operations between bitmaps, are also provided. 
 *
 * The system uses bitmaps the most for vertices (and pages). The logic mainly 
 * supported by Bitmap is to process the function for each bit set in the bitmap 
 * (e.g. vertices to be processed) for the bitmap and function given as parameters. 
 * Functions such as InvokeIfMarked and InvokeIfMarkedOnIntersection support this 
 * logic.
 *
 */

#include <algorithm>
#include <atomic>
#include <list>
#include <vector>
#include <stdexcept>
#include <cmath>
#include <cstdlib>
#include <bitset>
#include <immintrin.h>

#include "analytics/core/TypeDef.hpp"
#include "analytics/util/util.hpp"

#define SizeInBits(X) ((8*sizeof(X)))


template <typename IdxTmp_t>
class BitMap {
  public:

    typedef int64_t Idx_t;
	typedef int64_t bitmap_entry_t;

    /*
    * Construct the bitmap
    */
	BitMap() : entries_ (NULL), num_entries_(0) {
		array_size = 0;
        for (int64_t idx = 0; idx < MAX_NUM_CPU_CORES; idx++) {
            counts[idx].idx = 0;
        }
    }

    /*
    * Deconstruct the bitmap
    */
	~BitMap() {
        Close();
	}

    /*
    * Free the memory space
    */
    void Close() {
        if (entries_ != NULL) {
            NumaHelper::free_numa_memory(entries_, sizeof(bitmap_entry_t) * array_size);
            entries_ = NULL;
		    array_size = 0;
            num_entries_ = 0;
        }
    }

    /*
    * Initialize the bitmap: 1) calculate the required size 2) allocate the memory space
    */
    void Init(Idx_t num_entries) {
        Close();
        ALWAYS_ASSERT (num_entries > 0);
        ALWAYS_ASSERT ( (sizeof(bitmap_entry_t) * ((num_entries + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t))) >= (num_entries + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t));
        /* Calculate the required size */
        num_entries_ = num_entries;
        array_size = ((num_entries + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t));
        /* Allocate the memory space */
        entries_ = (bitmap_entry_t *) NumaHelper::alloc_numa_memory(sizeof(bitmap_entry_t) * array_size);
        /* Initialize the memory space space */
        memset(entries_, 0, array_size * sizeof(bitmap_entry_t));
    }

    /*
    * Chech wheter the bitmap is initialized
    */
    bool IsInitialized() {
        return (num_entries_ > 0 && entries_ != nullptr);
    }

    /*
    * Return the allocated memory space
    */
    int64_t* GetBitMap() {
        return entries_;
    }
	
    // assuming that l == o
    /*
    * Union the parts of given bitmaps
    *
    * Input bitmaps (o, rs)
    *                       [ from ... to )
    *    o[k] : [ 1, 0, ...,| 0, 0, 1, 0, | ... ]
    *   rs[0] : [ 0, 1, ...,| 0, 1, 0, 0, | ... ]
    *   rs[1] : [ 1, 1, ...,| 0, 1, 0, 1, | ... ]
    * 
    * Output bitmap: (o)
    *    o[k] : [ 1, 0, ...,| 0, 1, 1, 1, | ... ]   
    *
    */
    static void Union(BitMap<Idx_t>& l, std::vector<BitMap<Idx_t>*>& rs, BitMap<Idx_t>& o, Idx_t from, Idx_t to, bool single_thread) {
		ALWAYS_ASSERT (l.num_entries() == o.num_entries());
       
        Range<Idx_t> idx_range(from, to);

        if (idx_range.length() <= 63) {
            /* Union small bitmaps */
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

            if (single_thread) {
                /* Process the union operation for the bits in C */
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
    * Execute a complex bit operation the bitmaps
    */
    static void ComplexOperation1(BitMap<Idx_t>& A, BitMap<Idx_t>& B, BitMap<Idx_t>& C, BitMap<Idx_t>& X, BitMap<Idx_t>& Y, BitMap<Idx_t>& Z) {
		ALWAYS_ASSERT (A.num_entries() == B.num_entries());
		ALWAYS_ASSERT (A.num_entries() == C.num_entries());
		ALWAYS_ASSERT (A.num_entries() == X.num_entries());
#pragma omp parallel for
        for (Idx_t i = 0; i < A.array_size; i++) {
            X.entries_[i] = A.entries_[i] & (~B.entries_[i] | (C.entries_[i] & B.entries_[i]));
            Y.entries_[i] = B.entries_[i] & (~A.entries_[i] | (C.entries_[i] & A.entries_[i]));
            Z.entries_[i] = X.entries_[i] | Y.entries_[i];
        }
    }

    /*
    * Union the two bitmaps l and r and write the result to the output bitmap o
    *
    * Option 1: l OR r
    * Option 2: l OR ~r
    * Option 3: ~l OR r
    * Option 4: ~l OR ~r
    */
    static void Union(BitMap<Idx_t>& l, BitMap<Idx_t>& r, BitMap<Idx_t>& o, bool inv_l = false, bool inv_r = false) {
		ALWAYS_ASSERT (l.num_entries() == o.num_entries());
		ALWAYS_ASSERT (l.num_entries() == r.num_entries());

        if (inv_l) {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = ~l.entries_[i] | ~r.entries_[i];
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = ~l.entries_[i] | r.entries_[i];
                }
            }
        } else {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = l.entries_[i] | ~r.entries_[i];
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = l.entries_[i] | r.entries_[i];
                }
            }
        }
	}

    /*
    * Check whether the result of the intersection of two bitmaps is empty 
    */
	static bool IsIntersectionEmpty(BitMap<Idx_t>& l, BitMap<Idx_t>& r) {
        //ALWAYS_ASSERT (l.num_entries() == o.num_entries());
        ALWAYS_ASSERT (l.num_entries() == r.num_entries());
        for (Idx_t i = 0; i < r.array_size; i++) {
            bitmap_entry_t tmp = l.entries_[i] & r.entries_[i];
		    if (tmp != 0) return false;
        }
        return true;
    }
    
    /*
    * Intersect the two bitmaps and write the result to the output bitmap if there is a change
    *
    * Options: l AND r, l AND ~r,  ~l AND r, ~l AND ~r
    */
    static void IntersectionWriteOnlyWhenChanges(BitMap<Idx_t>& l, BitMap<Idx_t>& r, BitMap<Idx_t>& o, bool inv_l = false, bool inv_r = false) {
        ALWAYS_ASSERT (l.num_entries() == o.num_entries());
        ALWAYS_ASSERT (l.num_entries() == r.num_entries());

        if (inv_l) {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    bitmap_entry_t tmp = ~l.entries_[i] & ~r.entries_[i];
                    if (tmp != o.entries_[i]) o.entries_[i] = tmp;
                    //o.entries_[i] = ~l.entries_[i] & ~r.entries_[i];
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    bitmap_entry_t tmp = ~l.entries_[i] & r.entries_[i];
                    if (tmp != o.entries_[i]) o.entries_[i] = tmp;
                    //o.entries_[i] = ~l.entries_[i] & r.entries_[i];
                }
            }
        } else {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    bitmap_entry_t tmp = l.entries_[i] & ~r.entries_[i];
                    if (tmp != o.entries_[i]) o.entries_[i] = tmp;
                    //o.entries_[i] = l.entries_[i] & ~r.entries_[i];
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    bitmap_entry_t tmp = l.entries_[i] & r.entries_[i];
                    if (tmp != o.entries_[i]) o.entries_[i] = tmp;
                    //o.entries_[i] = l.entries_[i] & r.entries_[i];
                }
            }
        }
    }
    /*
    * Intersect the two bitmaps and execute the given function for the result 
    *
    * It executes the 64 bit AND bit operation first and 
    *
    * Options: l AND r, l AND ~r,  ~l AND r, ~l AND ~r
    */    
    static void InvokeIfMarkedOnIntersection(const std::function<void(IdxTmp_t)>& f, BitMap<Idx_t>& l, BitMap<Idx_t>& r, bool inv_l = false, bool inv_r = false) {
        ALWAYS_ASSERT (l.num_entries() == r.num_entries());
        bitmap_entry_t one = 0x01;
        if (inv_l) {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < l.array_size; i++) {
                    bitmap_entry_t tmp = ~l.entries_[i] & ~r.entries_[i];
                    if (tmp == 0) continue;
                    for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                        bool val = (bool) ((tmp >> j) & one);
                        int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                        if (val && idx < l.num_entries_) { 
                            f(idx);
                        }
                    }   
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < l.array_size; i++) {
                    bitmap_entry_t tmp = ~l.entries_[i] & r.entries_[i];
                    if (tmp == 0) continue;
                    for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                        bool val = (bool) ((tmp >> j) & one);
                        int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                        if (val && idx < l.num_entries_) { 
                            f(idx);
                        }
                    }
                }
            }
        } else {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < l.array_size; i++) {
                    bitmap_entry_t tmp = l.entries_[i] & ~r.entries_[i];
                    if (tmp == 0) continue;
                    for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                        bool val = (bool) ((tmp >> j) & one);
                        int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                        if (val && idx < l.num_entries_) { 
                            f(idx);
                        }
                    }
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < l.array_size; i++) {
                    bitmap_entry_t tmp = l.entries_[i] & r.entries_[i];
                    if (tmp == 0) continue;
                    for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                        bool val = (bool) ((tmp >> j) & one);
                        int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                        if (val && idx < l.num_entries_) { 
                            f(idx);
                        }
                    }
                }
            }
        }

    }

    /*
    * Intersect the two bitmaps and write the result to the output bitmap
    *
    * Options: l AND r, l AND ~r,  ~l AND r, ~l AND ~r
    */
    static void Intersection(BitMap<Idx_t>& l, BitMap<Idx_t>& r, BitMap<Idx_t>& o, bool inv_l = false, bool inv_r = false) {
        ALWAYS_ASSERT (l.num_entries() == o.num_entries());
        ALWAYS_ASSERT (l.num_entries() == r.num_entries());

        if (inv_l) {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = ~l.entries_[i] & ~r.entries_[i];
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = ~l.entries_[i] & r.entries_[i];
                }
            }
        } else {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = l.entries_[i] & ~r.entries_[i];
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.array_size; i++) {
                    o.entries_[i] = l.entries_[i] & r.entries_[i];
                }
            }
        }
    }

    /*
    * Swap the two bitmaps
    */
    static void SwapBitMap(BitMap<Idx_t>& l, BitMap<Idx_t>& r) {
        l.SwapBitMap(r);
    }

    /*
    * Copy the bits from the given bitmap
    */
	void CopyFrom(BitMap<Idx_t>& from) {
		ALWAYS_ASSERT (array_size == from.array_size);
        ALWAYS_ASSERT(entries_ != NULL && from.entries_ != NULL);
		#pragma omp parallel for
		for (Idx_t i = 0; i < array_size; i++) {
			entries_[i] = from.entries_[i];
		}
	}

    /*
    * Sum the bits in the given range [from, to] using multi threads
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
    * Sum the all bits using multi threads
    */
	int64_t GetTotal() {
        int64_t total_cnts = 0;
		#pragma omp parallel
		{
			int64_t cnt = 0;
			#pragma omp for
			for (Idx_t i = 0; i <= array_size; i++) {
				cnt += _mm_popcnt_u64(entries_[i]); //_popcnt64(entries_[i]); //_popcnt64(entries_[i]);
			}
			std::atomic_fetch_add((std::atomic<int64_t>*) &total_cnts, cnt);
		}
		return total_cnts;
	}

    /*
    * Sum the all bits using a single thread
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
        for (size_t i = num_entries_; i < array_size * SizeInBits(bitmap_entry_t); i++) {
            ClearUnsafe(i);
        }
	}
	
    /*
    * Print the bitmap
    */
    inline void Dump() {
        for (Idx_t i = 0; i < array_size; i++) {
            bitmap_entry_t tmp = entries_[i];
            for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                bool val = (bool) ((tmp >> j) & one);
                int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                if (idx < num_entries_) {
                    if (val) fprintf(stdout, "%ld true\n", idx);
                    else fprintf(stdout, "%ld false\n", idx);
                }
            }   
        }
	}
    
    /*
    * Print the bit at the given location
    */
    inline void Dump(int64_t target_idx) {
        for (Idx_t i = 0; i < array_size; i++) {
            bitmap_entry_t tmp = entries_[i];
            for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                bool val = (bool) ((tmp >> j) & one);
                int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                if (idx == target_idx && idx < num_entries_) {
                    if (val) fprintf(stdout, "%ld true\n", idx);
                    else fprintf(stdout, "%ld false\n", idx);
                }
            }   
        }
	}

    /* Set the all bits to zero */
	void ClearAll(bool single_thread = false) {
		ALWAYS_ASSERT (entries_ != NULL);
        memset(entries_, 0, array_size * sizeof(bitmap_entry_t));
	}

    /* Check whether the bit at the given location is 1 */
    inline bool operator[](int64_t idx) {
        ALWAYS_ASSERT(0 <= idx);
        ALWAYS_ASSERT((idx) / SizeInBits(bitmap_entry_t) < array_size);
        Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
        Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
        bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
        return (bool)val;
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
	inline void ClearUnsafe(Idx_t idx) {
		ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t and_val = ~(one << bit_loc);
		entries_[bit_pos] &= and_val;
	}

    /* Set the bit at the given location to 0 */
    inline void Clear(Idx_t idx) {
		ALWAYS_ASSERT(0 <= idx && (idx) / SizeInBits(bitmap_entry_t) < array_size);
        ALWAYS_ASSERT(idx < num_entries_);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t and_val = ~(one << bit_loc);
		entries_[bit_pos] &= and_val;
	}

    /* Get the value of the bit at the given location */
	inline bool Get(Idx_t idx) const {
		ALWAYS_ASSERT(0 <= idx);
        ALWAYS_ASSERT(idx < num_entries_);
		ALWAYS_ASSERT((idx) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t bit_pos = idx / SizeInBits(bitmap_entry_t);
		Idx_t bit_loc = idx % SizeInBits(bitmap_entry_t);
		bitmap_entry_t val = (entries_[bit_pos] >> bit_loc) & one;
		return (bool)val;
	}
        
    /* Get the bit at the given location atomically */
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
        /* It set the bit to 1 if the bit is 0, to reduce the number of memory writes */
		if (!val) {
			bitmap_entry_t or_val = one << bit_loc;
			entries_[bit_pos] |= or_val;
		}
		return val;
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
        /* It set the bit to 1 atomically if the bit is 0, to reduce the number of atomic updates*/
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

            for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                /* Set bits to 0 in part C */
                Clear(i);
            }
        }
	}
    
    /* 
    * Check whether there is at least one bit with a value of 1 in the bitmap
    */
    inline bool IsSet() const {
        return IsSetInRange(0, array_size * SizeInBits(bitmap_entry_t) - 1);
    }

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
            upto = std::min(upto, to);
            if (GetSetBits(from_idx, from % SizeInBits(bitmap_entry_t), upto % SizeInBits(bitmap_entry_t)) != 0) {
                return true;
            }
            from_idx += 1;
        }

        ALWAYS_ASSERT(from_idx <= from);
        ALWAYS_ASSERT(to_idx <= to);
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
    * Find the location of the last bit with a value of 1
    */
    inline Idx_t FindLastMarkedEntry() const {
        return FindLastMarkedEntry(0, num_entries_ - 1);
    }

    /*
    * Find the location of the last bit with a value of 1 in the given range
    */
	inline Idx_t FindLastMarkedEntry(Idx_t from, Idx_t to) const {
		ALWAYS_ASSERT(0 <= from && (from) / SizeInBits(bitmap_entry_t) < array_size);
		ALWAYS_ASSERT(0 <= to && (to) / SizeInBits(bitmap_entry_t) < array_size);
		Idx_t from_idx = from / SizeInBits(bitmap_entry_t);
		Idx_t to_idx = to / SizeInBits(bitmap_entry_t);
		Idx_t found = -1;

        /* It assumes that the number of bits is a multiple of 64 */

        /* Find the bit with a value of 1 using 64 bit operations first */
		for (Idx_t idx = to_idx; idx >= from_idx; idx--) {
			if (entries_[idx] != 0) {
				found = SizeInBits(bitmap_entry_t) * idx;
				break;
			}
		}
		if (found == -1) return -1;
		to = (to > found) ? to : found;
        /* Find the bit with a value of 1 */
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
    void InvokeIfNotMarked(const std::function<void(IdxTmp_t)>& f) {
        Range<Idx_t> total_range(0, num_entries_ - 1);
        InvokeIfNotMarked(f, total_range);
    }

    /*
    * For each bit in the given range, invoke the function f(location of the bit) if the bit is 0
    */
    void InvokeIfNotMarked(const std::function<void(IdxTmp_t)>& f, Range<IdxTmp_t> idx_range) {
        if (idx_range.length() <= 63) {
            /* Invoke the function for the bits with a value of 0 in the short range */
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
#pragma omp parallel for schedule (dynamic, 16*64)
                for (int64_t i = bytefrom; i < byteto; i++) {
                    /* Check whether there is a bit with a value of 1 in 64 bits first */
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
    void InvokeIfMarked(const std::function<bool(IdxTmp_t)>& f, Range<IdxTmp_t> idx_range, bool single_thread = false, int64_t num_threads = UserArguments::NUM_THREADS) {
        ALWAYS_ASSERT (num_threads >= 1 && num_threads <= UserArguments::NUM_TOTAL_CPU_CORES);
        if (idx_range.length() <= 63) {
            /* Invoke the function for the bits with a value of 1 in the short range */
            for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                if (Get(i)) 
                    if (!f(i)) return;
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
                        if (Get(i)) 
                            if (!f(i)) return;
                    }
                } else {
//#pragma omp parallel for schedule (dynamic, 16*64) num_threads(num_threads)
                    for (Idx_t i = from; i <= upto; i++) {
                        if (Get(i))
                            if (!f(i)) return;
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
                                if (!f(i * SizeInBits(bitmap_entry_t) + j)) return;
                            }
                        }
                    }
                } else {
//#pragma omp parallel for schedule (dynamic, 16*64) num_threads(num_threads)
                    for (int64_t i = bytefrom; i < byteto; i++) {
                        if (entries_[i] == 0) continue;
                        for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                            bool val = (bool) ((entries_[i] >> j) & one);
                            if (val) { 
                                if (!f(i * SizeInBits(bitmap_entry_t) + j)) return;
                            }
                        }
                    }
                }
            }
             /* Invoke the function f for the bits with a value of 1 in the part C */
            if (single_thread) {
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    if (Get(i)) 
                        if (!f(i)) return;
                }
            } else {
//#pragma omp parallel for schedule (dynamic, 16*64) num_threads(num_threads)
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    if (Get(i)) 
                        if (!f(i)) return;
                }
            }

        }
    }

    /*
    * For each bit in the given range, invoke the function f(location of the bit) if the bit is 1
    */
    void InvokeIfMarked(const std::function<void(IdxTmp_t)>& f, Range<IdxTmp_t> idx_range, bool single_thread = false, int64_t num_threads = UserArguments::NUM_THREADS) {
        ALWAYS_ASSERT (num_threads >= 1 && num_threads <= UserArguments::NUM_TOTAL_CPU_CORES);
        if (idx_range.length() <= 63) {
            /* Invoke the function for the bits with a value of 1 in the short range */
            for (Idx_t i = idx_range.GetBegin(); i <= idx_range.GetEnd(); i++) {
                if (Get(i)) f(i);
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
#pragma omp parallel for schedule (dynamic, 16*64) num_threads(num_threads)
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
#pragma omp parallel for schedule (dynamic, 16*64) num_threads(num_threads)
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
#pragma omp parallel for schedule (dynamic, 16*64) num_threads(num_threads)
                for (Idx_t i = byteto * SizeInBits(bitmap_entry_t); i <= to; i++) {
                    if (Get(i)) f(i);
                }
            }

        }
    }

    /*
    * 1) Execute the given function before_f() if it exists
    * 2) For each bit in the bitmap, invoke the function f(location of the bit) if the bit is 1
    * 3) Execute the given function after_f() if it exists
    */
    void InvokeIfMarked(const std::function<void(IdxTmp_t)>& f, bool single_thread = false, int64_t num_threads = UserArguments::NUM_THREADS, const std::function<void()>& after_f = {}, const std::function<void()>& before_f = {}) {
        ALWAYS_ASSERT (num_threads >= 1 && num_threads <= UserArguments::NUM_TOTAL_CPU_CORES);
        ALWAYS_ASSERT(entries_ != NULL);
        if (single_thread) {
            if (before_f) {
                before_f();
            }
            /* For each bit in the bitmap, invoke the function f(location of the bit) if the bit is 1 */
            for (int64_t i = 0; i < array_size; i++) {
                if (entries_[i] == 0) continue;
                for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                    bool val = (bool) ((entries_[i] >> j) & one);
                    if (!val) continue;
                    int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                    if (idx < this->num_entries_) {
                        f(idx);
                    }
                }
            }
            if (after_f) {
                after_f();
            }
        } else {
#pragma omp parallel num_threads(num_threads)
            {
                if (before_f) {
                    before_f();
                }
                /* For each bit in the bitmap, invoke the function f(location of the bit) if the bit is 1 */
#pragma omp for schedule (dynamic, 16*64)
                for (int64_t i = 0; i < array_size; i++) {
                    if (entries_[i] == 0) continue;
                    for (int64_t j = 0; j < SizeInBits(bitmap_entry_t); j++) {
                        bool val = (bool) ((entries_[i] >> j) & one);
                        if (!val) continue;
                        int64_t idx = i * SizeInBits(bitmap_entry_t) + j;
                        if (idx < this->num_entries_) {
                            f(idx);
                        }
                    }
                }
                
                if (after_f) {
                    after_f();
                }
            }
        }
    }

    /* return the address of the allocated memory space for the bitmap */
	inline char *data() {
		return (char *)entries_;
	}

    /* return the size of the allocated memory space for the bitmap */
	inline std::size_t container_size() const {
		return array_size * sizeof(bitmap_entry_t);
	}

    /* return the required memory size of the bitmap for the given number of entries  */
	static size_t compute_container_size(int64_t num_entries_) {
        int64_t array_size_ = ((num_entries_ + SizeInBits(bitmap_entry_t) - 1) / SizeInBits(bitmap_entry_t));
		return array_size_ * sizeof(bitmap_entry_t);
	}

    /* return the number of entries */
	Idx_t num_entries() const {
		return num_entries_;
	}

    /* Construct a bitmap from the given bitmap */
    void SwapBitMap(BitMap<Idx_t>& another) {
        ALWAYS_ASSERT (num_entries_ == another.num_entries_);
		bitmap_entry_t* tmp = entries_;
        this->entries_ = another.entries_;
        another.entries_ = tmp;
    }

    /* Do allreduce for the bitmap */
    void Allreduce() {
        MPI_Allreduce(MPI_IN_PLACE, (char*) entries_, sizeof(bitmap_entry_t) * array_size, MPI_CHAR, MPI_BOR, MPI_COMM_WORLD);
    }

  public:
	bitmap_entry_t *entries_; /* a pointer to the allocated memory space for the bitmap */
	Idx_t num_entries_; /* the number of entries in the bitmap */
	Idx_t array_size; /* the size of allocated memory space / 64 */
    bitmap_entry_t one = 0x1; /* the value one */
    std::list<bitmap_entry_t> entries_list_;  /* @tlsee */
    bool is_list_flag_ = false; /* @tslee */
    bool remain_list_ = false; /* @tslee */

    PaddedIdx counts[MAX_NUM_CPU_CORES];
};

#endif
