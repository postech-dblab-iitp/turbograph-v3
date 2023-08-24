#pragma once
#ifndef TWOLEVELBITMAP_H
#define TWOLEVELBITMAP_H

/*
 * Design of the TwoLevelBitMap
 * 
 * This class has 1 large BitMap(having the same size as the size given as input, LV-1) 
 * and 1 small BitMap(LV-2) as member variables. A bit in the LV-2 bitmap represents 
 * whether there are any bits set in the LV-1 bitmap in units of 256 bits (defined as NN).
 *
 * This data structure is designed to efficiently handle the case where the bits set 
 * in the bitmap are sparse. The cost of read the entire LV-1 bitmap is quite high, 
 * but the LV-2 bitmap allows you to quickly skip areas that don't need to be checked.
 *
 * There may be cases of false positives. When performing a set operation, set is 
 * also performed on the corresponding bit of LV-2. However, for cost reasons, when 
 * performing a clear operation, we do not check whether other 255 bits in the
 * corresponding same area of LV-1 are cleared.
 *
 * When the number of set bits is extremely small, the same API is supported using 
 * the list data structure internally.
 */
#include <immintrin.h>
#include "parallel/BitMap.hpp"

#define TwoLevelSizeInBits(X) ((NN * 8*sizeof(X)))
#define NN ((4*64))

//#define TEST_IF_MARKED_AT_LEVEL_TWO

template <typename IdxTmp_t>
class TwoLevelBitMap : public BitMap<IdxTmp_t> {
  public:

    typedef int64_t Idx_t;
	typedef int64_t bitmap_entry_t;

	TwoLevelBitMap() : BitMap<IdxTmp_t>() {
    }

	~TwoLevelBitMap() {
    }

    inline BitMap<IdxTmp_t>& GetBase() { return *(BitMap<IdxTmp_t>*) this; }
    
    void Init(Idx_t num_entries) {
        BitMap<IdxTmp_t>::Init(num_entries);
        level_two_bitmap.Init((num_entries + NN - 1) / NN);
    }
    
    void ClearAllLv2(bool single_thread = false) {
        level_two_bitmap.ClearAll(single_thread);
    }

    void ClearAll(bool single_thread = false) {
		D_ASSERT (BitMap<IdxTmp_t>::entries_ != NULL);
        if (this->is_list_flag_) {
            D_ASSERT(!this->remain_list_);
            this->entries_list_.clear();
            return;
        }
        if (this->remain_list_) {
            auto iter_ = this->entries_list_.begin();
            for (;iter_ != this->entries_list_.end(); iter_++) {
                TwoLevelBitMap<IdxTmp_t>::Clear(*iter_);
            }
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            level_two_bitmap.ClearAll();
#endif
            this->entries_list_.clear();
            this->remain_list_ = false;
            return;
        }
        if (single_thread) {
            for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
                for (Idx_t j = NN * i; j < std::min(BitMap<IdxTmp_t>::array_size, NN * (i+1)); j++) {
                    if (BitMap<IdxTmp_t>::entries_[j] == 0) continue;
                    BitMap<IdxTmp_t>::entries_[j] = 0;
                }
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                level_two_bitmap.entries_[i] = 0;
#endif
            }
        } else {
#pragma omp parallel for
            for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
                for (Idx_t j = NN * i; j < std::min(BitMap<IdxTmp_t>::array_size, NN * (i+1)); j++) {
                    if (BitMap<IdxTmp_t>::entries_[j] == 0) continue;
                    BitMap<IdxTmp_t>::entries_[j] = 0;
                }
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                level_two_bitmap.entries_[i] = 0;
#endif
            }
        }
    }

    void ClearList() {
        this->entries_list_.clear();
        this->is_list_flag_ = false;
        this->remain_list_ = false;
    }

    inline void SortAndRemoveDuplicatesInList() {
        if (!this->is_list_flag_) return;
        this->entries_list_.sort();
        this->entries_list_.unique();
    }
    
    inline void Set(Idx_t idx) {
        BitMap<IdxTmp_t>::Set(idx);
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        level_two_bitmap.Set(idx / NN);
#endif
    }
    inline bool Get(Idx_t idx) const {
        bool marked = BitMap<IdxTmp_t>::Get(idx);
#ifdef TEST_IF_MARKED_AT_LEVEL_TWO
        if (marked) {
            D_ASSERT(level_two_bitmap.Get(idx / NN));
        }
#endif
        return marked;
	}
    // must be performed by single thread
    inline void PushIntoList(Idx_t idx) {
        this->entries_list_.push_back(idx);
    }

	inline bool Set_Atomic(Idx_t idx) {
        //int i = omp_get_thread_num();
        //per_thread_idx[i].idx = idx;
        bool res = BitMap<IdxTmp_t>::Set_Atomic(idx);
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        level_two_bitmap.Set_Atomic(idx / NN);
#endif
        return res;
	}
	inline bool Get_Atomic(Idx_t idx) const {
	    bool marked = BitMap<IdxTmp_t>::Get_Atomic(idx);
#ifdef TEST_IF_MARKED_AT_LEVEL_TWO
        if (marked) {
            D_ASSERT(level_two_bitmap.Get_Atomic(idx / NN));
        }
#endif
        return marked;
	}
	inline bool Clear_Atomic(Idx_t idx) {
        bool res = BitMap<IdxTmp_t>::Clear_Atomic(idx);
        return res;
	}

    inline void Clear_IfSet(Idx_t idx) {
        if (BitMap<IdxTmp_t>::Get(idx)) { 
            BitMap<IdxTmp_t>::Clear(idx);
        }
    }
    inline bool Set_IfNotSet(Idx_t idx) {
        bool res = BitMap<IdxTmp_t>::Set_IfNotSet(idx);
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        if (res) {
            level_two_bitmap.Set(idx / NN);
        }
#endif
        return res;
    }
    inline bool Set_Atomic_IfNotSet(Idx_t idx) {
        bool res = BitMap<IdxTmp_t>::Set_Atomic_IfNotSet(idx);
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        if (res) {
            level_two_bitmap.Set_Atomic_IfNotSet(idx / NN);
        }
#endif
        return res;
    }

    bool IsSet() const {
        for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            if (level_two_bitmap.entries_[i] == 0) continue;
#endif
            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
            for (Idx_t j = NN * i; j < std::min(BitMap<IdxTmp_t>::array_size, NN * (i+1)); j++) {
                if (BitMap<IdxTmp_t>::entries_[j] != 0) return true;
            }
        }
	    return false;
    }

    inline bool IsSetInRange(Idx_t from, Idx_t to) const {
        Range<IdxTmp_t> idx_range(from, to);
        Range<Idx_t> level_two_range(idx_range.GetBegin() / SizeInBits(bitmap_entry_t) / NN, idx_range.GetEnd() / SizeInBits(bitmap_entry_t) / NN);
        
        if (idx_range.length() > 32 * TwoLevelSizeInBits(bitmap_entry_t)) {

            if (from % TwoLevelSizeInBits(bitmap_entry_t) != 0) {

            }
            
            for (Idx_t i = level_two_range.GetBegin(); i <= level_two_range.GetEnd(); i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                
                
                Idx_t j_begin =  (NN * i > from / SizeInBits(bitmap_entry_t)) ? (NN * i) : (from / SizeInBits(bitmap_entry_t)); 
                Idx_t j_end =  (NN * (i+1) < to / SizeInBits(bitmap_entry_t)) ? (NN * (i + 1)) : (to / SizeInBits(bitmap_entry_t)); 

                for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                    if (this->entries_[j] == 0) continue;

                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((this->entries_[j] >> k) & this->one);
                        if (!val) continue;
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (idx_range.contains(idx)) {
                            return true;
                        }
                    }
                }
            }
            
            
            return false;
        } else {
            return BitMap<IdxTmp_t>::IsSetInRange(from, to);
        }
    }
    
    inline bool IsSetInRangeUnSafe(Idx_t from, Idx_t to) const {
        Range<IdxTmp_t> idx_range(from, to);
        Range<Idx_t> level_two_range(idx_range.GetBegin() / SizeInBits(bitmap_entry_t) / NN, idx_range.GetEnd() / SizeInBits(bitmap_entry_t) / NN);
        
        if (idx_range.length() > 32 * TwoLevelSizeInBits(bitmap_entry_t)) {

            if (from % TwoLevelSizeInBits(bitmap_entry_t) != 0) {

            }
            
            for (Idx_t i = level_two_range.GetBegin(); i <= level_two_range.GetEnd(); i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
                
                Idx_t j_begin =  (NN * i > from / SizeInBits(bitmap_entry_t)) ? (NN * i) : (from / SizeInBits(bitmap_entry_t)); 
                Idx_t j_end =  (NN * (i+1) < to / SizeInBits(bitmap_entry_t)) ? (NN * (i + 1)) : (to / SizeInBits(bitmap_entry_t)); 

                for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                    if (this->entries_[j] == 0) continue;

                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((this->entries_[j] >> k) & this->one);
                        if (!val) continue;
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (idx_range.contains(idx)) {
                            return true;
                        }
                    }
                }
                D_ASSERT(false);
            }
            return false;
        } else {
            return BitMap<IdxTmp_t>::IsSetInRange(from, to);
        }
    }

    void InvokeIfNotMarked(const std::function<void(IdxTmp_t)>& f) {
        bitmap_entry_t mask = ~((bitmap_entry_t) 0);
#pragma omp parallel for
        for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

            for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                if (mask & this->entries_[j] == mask) continue;

                for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                    bool val = (bool) ((this->entries_[j] >> k) & this->one);
                    if (val) continue;
                    int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                    if (idx < this->num_entries_) {
                        f(idx);
                    }
                }
            }
        }
    }

    void InvokeIfMarked(const std::function<void(IdxTmp_t)>& f, Range<IdxTmp_t> idx_range, bool single_thread = false, int64_t num_threads = UserArguments::NUM_THREADS, const std::function<void()>& after_f = {}, const std::function<void()>& before_f = {}) {
        D_ASSERT (num_threads >= 1 && num_threads <= UserArguments::NUM_TOTAL_CPU_CORES);
        //D_ASSERT (entries_ != NULL);
        Range<Idx_t> level_two_range(idx_range.GetBegin() / SizeInBits(bitmap_entry_t) / NN, idx_range.GetEnd() / SizeInBits(bitmap_entry_t) / NN);

        if (single_thread) {
            if (before_f) {
                before_f();
            }
            for (Idx_t i = level_two_range.GetBegin(); i <= level_two_range.GetEnd(); i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                    if (this->entries_[j] == 0) continue;

                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((this->entries_[j] >> k) & this->one);
                        if (!val) continue;
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (idx_range.contains(idx)) {
                            f(idx);
                        }
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
#pragma omp for
//#pragma omp for schedule (dynamic, 64)
                for (Idx_t i = level_two_range.GetBegin(); i <= level_two_range.GetEnd(); i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                        if (this->entries_[j] == 0) continue;

                        for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                            bool val = (bool) ((this->entries_[j] >> k) & this->one);
                            if (!val) continue;
                            int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                            if (idx_range.contains(idx)) {
                                f(idx);
                            }
                        }
                    }
                }
                if (after_f) {
                    after_f();
                }
            }
        }
    }

    void InvokeIfMarked(const std::function<void(IdxTmp_t)>& f, bool single_thread = false, int64_t num_threads = UserArguments::NUM_THREADS, const std::function<void()>& after_f = {}, const std::function<void()>& before_f = {}) {
        D_ASSERT (this->entries_ != NULL);
        D_ASSERT (num_threads >= 1 && num_threads <= UserArguments::NUM_TOTAL_CPU_CORES);
        if (single_thread) {
            if (before_f) {
                before_f();
            }
            for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                    if (this->entries_[j] == 0) continue;

                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((this->entries_[j] >> k) & this->one);
                        if (!val) continue;
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (idx < this->num_entries_) {
                            f(idx);
                        }
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

//#pragma omp for schedule (dynamic, 64)
#pragma omp for
                for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                        if (this->entries_[j] == 0) continue;

                        for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                            bool val = (bool) ((this->entries_[j] >> k) & this->one);
                            if (!val) continue;
                            int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                            if (idx < this->num_entries_) {
                                f(idx);
                            }
                        }
                    }
                }
                
                if (after_f) {
                    after_f();
                }
            }
        }
    }

    void InvokeIfMarkedOnList(const std::function<void(IdxTmp_t)>& f) {
        D_ASSERT(this->is_list_flag_ || this->remain_list_);
        auto iter_ = this->entries_list_.begin();
        for (;iter_ != this->entries_list_.end(); iter_++) {
            f(*iter_);
        }
    }

    static void InvokeIfMarkedOnIntersection(const std::function<void(IdxTmp_t)>& f, TwoLevelBitMap<IdxTmp_t>& l, TwoLevelBitMap<IdxTmp_t>& r, bool inv_l = false, bool inv_r = false) {
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        bitmap_entry_t one = 0x01;

        if (inv_l) {
            if (inv_r) {
                D_ASSERT(false);
            } else {
//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
                for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (r.level_two_bitmap.entries_[i] == 0) continue;
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                        bitmap_entry_t tmp = ~l.entries_[j] & r.entries_[j];
                        if (tmp == 0) continue;
                        for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                            bool val = (bool) ((tmp >> k) & one);
                            int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                            if (val && idx < l.num_entries_) {
                                f(idx);
                            }
                        }
                    }
                }
            }
        } else {
            if (inv_r) {
                D_ASSERT(false);
            } else {
//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
                for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (r.level_two_bitmap.entries_[i] == 0 || l.level_two_bitmap.entries_[i] == 0) continue;
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                        bitmap_entry_t tmp = l.entries_[j] & r.entries_[j];
                        for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                            bool val = (bool) ((tmp >> k) & one);
                            int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                            if (val && idx < l.num_entries_) {
                                f(idx);
                            }
                        }
                    }
                }
            }
        }

    }

    static int64_t GetTotalOfUnion(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, TwoLevelBitMap<Idx_t>& rr, int64_t num_threads = UserArguments::NUM_THREADS) {
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (rr.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        D_ASSERT (l.num_entries() == rr.num_entries());
        bitmap_entry_t one = 0x01;
        int64_t total_cnts = 0;

//#pragma omp parallel num_threads(num_threads)
        {
            int64_t cnt = 0;
//#pragma omp for schedule (dynamic, 64)
            for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (l.level_two_bitmap.entries_[i] == 0 && r.level_two_bitmap.entries_[i] == 0 && rr.level_two_bitmap.entries_[i] == 0) continue;
#endif

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = l.entries_[j] | r.entries_[j] | rr.entries_[j];
                    cnt += _mm_popcnt_u64(tmp); //_popcnt64(tmp);
                }
            }
            std::atomic_fetch_add((std::atomic<int64_t>*) &total_cnts, cnt);
        }
        return total_cnts;
    }

    static int64_t GetTotalOfUnion(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, int64_t num_threads = UserArguments::NUM_THREADS) {
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        bitmap_entry_t one = 0x01;
        int64_t total_cnts = 0;

//#pragma omp parallel num_threads(num_threads)
        {
            int64_t cnt = 0;
//#pragma omp for schedule (dynamic, 64)
            for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (r.level_two_bitmap.entries_[i] == 0 && l.level_two_bitmap.entries_[i] == 0) continue;
#endif

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = l.entries_[j] | r.entries_[j];
                    cnt += _mm_popcnt_u64(tmp); //_popcnt64(tmp);
                }
            }
            std::atomic_fetch_add((std::atomic<int64_t>*) &total_cnts, cnt);
        }
        return total_cnts;
    }
    
    static int64_t GetTotalEstimationOfUnion(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, TwoLevelBitMap<Idx_t>& rr, int64_t num_threads = UserArguments::NUM_THREADS) {
#ifndef OPTIMIZE_SPARSE_COMPUTATION
				return GetTotalOfUnion(l, r, rr, num_threads);
#endif
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (rr.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        D_ASSERT (l.num_entries() == rr.num_entries());
        bitmap_entry_t one = 0x01;
        int64_t total_cnts = 0;

        for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
            bitmap_entry_t tmp = l.level_two_bitmap.entries_[i] | r.level_two_bitmap.entries_[i] | rr.level_two_bitmap.entries_[i];
            total_cnts += _mm_popcnt_u64(tmp); //_popcnt64(tmp);
        }
        return total_cnts;
    }
    
    static int64_t GetTotalEstimationOfUnion(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, int64_t num_threads = UserArguments::NUM_THREADS) {
#ifndef OPTIMIZE_SPARSE_COMPUTATION
				return GetTotalOfUnion(l, r, num_threads);
#endif
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        bitmap_entry_t one = 0x01;
        int64_t total_cnts = 0;

        for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
            bitmap_entry_t tmp = l.level_two_bitmap.entries_[i] | r.level_two_bitmap.entries_[i];
            total_cnts += _mm_popcnt_u64(tmp); //_popcnt64(tmp);
        }
        return total_cnts;
    }
    
    static int64_t GetTotalOfIntersection(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, int64_t num_threads = UserArguments::NUM_THREADS) {
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        bitmap_entry_t one = 0x01;
        int64_t total_cnts = 0;
   
#pragma omp parallel num_threads(num_threads)
        {
            int64_t cnt = 0;
//#pragma omp for schedule (dynamic, 64)
#pragma omp for
            for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (r.level_two_bitmap.entries_[i] == 0 || l.level_two_bitmap.entries_[i] == 0) continue;
#endif

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = l.entries_[j] & r.entries_[j];
                    cnt += _mm_popcnt_u64(tmp); //_popcnt64(tmp);
                }
            }
            std::atomic_fetch_add((std::atomic<int64_t>*) &total_cnts, cnt);
        }
		return total_cnts;
    }
    
    static void InvokeIfMarkedOnUnion(const std::function<void(IdxTmp_t)>& f, Range<IdxTmp_t> idx_range, bool single_thread, TwoLevelBitMap<IdxTmp_t>& l, TwoLevelBitMap<IdxTmp_t>& r) {
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        Range<Idx_t> level_two_range(idx_range.GetBegin() / SizeInBits(bitmap_entry_t) / NN, idx_range.GetEnd() / SizeInBits(bitmap_entry_t) / NN);
        bitmap_entry_t one = 0x01;

        if (single_thread) {
            for (Idx_t i = level_two_range.GetBegin(); i < level_two_range.GetEnd(); i++) {
                //if (r.level_two_bitmap.entries_[i] == 0 && l.level_two_bitmap.entries_[i] == 0) continue;

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = l.entries_[j] | r.entries_[j];
                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((tmp >> k) & one);
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (val && idx_range.contains(idx)) {
                            f(idx);
                        }
                    }
                }
            }
        } else {
//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
            for (Idx_t i = level_two_range.GetBegin(); i < level_two_range.GetEnd(); i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (r.level_two_bitmap.entries_[i] == 0 && l.level_two_bitmap.entries_[i] == 0) continue;
#endif

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = l.entries_[j] | r.entries_[j];
                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((tmp >> k) & one);
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (val && idx_range.contains(idx)) {
                            f(idx);
                        }
                    }
                }
            }
        }
    }
    

    static void InvokeIfMarkedOnUnion(const std::function<void(IdxTmp_t)>& f, TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, bool inv_l = false, bool inv_r = false) {
        D_ASSERT (l.entries_ != NULL);
        D_ASSERT (r.entries_ != NULL);
        D_ASSERT (l.num_entries() == r.num_entries());
        bitmap_entry_t one = 0x01;
        if (l.is_list_flag_ || r.is_list_flag_) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            D_ASSERT(l.is_list_flag_ && r.is_list_flag_); // XXX, support only list-list intersection currently
            auto l_iter_ = l.entries_list_.begin();
            auto r_iter_ = r.entries_list_.begin();
            if (!inv_l && !inv_r) {
                for (;;) {
                    if (l_iter_ == l.entries_list_.end()) {
                        for (;r_iter_ != r.entries_list_.end(); r_iter_++) {
                            f(*r_iter_);
                        }
                        break;
                    }
                    if (r_iter_ == r.entries_list_.end()) {
                        for (;l_iter_ != l.entries_list_.end(); l_iter_++) {
                            f(*l_iter_);
                        }
                        break;
                    }
                    if (*l_iter_ == *r_iter_) {
                        f(*l_iter_);
                        l_iter_++;
                        r_iter_++;
                    } else if (*l_iter_ < *r_iter_) {
                        f(*l_iter_);
                        l_iter_++;
                    } else {
                        f(*r_iter_);
                        r_iter_++;
                    }
                }
            } else {
                D_ASSERT(false);
            }
            return;
#else
						D_ASSERT(false);
#endif
        }

        if (inv_l) {
            if (inv_r) {
                D_ASSERT(false);
            } else {
//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
                for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
                    //if (r.level_two_bitmap.entries_[i] == 0 && l.level_two_bitmap.entries_[i] == 0) continue;

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                        bitmap_entry_t tmp = ~l.entries_[j] | r.entries_[j];
                        if (tmp == 0) continue;
                        for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                            bool val = (bool) ((tmp >> k) & one);
                            int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                            if (val && idx < l.num_entries_) {
                                f(idx);
                            }
                        }
                    }
                }
            }
        } else {
            if (inv_r) {
                D_ASSERT(false);
            } else {
#pragma omp parallel for schedule (dynamic, 64)
//#pragma omp parallel for
                for (Idx_t i = 0; i < l.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (r.level_two_bitmap.entries_[i] == 0 && l.level_two_bitmap.entries_[i] == 0) continue;
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(l.array_size, NN * (i+1)); j++) {
                        if (l.entries_[j] == 0 && r.entries_[j] == 0) continue;
                        bitmap_entry_t tmp = l.entries_[j] | r.entries_[j];
                        for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                            bool val = (bool) ((tmp >> k) & one);
                            int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                            if (val && idx < l.num_entries_) {
                                f(idx);
                            }
                        }
                    }
                }
            }
        }
    }
    
    static void InvokeIfMarkedOnUnion(const std::function<void(IdxTmp_t)>& f, std::list<TwoLevelBitMap<Idx_t>*>& bitmap_list, bool single_thread=false) {
        bitmap_entry_t one = 0x01;
        Idx_t lv2_bitmap_array_size = -1;
        Idx_t lv1_bitmap_array_size = -1;
        Idx_t lv1_num_entries = -1;

        for (auto& bitmap: bitmap_list) {
            D_ASSERT (bitmap->entries_ != NULL);
            
            Idx_t l2_bitmap_size = bitmap->level_two_bitmap.array_size;
            if (lv2_bitmap_array_size == -1) lv2_bitmap_array_size = l2_bitmap_size;
            else D_ASSERT(lv2_bitmap_array_size == l2_bitmap_size);

            Idx_t l1_bitmap_size = bitmap->array_size;
            if (lv1_bitmap_array_size == -1) lv1_bitmap_array_size = l1_bitmap_size;
            else D_ASSERT(lv1_bitmap_array_size == l1_bitmap_size);

            Idx_t l1_bitmap_num_entries = bitmap->num_entries();
            if (lv1_num_entries == -1) lv1_num_entries = l1_bitmap_num_entries;
            else D_ASSERT(lv1_num_entries == l1_bitmap_num_entries);
        }

        if (single_thread) {
            for (Idx_t i = 0; i < lv2_bitmap_array_size; i++) {
                bool skip = true;
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                for (auto& bitmap: bitmap_list) {
                    if (bitmap->level_two_bitmap.entries_[i] != 0) {
                        skip = false;
                        break;
                    }
                }
#endif
                if (skip) continue;

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(lv1_bitmap_array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = 0;
                    for (auto& bitmap: bitmap_list) {
                        tmp |= bitmap->entries_[j];
                    }
                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((tmp >> k) & one);
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (val && idx < lv1_num_entries) {
                            f(idx);
                        }
                    }
                }
            }
        } else {
//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
            for (Idx_t i = 0; i < lv2_bitmap_array_size; i++) {
                bool skip = true;
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                for (auto& bitmap: bitmap_list) {
                    if (bitmap->level_two_bitmap.entries_[i] != 0) {
                        skip = false;
                        break;
                    }
                }
#endif
                if (skip) continue;

                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(lv1_bitmap_array_size, NN * (i+1)); j++) {
                    bitmap_entry_t tmp = 0;
                    for (auto& bitmap: bitmap_list) {
                        tmp |= bitmap->entries_[j];
                    }
                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((tmp >> k) & one);
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (val && idx < lv1_num_entries) {
                            f(idx);
                        }
                    }
                }
            }
        }
    }
    
    static void Union(std::list<TwoLevelBitMap<Idx_t>*>& bitmap_list, TwoLevelBitMap<Idx_t>& o) {
        bitmap_entry_t one = 0x01;
        Idx_t lv2_bitmap_array_size = -1;
        Idx_t lv1_bitmap_array_size = -1;
        Idx_t lv1_num_entries = -1;

        for (auto& bitmap: bitmap_list) {
            D_ASSERT (bitmap->entries_ != NULL);
            
            Idx_t l2_bitmap_size = bitmap->level_two_bitmap.array_size;
            if (lv2_bitmap_array_size == -1) lv2_bitmap_array_size = l2_bitmap_size;
            else D_ASSERT(lv2_bitmap_array_size == l2_bitmap_size);

            Idx_t l1_bitmap_size = bitmap->array_size;
            if (lv1_bitmap_array_size == -1) lv1_bitmap_array_size = l1_bitmap_size;
            else D_ASSERT(lv1_bitmap_array_size == l1_bitmap_size);

            Idx_t l1_bitmap_num_entries = bitmap->num_entries();
            if (lv1_num_entries == -1) lv1_num_entries = l1_bitmap_num_entries;
            else D_ASSERT(lv1_num_entries == l1_bitmap_num_entries);
        }

//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
        for (Idx_t i = 0; i < lv2_bitmap_array_size; i++) {
            bool skip = true;
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            for (auto& bitmap: bitmap_list) {
                if (bitmap->level_two_bitmap.entries_[i] != 0) {
                    skip = false;
                    break;
                }
            }
#endif
            if (skip) continue;

            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

            for (Idx_t j = NN * i; j < std::min(lv1_bitmap_array_size, NN * (i+1)); j++) {
                bitmap_entry_t tmp = 0;
                for (auto& bitmap: bitmap_list) {
                    tmp |= bitmap->entries_[j];
                }
                o.entries_[j] = tmp;
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (o.entries_[j] != 0) {
                    Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                    do {
                        o.level_two_bitmap.Set_Atomic(k);
                        k++;
                    } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                }
#endif
            }
        }
    }
    
    static void InvokeIfMarkedOnIntersection(const std::function<void(IdxTmp_t)>& f, std::list<TwoLevelBitMap<Idx_t>*>& bitmap_list) {
        bitmap_entry_t one = 0x01;
        bitmap_entry_t mask = 0xffffffffffffffff;
        Idx_t lv2_bitmap_array_size = -1;
        Idx_t lv1_bitmap_array_size = -1;
        Idx_t lv1_num_entries = -1;

        for (auto& bitmap: bitmap_list) {
            D_ASSERT (bitmap->entries_ != NULL);
            
            Idx_t l2_bitmap_size = bitmap->level_two_bitmap.array_size;
            if (lv2_bitmap_array_size == -1) lv2_bitmap_array_size = l2_bitmap_size;
            else D_ASSERT(lv2_bitmap_array_size == l2_bitmap_size);

            Idx_t l1_bitmap_size = bitmap->array_size;
            if (lv1_bitmap_array_size == -1) lv1_bitmap_array_size = l1_bitmap_size;
            else D_ASSERT(lv1_bitmap_array_size == l1_bitmap_size);

            Idx_t l1_bitmap_num_entries = bitmap->num_entries();
            if (lv1_num_entries == -1) lv1_num_entries = l1_bitmap_num_entries;
            else D_ASSERT(lv1_num_entries == l1_bitmap_num_entries);
        }

//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
        for (Idx_t i = 0; i < lv2_bitmap_array_size; i++) {
            bool skip = false;
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            // XXX tslee
            /*for (auto& bitmap: bitmap_list) {
                if (bitmap->level_two_bitmap.entries_[i] == 0) {
                    skip = true;
                    break;
                }
            }*/
#endif
            if (skip) continue;

            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

            for (Idx_t j = NN * i; j < std::min(lv1_bitmap_array_size, NN * (i+1)); j++) {
                bitmap_entry_t tmp = mask;
                for (auto& bitmap: bitmap_list) {
                    tmp &= bitmap->entries_[j];
                }
                for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                    bool val = (bool) ((tmp >> k) & one);
                    int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                    if (val && idx < lv1_num_entries) {
                        f(idx);
                    }
                }
            }
        }
    }

    static void Xor(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, TwoLevelBitMap<Idx_t>& o)  {
        D_ASSERT (l.num_entries() == o.num_entries());
        D_ASSERT (l.num_entries() == r.num_entries());

#pragma omp parallel for
        for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

            for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                o.entries_[j] = l.entries_[j] ^ r.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (o.entries_[j] != 0) {
                    Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                    do {
                        o.level_two_bitmap.Set_Atomic(k);
                        k++;
                    } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                }
#endif
            }
        }
    }
    static void Intersection(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, std::list<node_t>& o, bool inv_l = false, bool inv_r = false)  {
        //D_ASSERT (l.num_entries() == o.num_entries());
        D_ASSERT (l.num_entries() == r.num_entries());

        if ((l.is_list_flag_ || r.is_list_flag_)) {
            D_ASSERT(l.is_list_flag_ && r.is_list_flag_); // XXX, support only list-list intersection currently
            if (!inv_l && !inv_r) {
                auto l_iter_ = l.entries_list_.begin();
                auto r_iter_ = r.entries_list_.begin();
                for (;;) {
                    if (l_iter_ == l.entries_list_.end()) break;
                    if (r_iter_ == r.entries_list_.end()) break;
                    if (*l_iter_ == *r_iter_) {
                        o.push_back(*l_iter_);
                        l_iter_++;
                        r_iter_++;
                    } else if (*l_iter_ < *r_iter_) {
                        l_iter_++;
                    } else {
                        r_iter_++;
                    }
                }
            } else {
                D_ASSERT(false);
            }
            return;
        }
    }

    static int64_t Intersection(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, TwoLevelBitMap<Idx_t>& o, bool inv_l = false, bool inv_r = false)  {
        D_ASSERT (l.num_entries() == o.num_entries());
        D_ASSERT (l.num_entries() == r.num_entries());

        int64_t cnts = -1;
        if ((l.is_list_flag_ || r.is_list_flag_)) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            cnts = 0;
            D_ASSERT(l.is_list_flag_ && r.is_list_flag_); // XXX, support only list-list intersection currently
            if (!inv_l && !inv_r) {
                if (o.is_list_flag_) {
                    auto l_iter_ = l.entries_list_.begin();
                    auto r_iter_ = r.entries_list_.begin();
                    for (;;) {
                        if (l_iter_ == l.entries_list_.end()) break;
                        if (r_iter_ == r.entries_list_.end()) break;
                        if (*l_iter_ == *r_iter_) {
                            o.entries_list_.push_back(*l_iter_);
                            cnts++;
                            l_iter_++;
                            r_iter_++;
                        } else if (*l_iter_ < *r_iter_) {
                            l_iter_++;
                        } else {
                            r_iter_++;
                        }
                    }
                } else {
                    auto l_iter_ = l.entries_list_.begin();
                    auto r_iter_ = r.entries_list_.begin();
                    for (;;) {
                        if (l_iter_ == l.entries_list_.end()) break;
                        if (r_iter_ == r.entries_list_.end()) break;
                        if (*l_iter_ == *r_iter_) {
                            o.Set(*l_iter_);//o.TwoLevelBitMap<IdxTmp_t>::Set(*l_iter_);
                            cnts++;
                            l_iter_++;
                            r_iter_++;
                        } else if (*l_iter_ < *r_iter_) {
                            l_iter_++;
                        } else {
                            r_iter_++;
                        }
                    }
                }
            } else if (!inv_l && inv_r) {
                if (o.is_list_flag_) {
                    auto l_iter_ = l.entries_list_.begin();
                    auto r_iter_ = r.entries_list_.begin();
                    for (;;) {
                        if (l_iter_ == l.entries_list_.end()) break;
                        if (r_iter_ == r.entries_list_.end()) {
                            for (;l_iter_ != l.entries_list_.end(); l_iter_++) {
                                o.entries_list_.push_back(*l_iter_);
                                cnts++;
                            }
                            break;
                        }
                        if (*l_iter_ == *r_iter_) {
                            l_iter_++;
                            r_iter_++;
                        } else if (*l_iter_ < *r_iter_) {
                            o.entries_list_.push_back(*l_iter_);
                            cnts++;
                            l_iter_++;
                        } else {
                            r_iter_++;
                        }
                    }
                } else {
                    auto l_iter_ = l.entries_list_.begin();
                    auto r_iter_ = r.entries_list_.begin();
                    for (;;) {
                        if (l_iter_ == l.entries_list_.end()) break;
                        if (r_iter_ == r.entries_list_.end()) {
                            for (;l_iter_ != l.entries_list_.end(); l_iter_++) {
                                o.Set(*l_iter_);//o.TwoLevelBitMap<IdxTmp_t>::Set(*l_iter_);
                                cnts++;
                            }
                            break;
                        }
                        if (*l_iter_ == *r_iter_) {
                            l_iter_++;
                            r_iter_++;
                        } else if (*l_iter_ < *r_iter_) {
                            o.entries_list_.push_back(*l_iter_);
                            cnts++;
                            l_iter_++;
                        } else {
                            r_iter_++;
                        }
                    }
                }
            } else {
                D_ASSERT(false);
            }
            return cnts;
#else
						D_ASSERT(false);
#endif
        }

        if (inv_l) {
            if (inv_r) {
                D_ASSERT(false);
#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
                    if (~l.level_two_bitmap.entries_[i] == 0 || ~r.level_two_bitmap.entries_[i] == 0) {
                        if (o.level_two_bitmap.entries_[i] != 0) {
                            for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                                if (o.entries_[j] != 0) o.entries_[j] = 0;
                            }
                            o.level_two_bitmap.entries_[i] = 0;
                        }
                        continue;
                    }

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    bool intersec = false;
                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = ~l.entries_[j] & ~r.entries_[j];
                        if (o.entries_[j] != 0) intersec = true;
                    }
                    if (intersec) {
                        o.level_two_bitmap.entries_[i] = ~l.level_two_bitmap.entries_[i] & ~r.level_two_bitmap.entries_[i];
                    }
                    //fprintf(stdout, "Intersection [%ld, %ld) at Level 2 --> [%ld, %ld) at Level 1\n", i * SizeInBits(bitmap_entry_t), (i + 1) * SizeInBits(bitmap_entry_t), NN * i * SizeInBits(bitmap_entry_t), std::min(o.num_entries_, (int64_t) (NN * (i+1) * SizeInBits(bitmap_entry_t))));
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < r.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (r.level_two_bitmap.entries_[i] == 0) {
                        continue;
                    }
#endif
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
                    for (Idx_t j = NN * i; j < std::min(r.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = r.entries_[j] & ~l.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (o.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                o.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
                }
            };
        } else {
            if (inv_r) {
                D_ASSERT(false);

#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (l.level_two_bitmap.entries_[i] == 0 || ~r.level_two_bitmap.entries_[i] == 0) {
                        if (o.level_two_bitmap.entries_[i] != 0) {
                            for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                                if (o.entries_[j] != 0) o.entries_[j] = 0;
                            }
                            o.level_two_bitmap.entries_[i] = 0;
                        }
                        continue;
                    }
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    bool intersec = false;
                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = l.entries_[j] & ~r.entries_[j];
                        if (o.entries_[j] != 0) intersec = true;
                    }
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (intersec) {
                        o.level_two_bitmap.entries_[i] = l.level_two_bitmap.entries_[i] & ~r.level_two_bitmap.entries_[i];
                    }
#endif
                    //fprintf(stdout, "Intersection [%ld, %ld) at Level 2 --> [%ld, %ld) at Level 1\n", i * SizeInBits(bitmap_entry_t), (i + 1) * SizeInBits(bitmap_entry_t), NN * i * SizeInBits(bitmap_entry_t), std::min(o.num_entries_, (int64_t) (NN * (i+1) * SizeInBits(bitmap_entry_t))));
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (l.level_two_bitmap.entries_[i] == 0 || r.level_two_bitmap.entries_[i] == 0) {
                        if (o.level_two_bitmap.entries_[i] != 0) {
                            for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                                if (o.entries_[j] != 0) o.entries_[j] = 0;
                            }
                            o.level_two_bitmap.entries_[i] = 0;
                        }
                        continue;
                    }
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    bool intersec = false;
                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = l.entries_[j] & r.entries_[j];
                        if (o.entries_[j] != 0) intersec = true;
                    }
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (intersec) {
                        o.level_two_bitmap.entries_[i] = l.level_two_bitmap.entries_[i] & r.level_two_bitmap.entries_[i];
                    }
#endif
                }
            };
        }
        return cnts;
    }

    static void IntersectionToRight(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, bool inv_l = false, bool inv_r = false) {
        if ((l.is_list_flag_ || r.is_list_flag_) && !inv_l && !inv_r) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            D_ASSERT(l.is_list_flag_ && r.is_list_flag_); // XXX, support only list-list intersection currently
            auto l_iter_ = l.entries_list_.begin();
            auto r_iter_ = r.entries_list_.begin();
            for (;;) {
                if (l_iter_ == l.entries_list_.end()) {
                    r.entries_list_.erase(r_iter_, r.entries_list_.end());
                    break;
                }
                if (r_iter_ == r.entries_list_.end()) break;
                if (*l_iter_ == *r_iter_) {
                    l_iter_++;
                    r_iter_++;
                } else if (*l_iter_ < *r_iter_) {
                    l_iter_++;
                } else {
                    r_iter_ = r.entries_list_.erase(r_iter_);
                }
            }
            return;
#else
						D_ASSERT(false);
#endif
        }
        if (inv_l) {
            if (inv_r) {
                D_ASSERT(false);
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < r.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (r.level_two_bitmap.entries_[i] == 0) {
                        continue;
                    }
#endif
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
                    for (Idx_t j = NN * i; j < std::min(r.array_size, NN * (i+1)); j++) {
                        r.entries_[j] &= ~l.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (r.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                r.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
                }
            }
        } else {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < r.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (l.level_two_bitmap.entries_[i] == 0) {
                        continue;
                    }
#endif
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);
                    for (Idx_t j = NN * i; j < std::min(r.array_size, NN * (i+1)); j++) {
                        r.entries_[j] = l.entries_[j] & ~r.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (r.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                r.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
                }
            } else {
                D_ASSERT(false);
            }
        }
    }

    // Union(l, r) -> r
    static void UnionToRight(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, bool inv_l = false, bool inv_r = false) {
        if ((l.is_list_flag_ || r.is_list_flag_) && !inv_l && !inv_r) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            if(!(l.is_list_flag_ && r.is_list_flag_)) {
                fprintf(stdout, "%s %s\n", l.is_list_flag_ ? "true" : "false", r.is_list_flag_ ? "true" : "false");
            }
            D_ASSERT(l.is_list_flag_ && r.is_list_flag_); // XXX, support only list-list intersection currently
            auto l_iter_ = l.entries_list_.begin();
            auto r_iter_ = r.entries_list_.begin();
            for (;;) {
                if (l_iter_ == l.entries_list_.end()) break;
                if (r_iter_ == r.entries_list_.end()) {
                    for (;l_iter_ != l.entries_list_.end(); l_iter_++) {
                        r.entries_list_.push_back(*l_iter_);
                    }
                    break;
                }
                if (*l_iter_ == *r_iter_) {
                    l_iter_++;
                    r_iter_++;
                } else if (*l_iter_ < *r_iter_) {
                    r.entries_list_.insert(r_iter_, *l_iter_);
                    l_iter_++;
                } else {
                    r_iter_++;
                }
            }
            return;
#else
						D_ASSERT(false);
#endif
        }
        if (inv_l) {
            if (inv_r) {
                D_ASSERT(false);
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < r.level_two_bitmap.array_size; i++) {
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(r.array_size, NN * (i+1)); j++) {
                        r.entries_[j] |= ~l.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (r.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                r.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
               }
            }
        } else {
            if (inv_r) {
                D_ASSERT(false);
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < r.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (l.level_two_bitmap.entries_[i] == 0) {
                        continue;
                    }
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(r.array_size, NN * (i+1)); j++) {
                        r.entries_[j] |= l.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (r.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                r.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
                }
            }
        }
    }

    static void Union(TwoLevelBitMap<Idx_t>& l, TwoLevelBitMap<Idx_t>& r, TwoLevelBitMap<Idx_t>& o, bool inv_l = false, bool inv_r = false) {
        D_ASSERT (l.num_entries() == o.num_entries());
        D_ASSERT (l.num_entries() == r.num_entries());
        if ((l.is_list_flag_ || r.is_list_flag_) && !inv_l && !inv_r) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            D_ASSERT(l.is_list_flag_ && r.is_list_flag_); // XXX, support only list-list intersection currently
            auto l_iter_ = l.entries_list_.begin();
            auto r_iter_ = r.entries_list_.begin();
            if (!inv_l && !inv_r) {
                for (;;) {
                    if (l_iter_ == l.entries_list_.end()) {
                        for (;r_iter_ != r.entries_list_.end(); r_iter_++) {
                            o.entries_list_.push_back(*r_iter_);
                        }
                        break;
                    }
                    if (r_iter_ == r.entries_list_.end()) {
                        for (;l_iter_ != l.entries_list_.end(); l_iter_++) {
                            o.entries_list_.push_back(*l_iter_);
                        }
                        break;
                    }
                    if (*l_iter_ == *r_iter_) {
                        o.entries_list_.push_back(*l_iter_);
                        l_iter_++;
                        r_iter_++;
                    } else if (*l_iter_ < *r_iter_) {
                        o.entries_list_.push_back(*l_iter_);
                        l_iter_++;
                    } else {
                        o.entries_list_.push_back(*r_iter_);
                        r_iter_++;
                    }
                } 
            } else {
                D_ASSERT(false);
            }
            return;
#else
						D_ASSERT(false);
#endif
        }

        if (inv_l) {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = ~l.entries_[j] | ~r.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (o.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                o.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = ~l.entries_[j] | r.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (o.entries_[j] != 0) {
                            for (Idx_t k = j * SizeInBits(bitmap_entry_t) / NN; k < (j + 1) * SizeInBits(bitmap_entry_t) / NN; k++) {
                                o.level_two_bitmap.Set_Atomic(k);
                            }
                        }
#endif
                    }
                }
            }
        } else {
            if (inv_r) {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = l.entries_[j] | ~r.entries_[j];
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (o.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                o.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }
                }
            } else {
#pragma omp parallel for
                for (Idx_t i = 0; i < o.level_two_bitmap.array_size; i++) {
//                    std::cout << std::bitset<64> (l.level_two_bitmap.entries_[i]) << std::endl;
//                    std::cout << std::bitset<64> (r.level_two_bitmap.entries_[i]) << std::endl;
//                    std::cout << std::bitset<64> (o.level_two_bitmap.entries_[i]) << std::endl;

#ifdef OPTIMIZE_SPARSE_COMPUTATION
                    if (l.level_two_bitmap.entries_[i] == 0 && r.level_two_bitmap.entries_[i] == 0) {
                        if (o.level_two_bitmap.entries_[i] != 0) {
                            for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                                if (o.entries_[j] != 0) o.entries_[j] = 0;
                            }
                            o.level_two_bitmap.entries_[i] = 0;
                        }
                        continue;
                    }
#endif

                    D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                    D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                    for (Idx_t j = NN * i; j < std::min(o.array_size, NN * (i+1)); j++) {
                        o.entries_[j] = l.entries_[j] | r.entries_[j];

#ifdef OPTIMIZE_SPARSE_COMPUTATION
                        if (o.entries_[j] != 0) {
                            Idx_t k = j * SizeInBits(bitmap_entry_t) / NN;
                            do {
                                o.level_two_bitmap.Set_Atomic(k);
                                k++;
                            } while (k < (j + 1) * SizeInBits(bitmap_entry_t) / NN);
                        }
#endif
                    }

//                    std::cout << std::bitset<64> (l.level_two_bitmap.entries_[i]) << std::endl;
//                    std::cout << std::bitset<64> (r.level_two_bitmap.entries_[i]) << std::endl;
//                    std::cout << std::bitset<64> (o.level_two_bitmap.entries_[i]) << std::endl;
                }
            }
        }
    }


    static void ComplexOperation1(TwoLevelBitMap<Idx_t>& A, TwoLevelBitMap<Idx_t>& B, TwoLevelBitMap<Idx_t>& C, TwoLevelBitMap<Idx_t>& X, TwoLevelBitMap<Idx_t>& Y, TwoLevelBitMap<Idx_t>& Z, bool use_list=false) {
        if (use_list) {
            TwoLevelBitMap<int64_t>::IntersectionToRight(A, C);
            TwoLevelBitMap<int64_t>::IntersectionToRight(B, C);
            
            TwoLevelBitMap<int64_t>::Intersection(A, B, X, false, true);
            TwoLevelBitMap<int64_t>::UnionToRight(C, X);
            
            TwoLevelBitMap<int64_t>::Intersection(B, A, Y, false, true);
            TwoLevelBitMap<int64_t>::UnionToRight(C, Y);
        } else {
            D_ASSERT (A.num_entries() == B.num_entries());
            D_ASSERT (A.num_entries() == C.num_entries());
            D_ASSERT (A.num_entries() == X.num_entries());
            TwoLevelBitMap<int64_t>::Intersection(B, C, X);
            TwoLevelBitMap<int64_t>::UnionToRight(B, X, true, false);
            TwoLevelBitMap<int64_t>::Intersection(X, A, X);

            TwoLevelBitMap<int64_t>::Intersection(A, C, Y);
            TwoLevelBitMap<int64_t>::UnionToRight(A, Y, true, false);
            TwoLevelBitMap<int64_t>::Intersection(Y, B, Y);
        }
    }
    
    // X <- ~X AND (A AND ~B)
    static void ComplexOperation2(TwoLevelBitMap<Idx_t>& A, TwoLevelBitMap<Idx_t>& B, TwoLevelBitMap<Idx_t>& X) {
        D_ASSERT (A.num_entries() == B.num_entries());
        D_ASSERT (A.num_entries() == X.num_entries());
        
        TwoLevelBitMap<int64_t>::IntersectionToRight(A, X, false, true);
        TwoLevelBitMap<int64_t>::IntersectionToRight(B, X, true, false);
    }


    // Invoke f only if
    // ~A AND B != C
    static void ComplexOperation3(TwoLevelBitMap<Idx_t>& A, TwoLevelBitMap<Idx_t>& B, TwoLevelBitMap<Idx_t>& C, const std::function<void(IdxTmp_t)>& f) {
        D_ASSERT (A.num_entries() == B.num_entries());
        D_ASSERT (A.num_entries() == C.num_entries());
        bitmap_entry_t one = 0x01;

//#pragma omp parallel for schedule (dynamic, 64)
#pragma omp parallel for
        for (Idx_t i = 0; i < A.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            if (B.level_two_bitmap.entries_[i] == 0 && C.level_two_bitmap.entries_[i] == 0 ) continue;
#endif

            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

            for (Idx_t j = NN * i; j < std::min(A.array_size, NN * (i+1)); j++) {
                bitmap_entry_t tmp = ~A.entries_[j] & (B.entries_[j] ^ C.entries_[j]);
                if (tmp == 0) continue;
                for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                    bool val = (bool) ((tmp >> k) & one);
                    int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                    if (val && idx < A.num_entries_) {
                        f(idx);
                    }
                }
            }
        }
    }

    // D <- (A AND ~B AND D)
    // C <- C OR D
    static void ComplexOperation4(TwoLevelBitMap<Idx_t>& A, TwoLevelBitMap<Idx_t>& B, TwoLevelBitMap<Idx_t>& C, TwoLevelBitMap<Idx_t>& D) {
        D_ASSERT (A.num_entries() == B.num_entries());
        D_ASSERT (A.num_entries() == D.num_entries());

        TwoLevelBitMap<int64_t>::IntersectionToRight(A, D);
        TwoLevelBitMap<int64_t>::IntersectionToRight(B, D, true, false);
        TwoLevelBitMap<int64_t>::UnionToRight(D, C);
    }
    
    int64_t GetTotalEstimation(int64_t num_threads = UserArguments::NUM_THREADS) {
#ifndef OPTIMIZE_SPARSE_COMPUTATION
				return GetTotal(num_threads);
#else
        int64_t total_cnts = 0;
        for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
            total_cnts += _mm_popcnt_u64(level_two_bitmap.entries_[i]); //_popcnt64(level_two_bitmap.entries_[i]);
        }
				return total_cnts * NN;
#endif
    }
    
	
    int64_t GetTotal(int64_t num_threads = UserArguments::NUM_THREADS) {
        if (this->is_list_flag_) return this->entries_list_.size();
        int64_t total_cnts = 0;
#pragma omp parallel num_threads(num_threads) 
        {
						int64_t per_thread_cnts = 0;
#pragma omp for
            for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
                if (level_two_bitmap.entries_[i] == 0) continue;
#endif
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                    per_thread_cnts += _mm_popcnt_u64(this->entries_[j]); //_popcnt64(this->entries_[j]);
                }
            }
						std::atomic_fetch_add((std::atomic<int64_t>*) &total_cnts, per_thread_cnts);
        }
				return total_cnts;
    }
    
    int64_t GetTotalOfLv2() {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        int64_t total_cnts = level_two_bitmap.GetTotal();
				return total_cnts;
#else
				D_ASSERT(false);
				return 0;
#endif
    }

    // Invoke f only if
    // ~A AND (B U C)
    static void ComplexOperation5(TwoLevelBitMap<Idx_t>& A, TwoLevelBitMap<Idx_t>& B, TwoLevelBitMap<Idx_t>& C, const std::function<void(IdxTmp_t)>& f) {
        D_ASSERT (A.num_entries() == B.num_entries());
        D_ASSERT (A.num_entries() == C.num_entries());
        bitmap_entry_t one = 0x01;

#pragma omp parallel for
        for (Idx_t i = 0; i < A.level_two_bitmap.array_size; i++) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            if (B.level_two_bitmap.entries_[i] == 0 && C.level_two_bitmap.entries_[i] == 0 ) continue;
#endif

            D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
            D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

            for (Idx_t j = NN * i; j < std::min(A.array_size, NN * (i+1)); j++) {
                bitmap_entry_t tmp = ~A.entries_[j] & (B.entries_[j] | C.entries_[j]);
                if (tmp == 0) continue;
                for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                    bool val = (bool) ((tmp >> k) & one);
                    int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                    if (val && idx < A.num_entries_) {
                        f(idx);
                    }
                }
            }
        }
    }
    
    // X <- A U (B XOR C)
    static void ComplexOperation6(TwoLevelBitMap<Idx_t>& A, TwoLevelBitMap<Idx_t>& B, TwoLevelBitMap<Idx_t>& C, TwoLevelBitMap<Idx_t>& X) {
        D_ASSERT (A.num_entries() == B.num_entries());
        D_ASSERT (A.num_entries() == X.num_entries());
        
        TwoLevelBitMap<int64_t>::Xor(B, C, X);
        TwoLevelBitMap<int64_t>::UnionToRight(A, X);
    }

    int64_t CopyFrom(TwoLevelBitMap<Idx_t>& from, bool copy_list_only=false) {
        D_ASSERT (BitMap<IdxTmp_t>::num_entries() == from.BitMap<IdxTmp_t>::num_entries());
        int64_t cnts = -1;
        if (from.is_list_flag_) {
            D_ASSERT(GetTotal() == 0);
            if (copy_list_only) {
                D_ASSERT(this->is_list_flag_);
                this->entries_list_.assign(from.entries_list_.begin(), from.entries_list_.end());
            } else {
                auto from_iter_ = from.entries_list_.begin();
                for (;from_iter_ != from.entries_list_.end(); from_iter_++) {
                    TwoLevelBitMap<IdxTmp_t>::Set(*from_iter_);
                }
                cnts = from.entries_list_.size();
            }
        } else {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
            level_two_bitmap.BitMap<IdxTmp_t>::CopyFrom(from.level_two_bitmap);
#endif
            BitMap<IdxTmp_t>::CopyFrom(from);
        }
        return cnts;
    }

    void SetAll() {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        level_two_bitmap.SetAll();
#endif
        BitMap<IdxTmp_t>::SetAll();
    }

    void SwapBitMap(TwoLevelBitMap<Idx_t>& another) {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        level_two_bitmap.SwapBitMap(another.level_two_bitmap);
#endif
        BitMap<IdxTmp_t>::SwapBitMap(another);
    }

    void MarkLevelTwoBitMapAccordingToTheLevelOneBitMap() {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        level_two_bitmap.ClearAll();
        BitMap<Idx_t>::InvokeIfMarked([&](node_t idx) {
            level_two_bitmap.Set_Atomic(idx / NN);
        });
#endif
    }

    void SanityCheck() {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
        BitMap<Idx_t>::InvokeIfMarked([&](node_t idx) {
            D_ASSERT(level_two_bitmap.Get(idx / NN));
        });
#endif
    }

    void ConvertToList(bool is_bitmap_empty=false, bool change_flag_only=false) {
#ifndef OPTIMIZE_SPARSE_COMPUTATION
				D_ASSERT(false);
				return;
#endif
        //D_ASSERT(this->entries_list_.empty());
        if (change_flag_only) {
            this->is_list_flag_ = true;
            return;
        }
        if (this->is_list_flag_) return;
        if (is_bitmap_empty) {
            D_ASSERT(GetTotal() == 0);
            this->is_list_flag_ = true;
        } else {
            for (Idx_t i = 0; i < level_two_bitmap.array_size; i++) {
                if (level_two_bitmap.entries_[i] == 0) continue;
                D_ASSERT(NN * i % SizeInBits(bitmap_entry_t) == 0);
                D_ASSERT(NN * (i + 1) % SizeInBits(bitmap_entry_t) == 0);

                for (Idx_t j = NN * i; j < std::min(this->array_size, NN * (i+1)); j++) {
                    if (this->entries_[j] == 0) continue;

                    for (int64_t k = 0; k < SizeInBits(bitmap_entry_t); k++) {
                        bool val = (bool) ((this->entries_[j] >> k) & this->one);
                        if (!val) continue;
                        int64_t idx = j * SizeInBits(bitmap_entry_t) + k;
                        if (idx < this->num_entries_) {
                            this->entries_list_.push_back(idx);
                        }
                    }
                }
            }
            this->is_list_flag_ = true;
        }
    }

    void RevertList(bool do_not_mark_bitmap=false, bool do_not_clear_bitmap=false, bool change_flag_only=false, bool remain_list=false) {
#ifndef OPTIMIZE_SPARSE_COMPUTATION
				D_ASSERT(false);
				return;
#endif
        if (change_flag_only) {
            this->is_list_flag_ = false;
            return;
        }
        if (do_not_mark_bitmap) {
            if (remain_list) {
                this->remain_list_ = true;
            } else {
                this->entries_list_.clear();
                this->remain_list_ = false;
            }
            this->is_list_flag_ = false;
        } else {
            if (!do_not_clear_bitmap) {
                D_ASSERT(false); // avoid this case !!
                TwoLevelBitMap<IdxTmp_t>::ClearAll();
            }
            auto iter_ = this->entries_list_.begin();
            for (;iter_ != this->entries_list_.end(); iter_++) {
                TwoLevelBitMap<IdxTmp_t>::Set(*iter_);
            }
            if (remain_list) {
                this->remain_list_ = true;
            } else {
                this->remain_list_ = false;
                this->entries_list_.clear();
            }
            this->is_list_flag_ = false;
        }
    }

    char* two_level_data() {
        return level_two_bitmap.data();
    }
    
    int64_t two_level_container_size() {
        return level_two_bitmap.container_size();
    }

    int64_t total_container_size() {
        int64_t bytes = 0;
        bytes += level_two_bitmap.container_size();
        bytes += BitMap<Idx_t>::container_size();
        return bytes;
    }

  private:

    BitMap<IdxTmp_t> level_two_bitmap;
};

#endif
