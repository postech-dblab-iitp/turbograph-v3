#pragma once
#ifndef GB_VERSIONED_BITMAP_H
#define GB_VERSIONED_BITMAP_H

#include "analytics/datastructure/GBVersionedArrayBase.hpp"
#include "analytics/datastructure/MemoryMappedBitMap.hpp"

//#define PER_THREAD_BUFFER_SIZE (32*1024)
//#define FLOAT_PRECISION_TOLERANCE 0.02

#define Abs(x)    ((x) < 0 ? -(x) : (x))
#define Max(a, b) ((a) > (b) ? (a) : (b))

template <typename T, Op op>
class GBVersionedBitMap : public GBVersionedArrayBase<T, op> {

    public:

        GBVersionedBitMap() : ref(NULL) {}
        
        ~GBVersionedBitMap() {}
        
        virtual bool IsCreated() {
            return true;
        }
        
        virtual void InitReadOnly(GBVersionedArrayBase<T, op>& o, bool maintain_prev_ss=false) {
			this->buf_length = o.GetBufLength();
            this->array_path = o.GetArrayPath();

            this->buf_memory_usage = BitMap<int64_t>::compute_container_size(this->buf_length);
            this->bitmap_memory_usage = 0;

            this->current_version.SetUpdateVersion(0);
            this->current_version.SetSuperstepVersion(-1);

            GBVersionedArrayBase<T, op>* o_temp = &o;
            ref = dynamic_cast<GBVersionedBitMap<T, op>*>(o_temp);
            INVARIANT(ref != NULL);
        }
        
        virtual void CreateReadOnly() {
            if (UserArguments::NUM_ITERATIONS != INT_MAX) {
            //if (false) {
                buf.resize(UserArguments::NUM_ITERATIONS + 2);
                for (int i = 0; i < UserArguments::NUM_ITERATIONS + 2; i++) {
                    buf[i] = new TwoLevelBitMap<int64_t>();
                    buf[i]->Init(this->buf_length);
                }
                pre_allocated = true;
            } else {
                buf.resize(2);
                buf[0] = new TwoLevelBitMap<int64_t>();
                buf[1] = new TwoLevelBitMap<int64_t>();
                buf[0]->Init(this->buf_length);
                buf[1]->Init(this->buf_length);
            }
            buf_ptr = buf[0];
            next_buf_ptr = buf[1];
            LocalStatistics::register_mem_alloc_info((this->array_name + " GBVersionedArray(ReadOnly)").c_str(), this->buf_memory_usage / (1024 * 1024));
        }
        
        void SetPrev(GBVersionedBitMap<T, op>& prev_) {
            prev = &prev_;
        }
        
        virtual void InitializePerThreadBuffers() { // XXX move to base function with buffer
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                this->ptr_to_per_thread_buffer[i] = (DeltaMessage<T>*) RequestRespond::GetPerThreadBuffer();
                this->idx_in_per_thread_buffer[i].idx = 0;
            }
            
            cur_read_idx = 0;
            prev_read_idx = 0;
        }

        bool hasVersion(int u, int s) {
            INVARIANT(false); // XXX tslee 2019.07.24 not used
        }
        
        virtual void ComputeMemoryUsage() {
            this->buf_memory_usage = 0;
            this->bitmap_memory_usage = 0;

            // Compute memory usage for Main & Buffers
            this->buf_memory_usage += (2 * BitMap<int64_t>::compute_container_size(this->buf_length));
            
            // Compute memory usage for Bitmaps
            this->bitmap_memory_usage += (5 * BitMap<int64_t>::compute_container_size(this->buf_length));
            if (this->maintain_prev_ss_) this->bitmap_memory_usage += BitMap<int64_t>::compute_container_size(this->buf_length);
            if (this->advance_superstep_ && this->construct_next_ss_array_async_) this->bitmap_memory_usage += (4 * BitMap<int64_t>::compute_container_size(this->buf_length));
        }
        
        virtual void InitializeMainAndBuffer(std::string array_path, bool open) {
            if (UserArguments::NUM_ITERATIONS != INT_MAX) {
            //if (false) {
                buf.resize(UserArguments::NUM_ITERATIONS + 2);
                for (int i = 0; i < UserArguments::NUM_ITERATIONS + 2; i++) {
                    buf[i] = new TwoLevelBitMap<int64_t>();
                    buf[i]->Init(this->buf_length);
                }
                pre_allocated = true;
            } else {
                buf.resize(2);
                buf[0] = new TwoLevelBitMap<int64_t>();
                buf[1] = new TwoLevelBitMap<int64_t>();
                buf[0]->Init(this->buf_length);
                buf[1]->Init(this->buf_length);
            }
            buf_ptr = buf[0];
            next_buf_ptr = buf[1];
            sparse_mode = false;
            prev_sparse_mode = false;
            posix_memalign((void**) &is_set, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
            for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
                is_set[tid].idx = 0;
            }
            cnt1 = 0;
            cnt2 = 0;
        }
         
        virtual int64_t FreeMainAndBuffer(bool rm) {
            this->buf_length = 0;
            LocalStatistics::register_mem_alloc_info((this->array_path + " GBVersionedBitMap").c_str(), 0);
            
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                if (this->ptr_to_per_thread_buffer[i] == NULL) continue;
                char* data = (char*) this->ptr_to_per_thread_buffer[i];
                RequestRespond::ReturnPerThreadBuffer(data);
            }
            return -1; // TODO - return something?
        }

        virtual void CopyBuffer(GBVersionedArrayBase<T, op>* prev_) {
        }
        
        virtual void CopyMainBuffer() {
        }
        
        virtual void CopyMainBufferFromRef(GBVersionedArrayBase<T, op>* prev) {
        }
        
        void PrepareNextSSBuffer() {
            if (!this->construct_next_ss_array_async_) return;
            //fprintf(stdout, "%s PrepareNextSSBuffer at (%d, %d)\n", this->array_name.c_str(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
            if (UserArguments::UPDATE_VERSION == 0) {
                if (!pre_allocated) {
                    INVARIANT(buf.size() == UserArguments::SUPERSTEP + 2);
                    buf.push_back(new TwoLevelBitMap<int64_t>());
                    //fprintf(stdout, "%s buf.size = %ld, ss = %d\n", this->array_name.c_str(), buf.size(), UserArguments::SUPERSTEP);
                    buf[UserArguments::SUPERSTEP + 2]->Init(this->buf_length);
                    //fprintf(stdout, "%s buf init size %ld\n", this->array_name.c_str(), buf[UserArguments::SUPERSTEP + 1]->total_container_size());
                    next_buf_ptr = buf[UserArguments::SUPERSTEP + 2];
                    //fprintf(stdout, "%s CopyFrom\n", this->array_name.c_str());
                    //buf[UserArguments::SUPERSTEP + 1]->CopyFrom(*buf[UserArguments::SUPERSTEP]);
                } else {
                    next_buf_ptr = buf[UserArguments::SUPERSTEP + 2];
                }
            } else if (UserArguments::UPDATE_VERSION >= 1) {
                next_buf_ptr = buf[UserArguments::SUPERSTEP + 2];
                //fprintf(stdout, "%s PrepareNextSSBuffer at (%d, %d) to %d, total = %ld\n", this->array_name.c_str(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::SUPERSTEP + 2, next_buf_ptr->GetTotal());
            }
            if (prev != NULL) {
                std::vector<TwoLevelBitMap<int64_t>*>& prev_buf = prev->GetBuf();
                if (UserArguments::UPDATE_VERSION == 0) {
                    if (!pre_allocated) {
                        INVARIANT(prev_buf.size() == UserArguments::SUPERSTEP + 2);
                        prev_buf.push_back(new TwoLevelBitMap<int64_t>());
                        prev_buf[UserArguments::SUPERSTEP + 2]->Init(this->buf_length);
                        //fprintf(stdout, "memcpy %p <- %p\n", prev_buf[UserArguments::SUPERSTEP], buf[UserArguments::SUPERSTEP]);
                        prev_buf[UserArguments::SUPERSTEP + 1]->CopyFrom(*buf[UserArguments::SUPERSTEP + 1]);
                        if (UserArguments::SUPERSTEP == 0) {
                            prev_buf[UserArguments::SUPERSTEP]->CopyFrom(*buf[UserArguments::SUPERSTEP]);
                        }
                        //for (int64_t idx = 0; idx < 10; idx++)
                        //    fprintf(stdout, "%s prev_buf[%d][%ld] = %ld\n", this->array_name.c_str(), UserArguments::SUPERSTEP, idx, prev_buf[UserArguments::SUPERSTEP][idx]);
                    } else {
                        prev_buf[UserArguments::SUPERSTEP + 1]->CopyFrom(*buf[UserArguments::SUPERSTEP + 1]);
                        if (UserArguments::SUPERSTEP == 0) {
                            prev_buf[UserArguments::SUPERSTEP]->CopyFrom(*buf[UserArguments::SUPERSTEP]);
                        }
                    }
                } else if (UserArguments::UPDATE_VERSION >= 1) {
                }
            }
        }
        
        virtual int64_t GetNumberOfSuperstepVersions(int update_version) {
            if (update_version == 0) return buf.size() - 1;
            else return UserArguments::SUPERSTEP;
        }
        
        // IV
        virtual void MergeWithNext(Version from, Version to, GBVersionedArrayBase<T, op>* prev) {
            return;
        }
        
        virtual void MergeWithNext(Version from, Version to) {
            LOG_ASSERT(false); // by syko. 2019/10/30
        }
        
        static void Callback_ReadUpdatesAndConstructWithoutFlush_(diskaio::DiskAioRequest* req) {
            LOG_ASSERT(false);
        }
        
        static void Callback_ReadUpdatesAndConstructWithoutFlush_ListVersion_(diskaio::DiskAioRequest* req) {
            LOG_ASSERT(false);
        }

        virtual void ReadUpdatesAndConstructWithoutFlush(int i, int j, bool construct_next, bool construct_prev, bool mark_changed, GBVersionedArrayBase<T, op>* prev_) {
            LOG_ASSERT(false);
        }
        
        void ReadUpdatesAndConstructWithoutFlush_(int i, int j, TwoLevelBitMap<int64_t>* buf_, bool construct_next) {
            LOG_ASSERT(false);
        }
        
        virtual void InitializeAfterAdvanceUpdateVersion() {
            //sparse_mode = false;
            //prev_sparse_mode = false;
            //this->RevertNextFlagsAll();
            buf_ptr = buf[0]; 
            if (buf.size() >= 2)
                next_buf_ptr = buf[1];
            else next_buf_ptr = buf[0];
            //fprintf(stdout, "%s InitializeAfterAdvanceUpdateVersion ss ver = 0 (cur %d) buf_ptr %p next_buf_ptr %p\n", this->array_name.c_str(), this->current_version.GetSuperstepVersion(), buf_ptr, next_buf_ptr); 
            if (prev != NULL) prev->InitializeAfterAdvanceUpdateVersion();
        }

        virtual void CheckIsSparse(Version to) {
            return;
        }

        void InvokeIfDirty(const std::function<void(node_t)>& f) {
            this->dirty_flags.InvokeIfMarked(f);
        }

        void Initialize(node_t vid, T val) {
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            if (val) buf_ptr->Set_Atomic(idx);
            else buf_ptr->Clear_Atomic(idx);
        }
        
        void InitializeUnsafe(node_t vid, T val) {
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            if (val) buf_ptr->Set(idx);
            else buf_ptr->Clear(idx);
        }
        
        void Write(node_t vid, T val) {
            int tid = omp_get_thread_num();
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            this->MarkDirty(idx);
            //this->MarkChanged(idx);
            //if ((*buf_ptr)[idx] != val) {
                if (val) {
                    is_set[tid].idx = 1;
                    next_buf_ptr->Set_Atomic(idx);
                } else {
                    next_buf_ptr->Clear_Atomic(idx);
                }
            //}
        }
        
        void WriteUnsafe(node_t vid, T val) {
            LOG_ASSERT(false);
            int tid = omp_get_thread_num();
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            //fprintf(stdout, "active_vertices Write buf[%ld] = %s <-- %s\n", idx, buf[idx] ? "true" : "false", val ? "true" : "false");
            this->MarkDirtyUnsafe(idx);
            //if (*buf_ptr[idx] != val) {
                this->MarkChangedUnsafe(idx);
                if (val) {
                    is_set[tid].idx = 1;
                    next_buf_ptr->Set(idx);
                } else {
                    next_buf_ptr->Clear(idx);
                }
            //}
        }
        
        void SetAll() {
            buf_ptr->SetAll();
            //this->MarkDirtyAll();
            //this->MarkChangedAll();
            //this->MarkToFlushAll();
            //this->MarkDiscardAll();
        }

        // false positive
        bool IsSet() {
            bool ret_val = false;
            for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
                if (is_set[tid].idx == 1) {
                    ret_val = true;
                }
                is_set[tid].idx = 0;
            }
            return ret_val;
        }

        void ConvertToList(GBVersionedBitMap<T, op>& prev) {
            LOG_ASSERT(false);
            /*turbo_timer convert_tim;
            TwoLevelBitMap<int64_t>& buf_prev_ = prev.GetBitMap();
            int64_t cnts1 = 0;
            int64_t cnts2 = 0;
            int64_t buf_list_size = buf.entries_list_.size();
            int64_t buf_prev_list_size = buf_prev_.entries_list_.size();
            if (buf.remain_list_ && buf_prev_.remain_list_ && prev_sparse_mode) {
                convert_tim.start_timer(0);
                buf.SortAndRemoveDuplicatesInList();
                convert_tim.stop_timer(0);
                convert_tim.start_timer(1);
                auto buf_iter_ = buf.entries_list_.begin();
                while (buf_iter_ != buf.entries_list_.end()) {
                    if (!buf.Get(*buf_iter_)) {
                        buf_iter_ = buf.entries_list_.erase(buf_iter_);
                        cnts1++;
                    } else {
                        buf_iter_++;
                    }
                }
                convert_tim.stop_timer(1);
                
                convert_tim.start_timer(2);
                buf_prev_.SortAndRemoveDuplicatesInList();
                convert_tim.stop_timer(2);
                convert_tim.start_timer(3);
                auto buf_prev_iter_ = buf_prev_.entries_list_.begin();
                while (buf_prev_iter_ != buf_prev_.entries_list_.end()) {
                    if (!buf_prev_.Get(*buf_prev_iter_)) {
                        buf_prev_iter_ = buf_prev_.entries_list_.erase(buf_prev_iter_);
                        cnts2++;
                    } else {
                        buf_prev_iter_++;
                    }
                }
                convert_tim.stop_timer(3);
                buf.ConvertToList(false, true);
                buf_prev_.ConvertToList(false, true);
            } else {
                convert_tim.start_timer(4);
                if (buf.remain_list_) buf.entries_list_.clear();
                if (buf_prev_.remain_list_) buf_prev_.entries_list_.clear();
                buf.ConvertToList();
                buf_prev_.ConvertToList();
                convert_tim.stop_timer(4);
            }*/
            //fprintf(stdout, "[%ld] (%d, %d) ConvertToList ActiveVertices %.5f %.5f %.5f %.5f %.5f buf (%ld - %ld), buf_prev = (%ld, - %ld)\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, convert_tim.get_timer(0), convert_tim.get_timer(1), convert_tim.get_timer(2), convert_tim.get_timer(3), convert_tim.get_timer(4), buf_list_size, cnts1, buf_prev_list_size, cnts2);
        }

        void RevertList(GBVersionedBitMap<T, op>& prev, bool step2_processed) {
            LOG_ASSERT(false);
            //TwoLevelBitMap<int64_t>& buf_prev_ = prev.GetBitMap();
            //buf.RevertList(true, false, false, !step2_processed);
            //buf_prev_.RevertList(true, false, false, !step2_processed);
        }
        
        virtual void _SetBufPtr(int superstep_version) { 
            if (!this->construct_next_ss_array_async_) return;
            if (superstep_version <= this->current_version.GetSuperstepVersion()) return;
            int64_t total_ = 0;
            //fprintf(stdout, "%s _SetBufPtr ss = %d: total %ld (cur %d: total %ld) %p, dirty_total = %ld, changed_total[%d] = %ld cnt1 = %ld, cnt2 = %ld\n", this->array_name.c_str(), superstep_version, buf[superstep_version]->GetTotal(), this->current_version.GetSuperstepVersion(), buf[this->current_version.GetSuperstepVersion()]->GetTotal(), buf[superstep_version], this->dirty_flags.GetTotal(), this->changed_toggle_, this->changed_flags[this->changed_toggle_].GetTotal(), cnt1, cnt2); 
            cnt1 = 0;
            cnt2 = 0;
            //this->changed_flags.ClearAll();
            //buf_ptr = buf[UserArguments::SUPERSTEP + 1]; 
            char prev_toggle = (this->changed_toggle_ + 1) % 2;
            if (UserArguments::UPDATE_VERSION >= 1) {
                this->dirty_flags.InvokeIfMarked([&](node_t idx) {
                //this->changed_flags[this->changed_toggle_].InvokeIfMarked([&](node_t idx) {
                    //if (!this->changed_flags[this->changed_toggle_].Get_Atomic(idx)) {
                    //if (!this->dirty_flags.Get_Atomic(idx)) {
                        //if (UserArguments::SUPERSTEP == 1)
                        //    fprintf(stdout, "%s next_buf_ptr[%ld] %ld <- buf_ptr[%ld] %ld\n", this->array_name.c_str(), idx, (int64_t) next_buf_ptr[idx], idx, (int64_t) buf_ptr[idx]);
                        if (buf_ptr->Get(idx)) {
                            //next_buf_ptr->Set_Atomic(idx);
                        }
                        else next_buf_ptr->Clear_Atomic(idx);
                        //next_buf_ptr[idx] = buf_ptr[idx];
                        //std::atomic_fetch_add((std::atomic<int64_t>*) &total_, +1L);
                    //}
                });
                if (prev != NULL && superstep_version >= 1) {
                    std::vector<TwoLevelBitMap<int64_t>*>& prev_buf = prev->GetBuf();
                    TwoLevelBitMap<int64_t>* target_prev_buf = prev_buf[superstep_version - 1];
                    //fprintf(stdout, "%s copy superstep %d at version (%d, %d)\n", this->array_name.c_str(), superstep_version - 1, UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
                    target_prev_buf->CopyFrom(*buf[superstep_version - 1]);
                    //this->changed_flags[prev_toggle].InvokeIfMarked([&](node_t idx) {
                    //    if (buf[superstep_version - 1]->Get(idx)) target_prev_buf->Set_Atomic(idx);
                    //    else target_prev_buf->Clear_Atomic(idx);
                    //});
#ifndef PERFORMANCE
                    for (int64_t idx = 0; idx < this->buf_length; idx++) {
                        if (target_prev_buf->Get(idx) != buf[superstep_version - 1]->Get(idx)) {
                            fprintf(stdout, "ss %d prev_buf[%ld] = %s != buf %s\n", superstep_version - 1, idx, target_prev_buf->Get(idx) ? "true" : "false", buf[superstep_version - 1]->Get(idx) ? "true" : "false");
                            LOG_ASSERT(false);
                        }
                    }
#endif
                }
            }
            //this->changed_flags[this->changed_toggle_].ClearAll();
            //this->changed_flags[prev_toggle].ClearAll();
            buf_ptr = buf[superstep_version]; 
            //this->changed_toggle_ = (this->changed_toggle_ + 1) % 2;
            if (prev != NULL) {
                prev->_SetBufPtrInternal(superstep_version);
            }
        }
        void _SetBufPtrInternal(int superstep_version) { 
            //fprintf(stdout, "%s prev _SetBufPtr ss ver = %d (cur %d) %p\n", this->array_name.c_str(), superstep_version, this->current_version.GetSuperstepVersion(), buf_ptr); 
            buf_ptr = buf[superstep_version]; 
        }

        TwoLevelBitMap<int64_t>& data() { return *buf_ptr; }

        bool Read(node_t vid) { 
            ALWAYS_ASSERT (vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id());
            return buf_ptr->Get(vid - PartitionStatistics::my_first_internal_vid()); 
        }

        virtual T GetValue(int64_t idx) {
            return buf_ptr->Get(idx);
        }
        virtual T* GetBuffer() {
            INVARIANT(false);
            return nullptr;
        }
        virtual T* GetPrevBuffer() {
            INVARIANT(false);
            return nullptr;
        }
        
        TwoLevelBitMap<int64_t>& GetBitMap() { return *buf_ptr; }
        std::vector<TwoLevelBitMap<int64_t>*>& GetBuf() { return buf; }
        GBVersionedArrayBase<T, op>* GetRef() { return dynamic_cast<GBVersionedArrayBase<T, op>*>(ref); }
        TwoLevelBitMap<int64_t>& GetData(GBVersionedArrayBase<T, op>* va) {
            GBVersionedBitMap<T, op>* va_temp = dynamic_cast<GBVersionedBitMap<T, op>*>(va);
            return va_temp->data();
        }
        
        void ClearAll() {
            buf_ptr->ClearAll();
            ClearFlags();
        }

        void ClearFlags() {
            if (ref != NULL) return;
            this->dirty_flags.ClearAll();
            this->changed_flags[0].ClearAll();
            this->changed_flags[1].ClearAll();
            this->to_flush_flags.ClearAll();
        }

    private:
        //bool is_set;
        PaddedIdx* is_set; 
        bool sparse_mode;
        bool prev_sparse_mode;
        GBVersionedBitMap<T, op>* ref;
        GBVersionedBitMap<T, op>* prev;

        std::vector<TwoLevelBitMap<int64_t>*> buf;
        TwoLevelBitMap<int64_t>* buf_ptr;
        TwoLevelBitMap<int64_t>* next_buf_ptr;
 
        int64_t cur_read_idx, cur_read_size;
        int64_t prev_read_idx, prev_read_size;

        int64_t cnt1, cnt2;
        bool pre_allocated;
};


#endif
