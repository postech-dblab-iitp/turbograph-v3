#pragma once
#ifndef VERSIONED_BITMAP_H
#define VERSIONED_BITMAP_H

#include "VersionedArrayBase.hpp"
#include "MemoryMappedBitMap.hpp"

//#define PER_THREAD_BUFFER_SIZE (32*1024)
//#define FLOAT_PRECISION_TOLERANCE 0.02

#define Abs(x)    ((x) < 0 ? -(x) : (x))
#define Max(a, b) ((a) > (b) ? (a) : (b))

template <typename T, Op op>
class VersionedBitMap : public VersionedArrayBase<T, op> {

    public:

        VersionedBitMap() : ref(NULL) {}
        
        ~VersionedBitMap() {}
        
        virtual bool IsCreated() {
            return true;
        }
        
        virtual void InitReadOnly(VersionedArrayBase<T, op>& o, bool maintain_prev_ss=false) {
			this->buf_length = o.GetBufLength();
            this->array_path = o.GetArrayPath();

            this->buf_memory_usage = BitMap<int64_t>::compute_container_size(this->buf_length);
            this->bitmap_memory_usage = 0;

            this->current_version.SetUpdateVersion(0);
            this->current_version.SetSuperstepVersion(-1);

            VersionedArrayBase<T, op>* o_temp = &o;
            ref = dynamic_cast<VersionedBitMap<T, op>*>(o_temp);
            INVARIANT(ref != NULL);
        }
        
        virtual void CreateReadOnly() {
            buf.Init(this->buf_length);
            LocalStatistics::register_mem_alloc_info((this->array_name + " VersionedArray(ReadOnly)").c_str(), this->buf_memory_usage / (1024 * 1024));
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
            buf.Init(this->buf_length);
            buf_next.Init(this->buf_length);
            sparse_mode = false;
            prev_sparse_mode = false;
            posix_memalign((void**) &is_set, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
            for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
                is_set[tid].idx = 0;
            }
        }
         
        virtual int64_t FreeMainAndBuffer(bool rm) {
            this->buf_length = 0;
            LocalStatistics::register_mem_alloc_info((this->array_path + " VersionedBitMap").c_str(), 0);
            
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                if (this->ptr_to_per_thread_buffer[i] == NULL) continue;
                char* data = (char*) this->ptr_to_per_thread_buffer[i];
                RequestRespond::ReturnPerThreadBuffer(data);
            }


            return -1; // TODO - return something?
        }

        virtual void CopyBuffer(VersionedArrayBase<T, op>* prev_) {

        }
        
        virtual void CopyMainBuffer() {
            if (this->use_main_) {
                //memcpy((void*) buf_.data(), (void*) main.data(), main.container_size());
            }
        }
        
        virtual void CopyMainBufferFromRef(VersionedArrayBase<T, op>* prev) {
            // TODO
            TwoLevelBitMap<int64_t>& buf_ = GetData(prev);
            if (this->use_main_) {
                //memcpy((void*) buf_.data(), (void*) main.data(), main.container_size());
            }
        }
        
        // IV
        virtual void MergeWithNext(Version from, Version to, VersionedArrayBase<T, op>* prev) {
            //fprintf(stdout, "MergeWithNext %s |marked_next| = %ld |marked_next_first| = %ld\n", this->array_name.c_str(), this->next_flags[this->toggle_].GetTotal(), this->first_snapshot_delta_flags.GetTotal());
            TwoLevelBitMap<int64_t>& buf_prev_ = GetData(prev);
            int64_t cnts1 = 0;
            int64_t cnts2 = 0;
            int64_t cnts3 = 0;
            int64_t cnts4 = 0;
    
#ifdef HYBRID_COST_MODEL
            turbo_timer temp_tim;
            temp_tim.start_timer(0);
            temp_tim.start_timer(1);
            this->costs[0] = this->Compute_B();
            this->costs[1] = this->costs[0];
            this->costs[2] = this->Compute_C();
            temp_tim.stop_timer(1);
            temp_tim.start_timer(2);
            if (this->costs[2] == 0) 
                this->costs[4] = 0;
            else 
                this->costs[4] = this->next_flags[this->toggle_].GetTotalEstimation();
            temp_tim.stop_timer(2);

            temp_tim.start_timer(3);
            //fprintf(stdout, "[%ld] next total (lv1: %ld, lv2: %ld), first total (lv1: %ld, lv2: %ld), union total (lv1: %ld)\n", PartitionStatistics::my_machine_id(), this->next_flags[this->toggle_].GetTotal(), this->next_flags[this->toggle_].GetTotalOfLv2(), this->first_snapshot_delta_flags[this->toggle_].GetTotal(), this->first_snapshot_delta_flags[this->toggle_].GetTotalOfLv2(), TwoLevelBitMap<int64_t>::GetTotalOfUnion(this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]));
            if ((this->costs[0] + this->costs[2]) != 0) {
            //if (true) {
                if (buf.remain_list_ && buf_prev_.remain_list_ && sparse_mode) {
                    temp_tim.start_timer(5);
                    this->next_flags[this->toggle_].SortAndRemoveDuplicatesInList();
                    this->first_snapshot_delta_flags[this->toggle_].SortAndRemoveDuplicatesInList();
                    TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                        int tid = omp_get_thread_num();
                        if (!checkEquality(buf_prev_[idx], buf_next[idx])) {
                            if (buf_next[idx]) {
                                buf_prev_.Set(idx);
                                buf_prev_.PushIntoList(idx);
                            } else {
                                buf_prev_.Clear(idx);
                            }
                        }
                        if (!checkEquality(buf[idx], buf_next[idx])) {
                            this->discard_flags.Set(idx);
                            if (!this->dirty_flags.Get(idx)) {
                                if (buf_next[idx]) {
                                    is_set[tid].idx = 1;
                                    buf.Set(idx);
                                    buf.PushIntoList(idx);
                                } else {
                                    buf.Clear(idx);
                                }
                            } else {
                                this->to_flush_flags.Set(idx);
                            }
                        } else {
                            this->to_flush_flags.Clear(idx);
                        }
                    }, this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]);
                    temp_tim.stop_timer(5);
                } else {
                    temp_tim.start_timer(6);
                    TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                        if (!checkEquality(buf_prev_[idx], buf_next[idx])) {
                            if (buf_next[idx]) buf_prev_.Set(idx);
                            else buf_prev_.Clear(idx);
                        }
                    }, this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]);
                    temp_tim.stop_timer(6);
                    temp_tim.start_timer(7);
                    //int64_t temp_cnt[UserArguments::NUM_THREADS];
                    //for (int i = 0; i < UserArguments::NUM_THREADS; i++) temp_cnt[i] = 0;
                    TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                        int tid = omp_get_thread_num();
                        //temp_cnt[tid]++;
                        //if (!checkEquality(buf[idx], buf_next[idx])) {
                        if (!checkEquality(buf[idx], buf_next[idx])) {
                            this->discard_flags.Set(idx);
                            if (!this->dirty_flags.Get(idx)) {
                                if (buf_next[idx]) {
                                    //std::atomic_fetch_add((std::atomic<int64_t>*) &cnts1, +1L);
                                    is_set[tid].idx = 1;
                                    buf.Set(idx);
                                } else {
                                    //std::atomic_fetch_add((std::atomic<int64_t>*) &cnts2, +1L);
                                    buf.Clear(idx);
                                }
                            } else {
                                //std::atomic_fetch_add((std::atomic<int64_t>*) &cnts3, +1L);
                                this->to_flush_flags.Set(idx);
                            }
                        } else {
                            //std::atomic_fetch_add((std::atomic<int64_t>*) &cnts4, +1L);
                            this->to_flush_flags.Clear(idx);
                        }
                    }, this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]);
                    temp_tim.stop_timer(7);
                    //for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                    //    fprintf(stdout, "tid %d, cnt = %ld\n", i, temp_cnt[i]);
                    //}
                }
            }
            temp_tim.stop_timer(3);
            
            temp_tim.start_timer(4);
            this->costs[3] = this->to_flush_flags.GetTotalEstimation();
            temp_tim.stop_timer(4);
            temp_tim.stop_timer(0);
            //fprintf(stdout, "[%ld] %s IV MergeWithNext %.4f %.4f %.4f %.4f %.4f, next total %ld, first total %ld\n", PartitionStatistics::my_machine_id(), this->array_name.c_str(), temp_tim.get_timer(0), temp_tim.get_timer(1), temp_tim.get_timer(2), temp_tim.get_timer(3), temp_tim.get_timer(4), this->next_flags[this->toggle_].GetTotal(), this->first_snapshot_delta_flags[this->toggle_].GetTotal());
//#ifdef PRINT_PROFILING_TIMERS
            fprintf(stdout, "[%ld] %s IV MergeWithNext cnts (%ld, %ld, %ld, %ld) %.4f %.4f %.4f %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), this->array_name.c_str(), cnts1, cnts2, cnts3, cnts4, temp_tim.get_timer(0), temp_tim.get_timer(1), temp_tim.get_timer(2), temp_tim.get_timer(3), temp_tim.get_timer(4), temp_tim.get_timer(5), temp_tim.get_timer(6), temp_tim.get_timer(7));
//#endif
#else
            this->next_flags[this->toggle_].InvokeIfMarked([&](node_t idx) {
                if (!checkEquality(buf_prev_[idx], buf_next[idx])) {
                    if (buf_next[idx]) buf_prev_.Set(idx);
                    else buf_prev_.Clear(idx);
                }
                //fprintf(stdout, "\t[%s][IV MergeWithNext] buf[%ld] = %s <-- buf_next[%ld] = %s dirty flag = %s\n", this->array_name.c_str(), idx, buf[idx] ? "true" : "false", idx, buf_next[idx] ? "true" : "false", this->dirty_flags.Get(idx) ? "true" : "false");
                if (!checkEquality(buf[idx], buf_next[idx])) {
                    if (!this->dirty_flags.Get(idx)) {
                        if (buf_next[idx]) buf.Set(idx);
                        else buf.Clear(idx);
                    } else {
                        this->to_flush_flags.Set(idx);
                    }
                } else {
                    this->to_flush_flags.Clear(idx);
                }
            }, false, UserArguments::NUM_TOTAL_CPU_CORES);

#endif
        }
        
        virtual void MergeWithNext(Version from, Version to) {
            LOG_ASSERT(false); // by syko. 2019/10/30
        }
        
        static void Callback_ReadUpdatesAndConstructWithoutFlush_(diskaio::DiskAioRequest* req) {
            diskaio::DiskAioRequestUserInfo& info = req->user_info;
            VersionedBitMap<T, op>* caller = (VersionedBitMap<T, op>*) info.caller;
            char* data = (char*) req->GetBuf();
            size_t num_entries = req->GetIoSize();
            DeltaMessage<T>* updates = (DeltaMessage<T>*) data;
            size_t num_updates = num_entries / sizeof(DeltaMessage<T>);
            
            TwoLevelBitMap<int64_t>* buf = (TwoLevelBitMap<int64_t>*) req->user_info.buf_to_construct;
            TwoLevelBitMap<int64_t>& next_flags = (caller->next_flags[caller->toggle_]);
            bool construct_next = req->user_info.construct_next;

            // XXX - who put this 'omp parallel for'? tslee?...
//#pragma omp parallel for
            for (int64_t idx = 0; idx < num_updates; idx++) {
                DeltaMessage<T> upd = updates[idx];
                if (upd.dst_vid == -1) continue;
#ifdef INCREMENTAL_LOGGING
                if (INCREMENTAL_DEBUGGING_TARGET(upd.dst_vid)) {
                    fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] buf_prev_[%ld] = %ld <-- %ld / %ld-th msg among %ld construct_next ? %s\n", caller->array_name.c_str(), upd.dst_vid, buf->Get(upd.dst_vid), upd.message, idx, num_updates, construct_next ? "true" : "false");
                }
#endif
                if (upd.message) buf->Set(upd.dst_vid);
                else buf->Clear(upd.dst_vid);
                if (construct_next) next_flags.Set(upd.dst_vid);
            }
            //fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] |marked_next| = %ld\n", caller->array_name.c_str(), next_flags.GetTotal());
            
            RequestRespond::ReturnPerThreadBuffer(data);
        }
        
        static void Callback_ReadUpdatesAndConstructWithoutFlush_ListVersion_(diskaio::DiskAioRequest* req) {
            diskaio::DiskAioRequestUserInfo& info = req->user_info;
            VersionedBitMap<T, op>* caller = (VersionedBitMap<T, op>*) info.caller;
            char* data = (char*) req->GetBuf();
            size_t num_entries = req->GetIoSize();
            DeltaMessage<T>* updates = (DeltaMessage<T>*) data;
            size_t num_updates = num_entries / sizeof(DeltaMessage<T>);
            
            TwoLevelBitMap<int64_t>* buf = (TwoLevelBitMap<int64_t>*) req->user_info.buf_to_construct;
            TwoLevelBitMap<int64_t>& next_flags = (caller->next_flags[caller->toggle_]);
            bool construct_next = req->user_info.construct_next;
            INVARIANT(next_flags.is_list_flag_);

            for (int64_t idx = 0; idx < num_updates; idx++) {
                DeltaMessage<T> upd = updates[idx];
                if (upd.dst_vid == -1) continue;
                if (upd.message) buf->Set(upd.dst_vid);
                else buf->Clear(upd.dst_vid);
                if (construct_next) next_flags.PushIntoList(upd.dst_vid);
            }
            RequestRespond::ReturnPerThreadBuffer(data);
        }

        virtual void ReadUpdatesAndConstructWithoutFlush(int i, int j, bool construct_next, bool construct_prev, bool mark_changed, VersionedArrayBase<T, op>* prev_) {
            // Four cases {construct_next} X {construct_prev}
            // (1) (true , true ): Not Supported
            // (2) (true , false): Construct Next Buffer
            // (3) (false, true ): Construct Previous Buffer
            // (4) (false, false): Construct Current Buffer
            INVARIANT(!construct_next || !construct_prev);
            
            TwoLevelBitMap<int64_t>* buf_;
            
            if (construct_next) buf_ = &buf_next;
            else if (construct_prev) buf_ = &(GetData(prev_));
            else buf_ = &buf;
            
            ReadUpdatesAndConstructWithoutFlush_(i, j, buf_, construct_next);
        }
        
        void ReadUpdatesAndConstructWithoutFlush_(int i, int j, TwoLevelBitMap<int64_t>* buf_, bool construct_next) {
            if (sparse_mode) {
                this->ReadDeltasAndProcessUsingDiskAio(i, j,
                    sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE,
                    this, &Callback_ReadUpdatesAndConstructWithoutFlush_ListVersion_,
                    (void*) buf_, construct_next);
            } else {
                this->ReadDeltasAndProcessUsingDiskAio(i, j,
                    sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE,
                    this, &Callback_ReadUpdatesAndConstructWithoutFlush_,
                    (void*) buf_, construct_next);
            }
        }
        
        virtual void InitializeAfterAdvanceUpdateVersion() {
            //sparse_mode = false;
            //prev_sparse_mode = false;
            //this->RevertNextFlagsAll();
        }

        virtual void CheckIsSparse(Version to) {
#ifndef OPTIMIZE_SPARSE_COMPUTATION
            return; // XXX sparse mode activation condition should be changed.. sometime it consumes a lot of memory
#endif
            return; 
            int64_t delta_size = this->Compute_B(to) + this->Compute_C(to);
            if (delta_size > (UserArguments::NUM_THREADS * (to.GetUpdateVersion() + 1) * (4096 / sizeof(DeltaMessage<T>)))) {
                prev_sparse_mode = sparse_mode;
                sparse_mode = false;
                this->RevertNextFlags();
            } else {
                prev_sparse_mode = sparse_mode;
                sparse_mode = true;
                this->ConvertNextFlagsToList();
            }
            fprintf(stdout, "[%ld] CheckIsSparse to.GetUpd = %d, delta_size = %ld, threshold = %ld, sparse_mode = %s, prev_sparse_mode = %s\n", PartitionStatistics::my_machine_id(), to.GetUpdateVersion(), delta_size, ((to.GetUpdateVersion() + 1)* (4096 / sizeof(DeltaMessage<T>))), sparse_mode ? "true" : "false", prev_sparse_mode ? "true" : "false");
        }

        void InvokeIfDirty(const std::function<void(node_t)>& f) {
            this->dirty_flags.InvokeIfMarked(f);
        }
        

        void Initialize(node_t vid, T val) {
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            if (val) buf.Set_Atomic(idx);
            else buf.Clear_Atomic(idx);
        }
        
        void InitializeUnsafe(node_t vid, T val) {
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            if (val) buf.Set(idx);
            else buf.Clear(idx);
        }
        
        void Write(node_t vid, T val) {
            int tid = omp_get_thread_num();
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            this->MarkDirty(idx);
            if (buf[idx] != val) {
                this->MarkChanged(idx);
                this->MarkToFlush(idx);
                this->MarkDiscard(idx);
                if (val) {
                    is_set[tid].idx = 1;
                    buf.Set_Atomic(idx);
                } else {
                    buf.Clear_Atomic(idx);
                }
            }
        }
        
        void WriteUnsafe(node_t vid, T val) {
            int tid = omp_get_thread_num();
            node_t idx = vid - PartitionStatistics::my_first_internal_vid();
            //fprintf(stdout, "active_vertices Write buf[%ld] = %s <-- %s\n", idx, buf[idx] ? "true" : "false", val ? "true" : "false");
            this->MarkDirtyUnsafe(idx);
            if (buf[idx] != val) {
                this->MarkChangedUnsafe(idx);
                this->MarkToFlushUnsafe(idx);
                this->MarkDiscardUnsafe(idx);
                if (val) {
                    is_set[tid].idx = 1;
                    buf.Set(idx);
                } else {
                    buf.Clear(idx);
                }
            }
        }
        
        void SetAll() {
            buf.SetAll();
            this->MarkDirtyAll();
            this->MarkChangedAll();
            this->MarkToFlushAll();
            this->MarkDiscardAll();
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

        void ConvertToList(VersionedBitMap<T, op>& prev) {
            turbo_timer convert_tim;
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
            }
            //fprintf(stdout, "[%ld] (%d, %d) ConvertToList ActiveVertices %.5f %.5f %.5f %.5f %.5f buf (%ld - %ld), buf_prev = (%ld, - %ld)\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, convert_tim.get_timer(0), convert_tim.get_timer(1), convert_tim.get_timer(2), convert_tim.get_timer(3), convert_tim.get_timer(4), buf_list_size, cnts1, buf_prev_list_size, cnts2);
        }

        void RevertList(VersionedBitMap<T, op>& prev, bool step2_processed) {
            TwoLevelBitMap<int64_t>& buf_prev_ = prev.GetBitMap();
            buf.RevertList(true, false, false, !step2_processed);
            buf_prev_.RevertList(true, false, false, !step2_processed);
        }

        TwoLevelBitMap<int64_t>& data() { return buf; }

        bool Read(node_t vid) { 
            ALWAYS_ASSERT (vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id());
            return buf.Get(vid - PartitionStatistics::my_first_internal_vid()); 
        }

        virtual T GetValue(int64_t idx) {
            return buf.Get(idx);
        }
        virtual T* GetBuffer() {
            INVARIANT(false);
            return nullptr;
        }
        virtual T* GetPrevBuffer() {
            INVARIANT(false);
            return nullptr;
        }
        
        TwoLevelBitMap<int64_t>& GetBitMap() { return buf; }
        VersionedArrayBase<T, op>* GetRef() { return dynamic_cast<VersionedArrayBase<T, op>*>(ref); }
        TwoLevelBitMap<int64_t>& GetData(VersionedArrayBase<T, op>* va) {
            VersionedBitMap<T, op>* va_temp = dynamic_cast<VersionedBitMap<T, op>*>(va);
            return va_temp->data();
        }
        
        void ClearAll() {
            buf.ClearAll();
            ClearFlags();
        }

        void ClearFlags() {
            if (ref != NULL) return;
            this->dirty_flags.ClearAll();
            this->changed_flags.ClearAll();
            this->to_flush_flags.ClearAll();
        }

    private:
        //bool is_set;
        PaddedIdx* is_set; 
        bool sparse_mode;
        bool prev_sparse_mode;
        VersionedBitMap<T, op>* ref;

        TwoLevelBitMap<int64_t> buf;
        TwoLevelBitMap<int64_t> buf_next;
 
        int64_t cur_read_idx, cur_read_size;
        int64_t prev_read_idx, prev_read_size;
};


#endif
