#pragma once
#ifndef GB_VERSIONED_ARRAY_H
#define GB_VERSIONED_ARRAY_H

#include "GBVersionedArrayBase.hpp"

//#define PER_THREAD_BUFFER_SIZE (32*1024)
//#define FLOAT_PRECISION_TOLERANCE 0.02

template <typename T, Op op>
class GBVersionedArray : public GBVersionedArrayBase<T, op> {

    public:
        GBVersionedArray() : ref(NULL),  prev(NULL) {}

        ~GBVersionedArray() {}

        virtual bool IsCreated() {
            return (buf.size() != 0);
        }
        
        virtual void InitReadOnly(GBVersionedArrayBase<T, op>& o, bool maintain_prev_ss=false) {
            GBVersionedArrayBase<T, op>* o_temp = &o;
            ref = dynamic_cast<GBVersionedArray<T, op>*>(o_temp);
            INVARIANT(ref != NULL);
            this->buf_memory_usage = 0;
            this->bitmap_memory_usage = 0;

			this->buf_length = o.GetBufLength();
            this->array_path = o.GetArrayPath();
            this->array_name = o.GetArrayName();
            this->maintain_prev_ss_ = maintain_prev_ss;
            this->buf_memory_usage += sizeof(T) * this->buf_length;
            this->use_main_ = o.use_main_;
            if (this->maintain_prev_ss_) this->buf_memory_usage += sizeof(T) * this->buf_length;
            this->maintain_prev_ss_flags.Init(this->buf_length);
            this->bitmap_memory_usage += BitMap<int64_t>::compute_container_size(this->buf_length);
            
            this->current_version.SetUpdateVersion(0);
            this->current_version.SetSuperstepVersion(-1);
        }

        virtual void CreateReadOnly() {
            if (UserArguments::NUM_ITERATIONS != INT_MAX) {
            //if (false) {
                buf.resize(UserArguments::NUM_ITERATIONS + 2);
                for (int i = 0; i < UserArguments::NUM_ITERATIONS + 2; i++) {
                    buf[i] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                    for (int64_t j = 0; j < this->buf_length; j++)
                        buf[i][j] = idenelem<T>(op);
                }
                buf_ptr = buf[0];
                next_buf_ptr = buf[0];
                pre_allocated = true;
            } else {
                buf.resize(1);
                buf[0] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
                //buf[1] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
                buf_ptr = buf[0];
                next_buf_ptr = buf[0];

            }
            if (this->use_main_) {
                buf_orig = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_orig[i] = idenelem<T>(op);
                buf_ptr = buf_orig;
                next_buf_ptr = buf_orig;
            }
        }

        void SetPrev(GBVersionedArray<T, op>& prev_) {
            prev = &prev_;
            is_prev_exist = true;
        }

        virtual void InitializePerThreadBuffers() {
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
            // XXX cannot compute exactly
            this->buf_memory_usage = 0;
            this->bitmap_memory_usage = 0;

            // Compute memory usage for Main & Buffers
            if (this->use_main_) this->buf_memory_usage += (sizeof(T) * this->buf_length);
            this->buf_memory_usage += (sizeof(T) * this->buf_length); // Basic buffer
            if (this->construct_next_ss_array_async_) this->buf_memory_usage += (sizeof(T) * this->buf_length); // Next buffer
            if (this->maintain_prev_ss_) this->buf_memory_usage += (sizeof(T) * this->buf_length);
            
            // Compute memory usage for Bitmaps
            this->bitmap_memory_usage += (5 * BitMap<int64_t>::compute_container_size(this->buf_length));
            if (this->maintain_prev_ss_) this->bitmap_memory_usage += BitMap<int64_t>::compute_container_size(this->buf_length);
            if (this->advance_superstep_ && this->construct_next_ss_array_async_) this->bitmap_memory_usage += (4 * BitMap<int64_t>::compute_container_size(this->buf_length));
        }

        virtual void InitializeMainAndBuffer(std::string array_path, bool open) {
            std::string main_array_path = array_path + "_" + "0";
            if (UserArguments::NUM_ITERATIONS != INT_MAX) {
            //if (false) {
                buf.resize(UserArguments::NUM_ITERATIONS + 2);
                for (int i = 0; i < UserArguments::NUM_ITERATIONS + 2; i++) {
                    buf[i] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                    for (int64_t j = 0; j < this->buf_length; j++)
                        buf[i][j] = idenelem<T>(op);
                }
                buf_ptr = buf[0];
                next_buf_ptr = buf[0];
                pre_allocated = true;
            } else {
                buf.resize(1);
                buf[0] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf[0][i] = idenelem<T>(op);
                buf_ptr = buf[0];
                next_buf_ptr = buf[0];
            }

            if (this->use_main_) {
                //LOG_ASSERT(false); // Maybe not use..
                if (open) this->main.Open(main_array_path.c_str(), true);
                else this->main.Create(main_array_path.c_str(), this->buf_length);

                //this->main.Mlock(false);
                memset((char*) this->main.data(), 0, sizeof(T) * this->buf_length);
                
                buf_orig = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_orig[i] = idenelem<T>(op);
                if (this->construct_next_ss_array_async_) {
                    buf_next_orig = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                    for (int64_t i = 0; i < this->buf_length; i++)
                        buf_next_orig[i] = idenelem<T>(op);
                }
                buf_ptr = buf_orig;
                next_buf_ptr = buf_orig;
            }
        }
        
        virtual int64_t FreeMainAndBuffer(bool rm) {
            for (int64_t i = 0; i < buf.size(); i++) {
                if (buf[i] != NULL) 
                    NumaHelper::free_numa_memory(buf[i], sizeof(T) * this->buf_length);
                buf[i] = NULL;
            }
            if (GetRef() != NULL) return 0;

            if (this->use_main_) {
                this->main.Munlock();
                this->main.Close(true);
                if (buf_orig != NULL) NumaHelper::free_numa_memory(buf_orig, sizeof(T) * this->buf_length);
            }
            LocalStatistics::register_mem_alloc_info((this->array_name + " GBVersionedArray").c_str(), 0);
            
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                if (this->ptr_to_per_thread_buffer[i] == NULL) continue;
                char* data = (char*) this->ptr_to_per_thread_buffer[i];
                this->ptr_to_per_thread_buffer[i] = NULL;
                RequestRespond::ReturnPerThreadBuffer(data);
            }
            
            return 2 * sizeof(T) * this->buf_length;
        }

        virtual void CopyBuffer(GBVersionedArrayBase<T, op>* prev_) {
            T* buf_prev_ = GetData(prev_);
            memcpy((void*) buf_orig, (void*) buf_prev_, sizeof(T) * this->buf_length);
        }

        virtual void CopyMainBuffer() {
            if (this->use_main_) {
                memcpy((void*) buf_orig, (void*) this->main.data(), sizeof(T) * this->main.length());
            }
        }
        
        virtual void CopyMainBufferFromRef(GBVersionedArrayBase<T, op>* prev_) {
            if (this->use_main_) {
                T* buf_ = GetData(prev_);
                memcpy((void*) buf_, (void*) this->main.data(), sizeof(T) * this->main.length());
            }
        }

        virtual void CopyPrevSSBuffer() {
        }

        void PrepareNextSSBuffer() {
            if (this->use_main_) return;
            if (!this->construct_next_ss_array_async_) return;
            //fprintf(stdout, "%s PrepareNextSSBuffer at (%d, %d)\n", this->array_name.c_str(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
            if (UserArguments::UPDATE_VERSION == 0) {
                if (!pre_allocated) {
                    INVARIANT(buf.size() == UserArguments::SUPERSTEP + 1);
                    buf.push_back(nullptr);
                    buf[UserArguments::SUPERSTEP + 1] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
                    next_buf_ptr = buf[UserArguments::SUPERSTEP + 1];
                    if (!this->is_ov_) {
                        memcpy((void*) buf[UserArguments::SUPERSTEP + 1], (void*) buf[UserArguments::SUPERSTEP], sizeof(T) * this->buf_length);
                    } else {
#pragma omp parallel for
                        for (int64_t i = 0; i < this->buf_length; i++)
                            buf[UserArguments::SUPERSTEP + 1][i] = idenelem<T>(op);
                        //if (this->is_ov_)
                        //    fprintf(stdout, "%s buf_ptr[%ld] = %ld, %p\n", this->array_name.c_str(), 6954354, buf_ptr[6954354], buf_ptr);
                        buf_ptr = buf[UserArguments::SUPERSTEP + 1];
                        //fprintf(stdout, "%s setbuffptr ss %d, %p\n", this->array_name.c_str(), UserArguments::SUPERSTEP + 1, buf[UserArguments::SUPERSTEP + 1]);
                    }
                } else {
                    next_buf_ptr = buf[UserArguments::SUPERSTEP + 1];
                    if (!this->is_ov_) {
#pragma omp parallel for
                        for (int64_t idx = 0; idx < this->buf_length; idx++) {
                            buf[UserArguments::SUPERSTEP + 1][idx] = buf[UserArguments::SUPERSTEP][idx];
                        }
                        //memcpy((void*) buf[UserArguments::SUPERSTEP + 1], (void*) buf[UserArguments::SUPERSTEP], sizeof(T) * this->buf_length);
                    } else {
                        buf_ptr = buf[UserArguments::SUPERSTEP + 1];
                    }
                }
            } else if (UserArguments::UPDATE_VERSION >= 1) {
                if (this->is_ov_) buf_ptr = buf[UserArguments::SUPERSTEP + 1];
                next_buf_ptr = buf[UserArguments::SUPERSTEP + 1];
            }
            if (prev != NULL) {
                std::vector<T*>& prev_buf = prev->GetBuf();
                if (UserArguments::UPDATE_VERSION == 0) {
                    if (!pre_allocated) {
                        INVARIANT(prev_buf.size() == UserArguments::SUPERSTEP + 1);
                        prev_buf.push_back(nullptr);
                        prev_buf[UserArguments::SUPERSTEP + 1] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
                        //fprintf(stdout, "memcpy %p <- %p\n", prev_buf[UserArguments::SUPERSTEP], buf[UserArguments::SUPERSTEP]);
                        memcpy((void*) prev_buf[UserArguments::SUPERSTEP], (void*) buf[UserArguments::SUPERSTEP], sizeof(T) * this->buf_length);
                        //for (int64_t idx = 0; idx < 10; idx++)
                        //    fprintf(stdout, "%s prev_buf[%d][%ld] = %ld\n", this->array_name.c_str(), UserArguments::SUPERSTEP, idx, prev_buf[UserArguments::SUPERSTEP][idx]);
                    } else {
#pragma omp parallel for
                        for (int64_t idx = 0; idx < this->buf_length; idx++) {
                            prev_buf[UserArguments::SUPERSTEP][idx] = buf[UserArguments::SUPERSTEP][idx];
                        }
                    }
                } else if (UserArguments::UPDATE_VERSION >= 1) {
                }
            }
        }

        virtual int64_t GetNumberOfSuperstepVersions(int update_version) {
            //if (update_version == 0) return buf.size();
            if (update_version == UserArguments::UPDATE_VERSION - 1) return buf.size();
            else return UserArguments::SUPERSTEP;
        }

        // IV
        virtual void MergeWithNext(Version from, Version to, GBVersionedArrayBase<T, op>* prev_) {
            return;
        }
       
        // OV
        virtual void MergeWithNext(Version from, Version to) {
            return;
        }

        static void Callback_ReadUpdatesAndConstructWithoutFlush_(diskaio::DiskAioRequest* req) {
            diskaio::DiskAioRequestUserInfo& info = req->user_info;
            VersionedArray<T, op>* caller = (VersionedArray<T, op>*) info.caller;
            char* data = (char*) req->GetBuf();
            size_t num_entries = req->GetIoSize();

            DeltaMessage<T>* updates = (DeltaMessage<T>*) data;
            size_t num_updates = num_entries / sizeof(DeltaMessage<T>);

            T* buf = (T*) req->user_info.buf_to_construct;
            //TwoLevelBitMap<int64_t>& next_flags = (caller->next_flags[caller->toggle_]);
            TwoLevelBitMap<int64_t>& changed_flags = (caller->changed_flags);
            bool construct_next = req->user_info.construct_next;
            bool mark_changed = req->user_info.mark_changed;
            LOG_ASSERT(!construct_next);

            for (int64_t idx = 0; idx < num_updates; idx++) {
                DeltaMessage<T> upd = updates[idx];
                if (upd.dst_vid == -1) continue;
#ifdef INCREMENTAL_LOGGING
                if (INCREMENTAL_DEBUGGING_TARGET(PartitionStatistics::my_first_node_id() + upd.dst_vid)) {
                    fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] buf[%ld] = %s <-- %s / construct_next ? %s\n", caller->array_name.c_str(), PartitionStatistics::my_first_node_id() + upd.dst_vid, std::to_string(buf[upd.dst_vid]).c_str(), std::to_string(upd.message).c_str(), construct_next ? "true" : "false");
                }
#endif
                //fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] buf[%ld] = %s <-- %s / construct_next ? %s / mark_changed ? %s\n", caller->array_name.c_str(), PartitionStatistics::my_first_node_id() + upd.dst_vid, std::to_string(buf[upd.dst_vid]).c_str(), std::to_string(upd.message).c_str(), construct_next ? "true" : "false", mark_changed ? "true" : "false");
                buf[upd.dst_vid] = upd.message;
                //if (construct_next) next_flags.Set(upd.dst_vid);
                if (mark_changed) changed_flags.Set(upd.dst_vid);
            }
            //fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] |marked_next| = %ld\n", caller->array_name.c_str(), next_flags.GetTotal());

            RequestRespond::ReturnPerThreadBuffer(data);
        }

        virtual void ReadUpdatesAndConstructWithoutFlush(int i, int j, bool construct_next, bool construct_prev, bool mark_changed, GBVersionedArrayBase<T, op>* prev_) {
            if (!this->use_main_) return;
            T* buf_ = buf_orig;
            if (construct_next) buf_ = buf_next_orig;
            else if (construct_prev) buf_ = GetData(prev_);

            ReadUpdatesAndConstructWithoutFlush_(i, j, buf_, construct_next, mark_changed);
        }
        
        // Used in Construct Function
        void ReadUpdatesAndConstructWithoutFlush_(int i, int j, T* buf_, bool construct_next, bool mark_changed) {
            ReadDeltasAndProcessUsingDiskAio(i, j,
                    sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE,
                    this, &Callback_ReadUpdatesAndConstructWithoutFlush_,
                    (void*) buf_, construct_next, mark_changed);
        }
        
        virtual void InitializeAfterAdvanceUpdateVersion() {
            buf_ptr = buf[0]; 
            if (buf.size() >= 2)
                next_buf_ptr = buf[1];
            else next_buf_ptr = buf[0];
            if (this->use_main_) {
                buf_ptr = buf_orig;
                next_buf_ptr = buf_orig;
            }
            //fprintf(stdout, "%s InitializeAfterAdvanceUpdateVersion ss ver = 0 (cur %d) %p\n", this->array_name.c_str(), this->current_version.GetSuperstepVersion(), buf_ptr); 
            if (prev != NULL) prev->InitializeAfterAdvanceUpdateVersion();
        }

        virtual T GetValue(int64_t idx) { return buf_ptr[idx]; }
        virtual T* GetBuffer() { return buf_ptr; }
        virtual T* GetPrevBuffer() { return nullptr; }
        virtual void _SetBufPtr(int superstep_version) { 
            if (this->use_main_) return;
            if (!this->construct_next_ss_array_async_) return;
            if (superstep_version <= this->current_version.GetSuperstepVersion()) return;
            int64_t total_ = 0;
            //fprintf(stdout, "%s _SetBufPtr ss ver = %d (cur %d) %p, total_ = %ld\n", this->array_name.c_str(), superstep_version, this->current_version.GetSuperstepVersion(), buf_ptr, total_); 
            //this->changed_flags.ClearAll();
            //buf_ptr = buf[UserArguments::SUPERSTEP + 1]; 
            char prev_toggle = (this->changed_toggle_ + 1) % 2;
            if (UserArguments::UPDATE_VERSION >= 1) {
                if (!this->is_ov_) {
                    this->dirty_flags.InvokeIfMarked([&](node_t idx) {
                        if (!this->changed_flags[this->changed_toggle_].Get_Atomic(idx)) {
                            //if (UserArguments::SUPERSTEP == 1)
                            //    fprintf(stdout, "%s next_buf_ptr[%ld] %ld <- buf_ptr[%ld] %ld\n", this->array_name.c_str(), idx, (int64_t) next_buf_ptr[idx], idx, (int64_t) buf_ptr[idx]);
                            if (next_buf_ptr[idx] != buf_ptr[idx]) {
                                this->changed_flags[this->changed_toggle_].Set_Atomic(idx);
                                next_buf_ptr[idx] = buf_ptr[idx];
                            }
                            //std::atomic_fetch_add((std::atomic<int64_t>*) &total_, +1L);
                        }
                    });
                }
                if (prev != NULL && superstep_version >= 1) {
                    std::vector<T*>& prev_buf = prev->GetBuf();
                    T* target_prev_buf = prev_buf[superstep_version - 1];
                    //fprintf(stdout, "%s copy superstep %d at version (%d, %d) changed_flags[%d] total %ld\n", this->array_name.c_str(), superstep_version - 1, UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, prev_toggle, this->changed_flags[prev_toggle].GetTotal());
                    //memcpy((void*) target_prev_buf, (void*) buf[superstep_version - 1], sizeof(T) * this->buf_length);
                    this->changed_flags[prev_toggle].InvokeIfMarked([&](node_t idx) {
                        target_prev_buf[idx] = buf[superstep_version - 1][idx];
                    });
#ifndef PERFORMANCE
                    for (int64_t idx = 0; idx < this->buf_length; idx++) {
                        if (target_prev_buf[idx] != buf[superstep_version - 1][idx]) {
                            fprintf(stdout, "ss %d prev_buf[%ld] = %ld != buf %ld, changed_flag (prev: %s, cur: %s) dirty_flag %s\n", superstep_version - 1, idx, target_prev_buf[idx], buf[superstep_version - 1][idx], this->changed_flags[prev_toggle].Get(idx) ? "true" : "false", this->changed_flags[this->changed_toggle_].Get(idx) ? "true" : "false", this->dirty_flags.Get(idx) ? "true" : "false");
                            LOG_ASSERT(false);
                        }
                    }
#endif
                }
            }
            this->changed_flags[prev_toggle].ClearAll();
            buf_ptr = buf[superstep_version]; 
            this->changed_toggle_ = (this->changed_toggle_ + 1) % 2;
            //if (this->is_ov_)
            //    fprintf(stdout, "%s buf_ptr[%ld] = %ld, %p\n", this->array_name.c_str(), 6954354, buf_ptr[6954354], buf_ptr);
            if (prev != NULL) {
                prev->_SetBufPtrInternal(superstep_version);
            }
        }
        void _SetBufPtrInternal(int superstep_version) { 
            //fprintf(stdout, "%s prev _SetBufPtr ss ver = %d (cur %d) %p\n", this->array_name.c_str(), superstep_version, this->current_version.GetSuperstepVersion(), buf_ptr); 
            buf_ptr = buf[superstep_version]; 
        }

        T* data() { return buf_ptr; }
        T* next_data() { return next_buf_ptr; }
        T* prev_ss_data() { return nullptr; }
        T* GetMainData() { return nullptr; }
        std::vector<T*>& GetBuf() { return buf; }
        size_t GetMainLength() { return 0; }
        GBVersionedArrayBase<T, op>* GetRef() { return dynamic_cast<GBVersionedArrayBase<T, op>*>(ref); }
        T* GetData(GBVersionedArrayBase<T, op>* va) {
            GBVersionedArray<T, op>* va_temp = dynamic_cast<GBVersionedArray<T, op>*>(va);
            return va_temp->data();
        }


    private:
        GBVersionedArray<T, op>* ref;
        GBVersionedArray<T, op>* prev;
        
        std::vector<T*> buf;
        T* buf_ptr;
        T* next_buf_ptr;
        
        T* buf_orig;
        T* buf_next_orig; // TODO changed to lightweight datastructure
        T* buf_prev_ss_orig; // for previous superstep (only used in difference encoding(..?))
 
        int64_t cur_read_idx, cur_read_size;
        int64_t prev_read_idx, prev_read_size;
        int64_t memory_usage;
        bool is_prev_exist = false;
        bool pre_allocated = false;
};

#endif
