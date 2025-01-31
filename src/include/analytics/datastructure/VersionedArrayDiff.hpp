#pragma once
#ifndef VERSIONED_ARRAY_DIFF_H
#define VERSIONED_ARRAY_DIFF_H

#include "VersionedArrayBase.hpp"

//#define PER_THREAD_BUFFER_SIZE (32*1024)
//#define FLOAT_PRECISION_TOLERANCE 0.02

template <typename T, Op op>
class VersionedArrayDiff : public VersionedArrayBase<T, op> {

    public:
        VersionedArrayDiff() : ref(NULL), buf(NULL) {}

        ~VersionedArrayDiff() {}

        virtual bool IsCreated() {
            return (buf != nullptr);
        }
        
        static void callAppend(Turbo_bin_aio_handler* handler, size_t size_to_append, char* buffer) {
            LOG_ASSERT(false);
            handler->Append(size_to_append, buffer, NULL); //kjhong change
        }

        virtual void InitReadOnly(VersionedArrayBase<T, op>& o, bool maintain_prev_ss=false) {
            INVARIANT(false);
        }
        
        virtual void CreateReadOnly() {
            INVARIANT(false);
        }

        void SetPrev(VersionedArrayDiff<T, op>& prev_) {
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
            this->buf_memory_usage = 0;
            this->bitmap_memory_usage = 0;

            // Compute memory usage for Main & Buffers
            if (this->use_main_) this->buf_memory_usage += (sizeof(T) * this->buf_length);
            this->buf_memory_usage += (2 * sizeof(T) * this->buf_length); // Basic buffer & PrevSS buffer
            if (this->construct_next_ss_array_async_) this->buf_memory_usage += (sizeof(T) * this->buf_length); // Next buffer
            
            // Compute memory usage for Bitmaps
            this->bitmap_memory_usage += (3 * BitMap<int64_t>::compute_container_size(this->buf_length));
            if (this->maintain_prev_ss_) this->bitmap_memory_usage += BitMap<int64_t>::compute_container_size(this->buf_length);
            if (this->advance_superstep_ && this->construct_next_ss_array_async_) this->bitmap_memory_usage += (6 * BitMap<int64_t>::compute_container_size(this->buf_length));
        }

        virtual void InitializeMainAndBuffer(std::string array_path, bool open) {
            iden = idenelem<T>(op);
            std::string main_array_path = array_path + "_" + "0";

            if (this->use_main_) {
                if (open) this->main.Open(main_array_path.c_str(), true);
                else this->main.Create(main_array_path.c_str(), this->buf_length);
                this->main.Mlock(false);
                memset((char*) this->main.data(), 0, sizeof(T) * this->buf_length);
            }

            buf = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf[i] = iden;

            if (this->construct_next_ss_array_async_) {
                buf_next = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_next[i] = iden;
            }

            buf_prev = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf_prev[i] = iden;
            //LocalStatistics::register_mem_alloc_info((this->array_name + " VersionedArrayDiff").c_str(), memory_usage / (1024 * 1024));
        }
        
        virtual int64_t FreeMainAndBuffer(bool rm) {
            NumaHelper::free_numa_memory(buf, sizeof(T) * this->buf_length);
            buf = NULL;
            if (GetRef() != NULL) return 0;

            NumaHelper::free_numa_memory(buf_next, sizeof(T) * this->buf_length);
            buf_next = NULL;
            LocalStatistics::register_mem_alloc_info((this->array_name + " VersionedArray").c_str(), 0);
            if (this->use_main_) {
                this->main.Munlock();
                this->main.Close(rm);
            }
            
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                if (this->ptr_to_per_thread_buffer[i] == NULL) continue;
                char* data = (char*) this->ptr_to_per_thread_buffer[i];
                this->ptr_to_per_thread_buffer[i] = NULL;
                RequestRespond::ReturnPerThreadBuffer(data);
            }
            
            return 2 * sizeof(T) * this->buf_length;
        }

        virtual void CopyBuffer(VersionedArrayBase<T, op>* prev_) {
            T* buf_prev_ = GetData(prev_);
            memcpy((void*) buf, (void*) buf_prev_, sizeof(T) * this->buf_length);
        }

        virtual void CopyMainBuffer() {
            if (this->use_main_) {
                memcpy((void*) buf, (void*) this->main.data(), sizeof(T) * this->main.length());
            }
        }
        
        virtual void CopyMainBufferFromRef(VersionedArrayBase<T, op>* prev_) {
            if (prev->use_main_) {
                T* buf_ = GetData(prev_);
                memcpy((void*) buf_, (void*) this->main.data(), sizeof(T) * this->main.length());
            }
        }

        // IV
        virtual void MergeWithNext(Version from, Version to, VersionedArrayBase<T, op>* prev_) {
            INVARIANT(false); // IV never use this class
        }
       
        // OV
        virtual void MergeWithNext(Version from, Version to) {
#ifdef HYBRID_COST_MODEL
            turbo_timer tim;
            tim.start_timer(0);
            this->costs[1] = this->Compute_B();
            this->costs[2] = this->Compute_C();
            tim.stop_timer(0);
            
            tim.start_timer(1);
            TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                //std::cout << this->array_name << " buf[" << idx << "] = " << buf[idx] << " <- buf_next[" << idx << "] = " << buf_next[idx] << "\n";
                Operation<T, op>(&buf[idx], buf_next[idx]);
                buf_prev[idx] = buf[idx];
                buf_next[idx] = iden;
            }, this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]);
            tim.stop_timer(1);
            tim.start_timer(2);
            tim.stop_timer(2);
            //fprintf(stdout, "[%ld] OV MergeWithNext %s %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(),  this->array_name.c_str(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2));
#else
            this->next_flags[this->toggle_].InvokeIfMarked([&](node_t idx) {
                buf[idx] += buf_next[idx];
            }, false, UserArguments::NUM_TOTAL_CPU_CORES);
#endif
        }

        static void Callback_ReadUpdatesAndConstructWithoutFlush_(diskaio::DiskAioRequest* req) {
            diskaio::DiskAioRequestUserInfo& info = req->user_info;
            VersionedArrayDiff<T, op>* caller = (VersionedArrayDiff<T, op>*) info.caller;
            char* data = (char*) req->GetBuf();
            size_t num_entries = req->GetIoSize();

            DeltaMessage<T>* updates = (DeltaMessage<T>*) data;
            size_t num_updates = num_entries / sizeof(DeltaMessage<T>);

            T* buf = (T*) req->user_info.buf_to_construct;
            TwoLevelBitMap<int64_t>& next_flags = (caller->next_flags[caller->toggle_]);
            bool construct_next = req->user_info.construct_next;

            for (int64_t idx = 0; idx < num_updates; idx++) {
                DeltaMessage<T> upd = updates[idx];
                if (upd.dst_vid == -1) continue;
#ifdef INCREMENTAL_LOGGING
                if (INCREMENTAL_DEBUGGING_TARGET(PartitionStatistics::my_first_node_id() + upd.dst_vid)) {
                    fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] buf[%ld] = %s <-- %s / construct_next ? %s\n", caller->array_name.c_str(), PartitionStatistics::my_first_node_id() + upd.dst_vid, std::to_string(buf[upd.dst_vid]).c_str(), std::to_string(upd.message).c_str(), construct_next ? "true" : "false");
                }
#endif
                //std::cout << caller->array_name << " buf[" << upd.dst_vid << "] = " << buf[upd.dst_vid] << " message = " << upd.message << "\n";
                Operation<T, op>(&buf[upd.dst_vid], upd.message);
                if (construct_next) next_flags.Set(upd.dst_vid);
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

            T* buf_ = buf;
            if (construct_next) buf_ = buf_next;
            else if (construct_prev) buf_ = GetData(prev_);

            ReadUpdatesAndConstructWithoutFlush_(i, j, buf_, construct_next);
        }

        virtual void InitializeAfterAdvanceUpdateVersion() {
            // Initialize OV to identity element
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf[i] = iden;
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf_prev[i] = iden;
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf_next[i] = iden;
        }
        
        // Used in Construct Function
        void ReadUpdatesAndConstructWithoutFlush_(int i, int j, T* buf_, bool construct_next) {
            //fprintf(stdout, "\t[ReadUpdatesAndConstructWithoutFlush_][%s] %ld %ld\n", this->array_name.c_str(), i, j);
            VersionedArrayBase<T,op>::ReadDeltasAndProcessUsingDiskAio(i, j,
                    sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE,
                    this, &Callback_ReadUpdatesAndConstructWithoutFlush_,
                    (void*) buf_, construct_next);
        }

        // Assumed that this function is only called once for each element
        virtual T GetValue(int64_t idx) {
            T ret_val = buf[idx];
            InverseOperation<T, op>(&ret_val, buf_prev[idx]);
            //std::cout << this->array_name << " ret_val = " << ret_val << " buf[" << idx << "] = " << buf[idx] << " buf_prev[" << idx << "] = " << buf_prev[idx] << "\n";
            buf_prev[idx] = buf[idx];
            return ret_val;
        }
        virtual T* GetBuffer() { return buf; }
        virtual T* GetPrevBuffer() { return buf_prev; }

        void Write(node_t idx, T val) {
            this->MarkDirty(idx);
            if (buf[idx] != val) {
                this->MarkChanged(idx);
                this->MarkDiscard(idx);
                this->MarkToFlush(idx);
                buf[idx] = val;
            }
        }

        T* data() { return buf; }
        T* GetMainData() { return this->main.data(); }
        size_t GetMainLength() { return this->main.length(); }
        VersionedArrayBase<T, op>* GetRef() { return dynamic_cast<VersionedArrayBase<T, op>*>(ref); }
        T* GetData(VersionedArrayBase<T, op>* va) {
            VersionedArrayDiff<T, op>* va_temp = dynamic_cast<VersionedArrayDiff<T, op>*>(va);
            return va_temp->data();
        }


    private:
        VersionedArrayDiff<T, op>* ref;
        VersionedArrayDiff<T, op>* prev;
        
        T* buf;
        T* buf_next; //TODO changed to lightweight datastructure
        T* buf_prev;
        T iden;
 
        int64_t cur_read_idx, cur_read_size;
        int64_t prev_read_idx, prev_read_size;
        int64_t memory_usage;
        bool is_prev_exist = false;
};

#endif
