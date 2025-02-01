#pragma once
#ifndef VERSIONED_ARRAY_H
#define VERSIONED_ARRAY_H

#include "analytics/datastructure/VersionedArrayBase.hpp"

//#define PER_THREAD_BUFFER_SIZE (32*1024)
//#define FLOAT_PRECISION_TOLERANCE 0.02

template <typename T, Op op>
class VersionedArray : public VersionedArrayBase<T, op> {

    public:
        VersionedArray() : ref(NULL), buf(NULL), buf_next(NULL) {}

        ~VersionedArray() {}

        virtual bool IsCreated() {
            return (buf != nullptr);
        }
        
        static void callAppend(Turbo_bin_aio_handler* handler, size_t size_to_append, char* buffer) {
            LOG_ASSERT(false);
            handler->Append(size_to_append, buffer, NULL); // kjhong
        }
        
        virtual void InitReadOnly(VersionedArrayBase<T, op>& o, bool maintain_prev_ss=false) {
            VersionedArrayBase<T, op>* o_temp = &o;
            ref = dynamic_cast<VersionedArray<T, op>*>(o_temp);
            INVARIANT(ref != NULL);
            this->buf_memory_usage = 0;
            this->bitmap_memory_usage = 0;

			this->buf_length = o.GetBufLength();
            this->array_path = o.GetArrayPath();
            this->array_name = o.GetArrayName();
            this->maintain_prev_ss_ = maintain_prev_ss;
            this->buf_memory_usage += sizeof(T) * this->buf_length;
            if (this->maintain_prev_ss_) this->buf_memory_usage += sizeof(T) * this->buf_length;
            this->maintain_prev_ss_flags.Init(this->buf_length);
            this->bitmap_memory_usage += BitMap<int64_t>::compute_container_size(this->buf_length);
            
            this->current_version.SetUpdateVersion(0);
            this->current_version.SetSuperstepVersion(-1);
        }

        virtual void CreateReadOnly() {
            buf = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf[i] = idenelem<T>(op);

            if (this->maintain_prev_ss_) {
                buf_prev_ss = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_prev_ss[i] = idenelem<T>(op);
            }
            LocalStatistics::register_mem_alloc_info((this->array_name + " VersionedArray(ReadOnly)").c_str(), (this->buf_memory_usage) / (1024 * 1024));
            LocalStatistics::register_mem_alloc_info((this->array_name + " VersionedArray(ReadOnly) BitMap").c_str(), (this->bitmap_memory_usage) / (1024 * 1024));
        }

        void SetPrev(VersionedArray<T, op>& prev_) {
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

            if (this->use_main_) {
                if (open) this->main.Open(main_array_path.c_str(), true);
                else this->main.Create(main_array_path.c_str(), this->buf_length);

                //this->main.Mlock(false);
                memset((char*) this->main.data(), 0, sizeof(T) * this->buf_length);
            }

            buf = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf[i] = idenelem<T>(op);
            if (this->construct_next_ss_array_async_) {
                buf_next = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_next[i] = idenelem<T>(op);
            }
            if (this->maintain_prev_ss_) {
                buf_prev_ss = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_prev_ss[i] = idenelem<T>(op);
            }
        }
        
        virtual int64_t FreeMainAndBuffer(bool rm) {
            if (buf != NULL) NumaHelper::free_numa_memory(buf, sizeof(T) * this->buf_length);
            buf = NULL;
            if (GetRef() != NULL) return 0;

            if (buf_next != NULL) NumaHelper::free_numa_memory(buf_next, sizeof(T) * this->buf_length);
            buf_next = NULL;
            if (this->use_main_) {
                this->main.Munlock();
                this->main.Close(true);
            }
            LocalStatistics::register_mem_alloc_info((this->array_name + " VersionedArray").c_str(), 0);
            
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
            T* buf_ = GetData(prev_);
            memcpy((void*) buf_, (void*) this->main.data(), sizeof(T) * this->main.length());
        }

        virtual void CopyPrevSSBuffer() {
            if (!this->maintain_prev_ss_) return;
            
            // For Static Processing or First Superstep
            if (UserArguments::INC_STEP == -1 || UserArguments::SUPERSTEP == 0) {
                if (UserArguments::SUPERSTEP == 0) {
                    memcpy((void*) buf_prev_ss, (void*) buf, sizeof(T) * this->buf_length);
                    this->prev_dirty_flags.ClearAll();
                    if (is_prev_exist && UserArguments::UPDATE_VERSION >= 1) {
                        T* buf_prev_ss_sn = prev->prev_ss_data();
                        T* buf_prev = prev->data();
                        memcpy((void*) buf_prev_ss_sn, (void*) buf_prev, sizeof(T) * this->buf_length);
                    }
                } else {
                    this->prev_dirty_flags.InvokeIfMarked([&](node_t idx) {
                        //std::cout << this->array_name << " buf_prev_ss[" << idx << "] = " << buf_prev_ss[idx] << " <- buf[" << idx << "] = " << buf[idx] << "\n";
                        buf_prev_ss[idx] = buf[idx];
                    });
                    this->prev_dirty_flags.ClearAll();
                }
                return;
            }

            // For Inc Processing
            this->maintain_prev_ss_flags.InvokeIfMarked([&](node_t idx) {
                buf_prev_ss[idx] = buf[idx];
            });
            this->maintain_prev_ss_flags.ClearAll();
            if (is_prev_exist) {
                T* buf_prev_ss_sn = prev->prev_ss_data();
                T* buf_prev_sn = prev->data();
                prev->GetMaintainFlags()->InvokeIfMarked([&](node_t idx) {
                    buf_prev_ss_sn[idx] = buf_prev_sn[idx];
                });
                prev->GetMaintainFlags()->ClearAll();
            }
        }

        // IV
        virtual void MergeWithNext(Version from, Version to, VersionedArrayBase<T, op>* prev_) {
            T* buf_prev_ = GetData(prev_);
            TwoLevelBitMap<int64_t>* prev_maintain_prev_ss_flags = prev_->GetMaintainFlags();
            if (this->maintain_prev_ss_) {
                this->maintain_prev_ss_flags.CopyFrom(this->changed_flags);
            }

#ifdef HYBRID_COST_MODEL
            //system_fprintf(0, stdout, "[%ld] MergeWithNext %s flush total %ld next total %ld %ld changed total %ld\n", PartitionStatistics::my_machine_id(), this->array_name.c_str(), this->to_flush_flags.GetTotal(), this->first_snapshot_delta_flags.GetTotal(), this->next_flags[this->toggle_].GetTotal(), this->changed_flags.GetTotal());
            turbo_timer tim;
            tim.start_timer(0);
            this->costs[1] = this->Compute_B();
            this->costs[2] = this->Compute_C();
						this->costs[6] = this->Compute_X();
            tim.stop_timer(0);
            tim.start_timer(1);
            //this->costs[4] = this->next_flags[this->toggle_].GetTotal();
            //system_fprintf(0, stdout, "[%ld] MergeWithNext %s flush total %ld next total %ld %ld changed total %ld, cost[4] = %ld\n", PartitionStatistics::my_machine_id(), this->array_name.c_str(), this->to_flush_flags.GetTotal(), this->first_snapshot_delta_flags.GetTotal(), this->next_flags[this->toggle_].GetTotal(), this->changed_flags.GetTotal(), this->costs[4]);
            tim.stop_timer(1);
            //TwoLevelBitMap<int64_t>::UnionToRight(this->first_snapshot_delta_flags, this->next_flags);
            //this->costs[5] = this->next_flags.GetTotal();
                    
            tim.start_timer(2);
            if (this->costs[1] + this->costs[2] != 0) {
                TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                    //std::cout << this->array_name << " buf[" << idx << "] = " << buf[idx] << " <- buf_next[" << idx << "] = " << buf_next[idx] << "\n";
                    if (!checkEquality(buf_prev_[idx], buf_next[idx])) {
                        buf_prev_[idx] = buf_next[idx];
                        prev_maintain_prev_ss_flags->Set(idx);
                    }
                    if (!checkEquality(buf[idx], buf_next[idx])) {
                        this->discard_flags.Set(idx);
                        if (!this->dirty_flags.Get(idx)) {
                            buf[idx] = buf_next[idx];
                            this->maintain_prev_ss_flags.Set(idx);
                            this->changed_flags.Clear_IfSet(idx);
                        } else {
                            this->changed_flags.Set(idx);
                            this->to_flush_flags.Set(idx);
                        }
                    } else {
                        this->to_flush_flags.Clear(idx);
                        this->changed_flags.Clear(idx);
                    }
                }, this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]);
            }
            tim.stop_timer(2);
            tim.start_timer(3);
            //this->costs[3] = this->to_flush_flags.GetTotalEstimation();
            this->costs[3] = this->to_flush_flags.GetTotal();
            if (this->costs[1] + this->costs[2] == 0) {
                this->costs[0] = this->costs[3];
            } else {
                this->costs[0] = this->discard_flags.GetTotal(); //XXX
                //this->costs[0] = this->discard_flags.GetTotalEstimation();
            }
            if (this->costs[2] == 0) {
                this->costs[4] = this->costs[3];
            } else {
                this->costs[4] = TwoLevelBitMap<int64_t>::GetTotalOfUnion(this->next_flags[this->toggle_], this->to_flush_flags);
                //this->costs[4] = TwoLevelBitMap<int64_t>::GetTotalEstimationOfUnion(this->next_flags[this->toggle_], this->to_flush_flags); //XXX
            }
            tim.stop_timer(3);
            
//#ifdef PRINT_PROFILING_TIMERS
            //fprintf(stdout, "[%ld] IV MergeWithNext %s %.4f %.4f %.4f %.4f, next total %ld %ld flush total %ld discard total %ld\n", PartitionStatistics::my_machine_id(), this->array_name.c_str(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3), this->first_snapshot_delta_flags[this->toggle_].GetTotal(), this->next_flags[this->toggle_].GetTotal(), this->to_flush_flags.GetTotal(), this->changed_flags.GetTotal());
            fprintf(stdout, "[%ld] IV MergeWithNext %s %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), this->array_name.c_str(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3));
//#endif
#else
            this->next_flags[this->toggle_].InvokeIfMarked([&](node_t idx) {
                if (!checkEquality(buf_prev_[idx], buf_next[idx])) {
                    buf_prev_[idx] = buf_next[idx];
                }
                if (!checkEquality(buf[idx], buf_next[idx])) {
                    if (!this->dirty_flags.Get(idx)) {
                        buf[idx] = buf_next[idx];
                        this->changed_flags.Clear_IfSet(idx);
                    } else {
                        this->changed_flags.Set(idx);
                        this->to_flush_flags.Set(idx);
                    }
                } else {
                    this->to_flush_flags.Clear(idx);
                    this->changed_flags.Clear(idx);
                }
            }, false, UserArguments::NUM_TOTAL_CPU_CORES);
#endif
            //fprintf(stdout, "[%s][MergeWithNext_IV] computeiocost\n", this->array_name.c_str());
            //this->ComputeIoCostModels(); 
        }
       
        // OV
        virtual void MergeWithNext(Version from, Version to) {
#ifdef HYBRID_COST_MODEL
            turbo_timer tim;
            tim.start_timer(0);
            this->costs[1] = this->Compute_B();
            this->costs[2] = this->Compute_C();
            this->costs[6] = this->Compute_X();
            tim.stop_timer(0);
            
            tim.start_timer(1);
            TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                //fprintf(stdout, "%s buf[%ld] = %s, cnt %s <- buf_next[%ld] = %s, cnt %s\n", this->array_name.c_str(), idx, to_string_with_precision(GET_VAL_64BIT(buf[idx])).c_str(), to_string_with_precision(GET_CNT_64BIT(buf[idx])).c_str(), idx, to_string_with_precision(GET_VAL_64BIT(buf_next[idx])).c_str(), to_string_with_precision(GET_CNT_64BIT(buf_next[idx])).c_str());
                if (!checkEquality(buf[idx], buf_next[idx])) {
                    buf[idx] = buf_next[idx];
                }
            }, this->next_flags[this->toggle_], this->first_snapshot_delta_flags[this->toggle_]);
            tim.stop_timer(1);
            tim.start_timer(2);
            tim.stop_timer(2);
            fprintf(stdout, "[%ld] OV MergeWithNext %s %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(),  this->array_name.c_str(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2));
#else

            //fprintf(stdout, "[%s][MergeWithNext_OV] |marked_next| = %ld\n", this->array_name.c_str(), this->next_flags[this->toggle_].GetTotal());
            this->next_flags[this->toggle_].InvokeIfMarked([&](node_t idx) {
#ifdef INCREMENTAL_LOGGING
                if (INCREMENTAL_DEBUGGING_TARGET(PartitionStatistics::my_first_node_id() + idx)) { 
                    fprintf(stdout, "[%s][OV MergeWithNext] buf[%ld] = %s <- buf_next[%ld] = %s\n", this->array_name.c_str(), PartitionStatistics::my_first_node_id() + idx, std::to_string(buf[idx]).c_str(), PartitionStatistics::my_first_node_id() + idx, std::to_string(buf_next[idx]).c_str());
                }
#endif
                if (!checkEquality(buf[idx], buf_next[idx])) {
                    buf[idx] = buf_next[idx];
                }
            }, false, UserArguments::NUM_TOTAL_CPU_CORES);
#endif
        }

        static void Callback_ReadUpdatesAndConstructWithoutFlush_(diskaio::DiskAioRequest* req) {
            diskaio::DiskAioRequestUserInfo& info = req->user_info;
            VersionedArray<T, op>* caller = (VersionedArray<T, op>*) info.caller;
            char* data = (char*) req->GetBuf();
            size_t num_entries = req->GetIoSize();

            DeltaMessage<T>* updates = (DeltaMessage<T>*) data;
            size_t num_updates = num_entries / sizeof(DeltaMessage<T>);

            T* buf = (T*) req->user_info.buf_to_construct;
            TwoLevelBitMap<int64_t>& next_flags = (caller->next_flags[caller->toggle_]);
            TwoLevelBitMap<int64_t>& changed_flags = (caller->changed_flags);
            bool construct_next = req->user_info.construct_next;
            bool mark_changed = req->user_info.mark_changed;

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
                if (construct_next) next_flags.Set(upd.dst_vid);
                if (mark_changed) changed_flags.Set(upd.dst_vid);
            }
            //fprintf(stdout, "\t[Callback_ReadUpdatesAndConstructWithoutFlush_][%s] |marked_next| = %ld\n", caller->array_name.c_str(), next_flags.GetTotal());

            RequestRespond::ReturnPerThreadBuffer(data);
        }

        virtual void ReadUpdatesAndConstructWithoutFlush(int i, int j, bool construct_next, bool construct_prev, bool mark_changed, VersionedArrayBase<T, op>* prev_) {
            // Four cases {construct_next} X {construct_prev}
            // (1) (true , true ): Not Supported
            // (2) (true , false): Construct Next Buffer
            // (3) (false, true ): Construct Previous Buffer
            // (4) (false, false): Construct Current Buffer
            INVARIANT(!construct_next || !construct_prev);
            //fprintf(stdout, "ReadUpdatesAndConstructWithoutFlush %s, (%d, %d) construct_next ? %s, construct_prev ? %s, mark_changed ? %s\n", this->array_name.c_str(), i, j, construct_next ? "true" : "false", construct_prev ? "true" : "false", mark_changed ? "true" : "false");

            T* buf_ = buf;
            if (construct_next) buf_ = buf_next;
            else if (construct_prev) buf_ = GetData(prev_);

            ReadUpdatesAndConstructWithoutFlush_(i, j, buf_, construct_next, mark_changed);
        }
        
        // Used in Construct Function
        void ReadUpdatesAndConstructWithoutFlush_(int i, int j, T* buf_, bool construct_next, bool mark_changed) {
            //fprintf(stdout, "\t[ReadUpdatesAndConstructWithoutFlush_][%s] %ld %ld\n", this->array_name.c_str(), i, j);
            this->ReadDeltasAndProcessUsingDiskAio(i, j,
                    sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE,
                    this, &Callback_ReadUpdatesAndConstructWithoutFlush_,
                    (void*) buf_, construct_next, mark_changed);
        }
        
        virtual void InitializeAfterAdvanceUpdateVersion() {
            if (!this->maintain_prev_ss_) return;
            //memset((char*) buf_prev_ss, 0, sizeof(T) * this->buf_length); // XXX iden value?
#pragma omp parallel for
            for (int64_t i = 0; i < this->buf_length; i++)
                buf_prev_ss[i] = idenelem<T>(op);
            if (is_prev_exist) {
                T* buf_prev_ss_sn = prev->prev_ss_data();
                //memset((void*) buf_prev_ss_sn, 0, sizeof(T) * this->buf_length);
#pragma omp parallel for
                for (int64_t i = 0; i < this->buf_length; i++)
                    buf_prev_ss_sn[i] = idenelem<T>(op);
            }
        }

        virtual T GetValue(int64_t idx) { return buf[idx]; }
        virtual T* GetBuffer() { return buf; }
        virtual T* GetPrevBuffer() { return nullptr; }

        T* data() { return buf; }
        T* prev_ss_data() { return buf_prev_ss; }
        T* GetMainData() { return this->main.data(); }
        size_t GetMainLength() { return this->main.length(); }
        VersionedArrayBase<T, op>* GetRef() { return dynamic_cast<VersionedArrayBase<T, op>*>(ref); }
        T* GetData(VersionedArrayBase<T, op>* va) {
            VersionedArray<T, op>* va_temp = dynamic_cast<VersionedArray<T, op>*>(va);
            return va_temp->data();
        }


    private:
        VersionedArray<T, op>* ref;
        VersionedArray<T, op>* prev;
        
        T* buf;
        T* buf_next; // TODO changed to lightweight datastructure
        T* buf_prev_ss; // for previous superstep (only used in difference encoding(..?))
        //T* buf_prev_ss_sn; // for previous superstep of previous snapshot (only used in difference encoding(..?))
 
        int64_t cur_read_idx, cur_read_size;
        int64_t prev_read_idx, prev_read_size;
        int64_t memory_usage;
        bool is_prev_exist = false;
};

#endif
