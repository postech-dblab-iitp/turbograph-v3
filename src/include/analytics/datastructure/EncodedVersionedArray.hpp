#pragma once

#include "Blocking_Turbo_bin_io_handler.hpp"
#include "Turbo_bin_mmapper.hpp"
#include "MemoryMappedArray.hpp"
#include "TypeDef.hpp"
#include "util.hpp"
#include "BitMap.hpp"
#include "eXDB_dist_internal.hpp"
#include "VersionedArrayBase.hpp"

#include <cstdlib>
#include <cmath>

#define ENCODED_PER_THREAD_BUFFER_SIZE (512*1024)

//#define COMPRESSION_VARINT
#define COMPRESSION_LZ4

template <typename T, Op op>
class EncodedVersionedArray : public VersionedArrayBase<T, op> {

    public:
        EncodedVersionedArray() : ref(NULL), buf(NULL), use_main_(false) {}
        
        virtual bool IsCreated() {
            return (buf != nullptr);
        }
        
        static void callAppend(Turbo_bin_io_handler* handler, size_t size_to_append, char* buffer) {
            handler->Append(size_to_append, buffer);
        }

        virtual void CreateReadOnly(VersionedArrayBase<T, op>& o) {
			this->buf_length = o.GetBufLength();
            this->array_path = o.GetArrayPath();
            buf = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
            buf_next = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
            LocalStatistics::register_mem_alloc_info((this->array_path + " Versioned Array(ReadOnly)").c_str(), (2 * sizeof(T) * this->buf_length) / (1024 * 1024));

            this->current_version.SetUpdateVersion(0);
            this->current_version.SetSuperstepVersion(-1);

            VersionedArrayBase<T, op>* o_temp = &o;
            ref = dynamic_cast<EncodedVersionedArray<T, op>*>(o_temp);
            INVARIANT(ref != NULL);
        }
        
        virtual void InitializePerThreadBuffers() {
            per_thread_buffers = new uint8_t[2 * ENCODED_PER_THREAD_BUFFER_SIZE * UserArguments::NUM_THREADS];
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                ptr_to_per_thread_buffer[0][i] = &per_thread_buffers[ENCODED_PER_THREAD_BUFFER_SIZE * 2 * i];
                ptr_to_per_thread_buffer[1][i] = &per_thread_buffers[ENCODED_PER_THREAD_BUFFER_SIZE * (2 * i + 1)];
                idx_in_per_thread_buffer[i].idx = 0;
                cur[i] = 0;
                first[i] = true;
            }
            
            per_thread_buffers_next = new uint8_t[2 * ENCODED_PER_THREAD_BUFFER_SIZE * UserArguments::NUM_THREADS];
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
                ptr_to_per_thread_buffer_next[0][i] = &per_thread_buffers_next[ENCODED_PER_THREAD_BUFFER_SIZE * 2 * i];
                ptr_to_per_thread_buffer_next[1][i] = &per_thread_buffers_next[ENCODED_PER_THREAD_BUFFER_SIZE * (2 * i + 1)];
                idx_in_per_thread_buffer_next[i].idx = 0;
            }
        }

        bool hasVersion(int u, int s) {
            if (this->deltas.size() > u && this->deltas[u].size() > s) return true;
            return false;
        }
        
        virtual void InitializeMainAndBuffer(std::string array_path, bool open, bool use_main, bool is_ov) {
            use_main_ = use_main;
            std::string main_array_path = array_path + "_" + "0";

            if (use_main_) {
#ifdef VERSIONED_ARRAY_IN_MEMORY
                main = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
#else
                if (open) main.Open(main_array_path.c_str(), true);
                else main.Create(main_array_path.c_str(), this->buf_length);
                main.Mlock(false);
#endif
            }
            buf = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
            buf_next = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * this->buf_length);
            LocalStatistics::register_mem_alloc_info((array_path + " Versioned Array").c_str(), (2 * sizeof(T) * this->buf_length) / (1024 * 1024));
        }
        
        virtual void InitializeBufNext() {

        }
        virtual void InitializeBufDelta() {

        }

        virtual int64_t FreeMainAndBuffer(bool rm) {
            NumaHelper::free_numa_memory(buf, sizeof(T) * this->buf_length);
            buf = NULL;
            if (GetRef() != NULL) return 0;

            NumaHelper::free_numa_memory(buf_next, sizeof(T) * this->buf_length);
            buf_next = NULL;
            LocalStatistics::register_mem_alloc_info((this->array_path + " Versioned Array").c_str(), 0);
            if (use_main_) {
#ifdef VERSIONED_ARRAY_IN_MEMORY
                NumaHelper::free_numa_memory(main, sizeof(T) * this->buf_length);
#else
                main.Munlock();
                main.Close(rm);
#endif
            }
            delete per_thread_buffers;
            delete per_thread_buffers_next;
            return 2 * sizeof(T) * this->buf_length;
        }

        virtual void CopyBuffer(VersionedArrayBase<T, op>& prev_) {

        }

        virtual void CopyMainBuffer() {
            if (use_main_) {
#ifdef VERSIONED_ARRAY_IN_MEMORY
                memcpy((void*) buf, (void*) main, sizeof(T) * this->buf_length);
#else
                memcpy((void*) buf, (void*) main.data(), sizeof(T) * main.length());
#endif
            }
        }
        
        virtual void CopyMainBufferFromRef(VersionedArrayBase<T, op>& prev) {
            T* buf_ = GetData(prev);
#ifdef VERSIONED_ARRAY_IN_MEMORY
            memcpy((void*) buf_, (void*) ref->main, sizeof(T) * ref->this->buf_length);
#else
            memcpy((void*) buf_, (void*) main.data(), sizeof(T) * main.length());
            //memcpy((void*) buf_, (void*) ref->GetMainData(), sizeof(T) * ref->GetMainLength());
#endif
        }
        
        virtual void MergeWithNextOpt(Version from, Version to, VersionedArrayBase<T, op>& prev) {
            T* buf_prev_ = GetData(prev);
            this->next_flags.InvokeIfMarked([&](node_t idx) {
                if (!checkEquality(buf_prev_[idx], buf_next[idx])) {
                    buf_prev_[idx] = buf_next[idx];
                }
                if (!checkEquality(buf[idx], buf_next[idx])) {
                    if (!this->dirty_flags.Get_Atomic(idx)) {
                        buf[idx] = buf_next[idx];
                    } else {
                        this->to_flush_flags.Set_Atomic_IfNotSet(idx);
                        //this->changed_flags.Set_Atomic(idx);
                    }
                } else {
                    this->to_flush_flags.Clear_Atomic(idx);
                    this->changed_flags.Clear_Atomic(idx);
                }
            });
        }
        
        virtual void MergeWithNextOpt(Version from, Version to, bool flush=false) {
            this->next_flags.InvokeIfMarked([&](node_t idx) {
                if (!checkEquality(buf[idx], buf_next[idx])) {
                    buf[idx] = buf_next[idx];
                }
            });
        }
        
        virtual void ReadUpdatesAndConstructNextWithoutFlushOpt(int i, int j) {
#ifdef VERSIONED_ARRAY_BLOCKING
            INVARIANT(false);
#else
            int64_t total_io_maximum;
            ReadDeltasAndProcess(i, j, total_io_maximum,
                    (char*) per_thread_buffers_next, ENCODED_PER_THREAD_BUFFER_SIZE,
                    [&](char* data, size_t num_entries) {
                        uint8_t* updates = (uint8_t*) data;
                        int64_t begin = 0;
                        int64_t end = num_entries / sizeof(uint8_t);
                        while  (end > begin) {
                            int64_t dst_vid = Decode<int64_t>(updates, begin, end);
                            T message = Decode<T>(updates, begin, end);
                            buf_next[dst_vid] = message;
                            this->next_flags.Set_Atomic(dst_vid);
                        }
                    });
#endif
        }
        
        virtual void ReadUpdatesAndConstructWithFlushOpt(int i, int j, int64_t& total_io_maximum) {
#ifdef VERSIONED_ARRAY_BLOCKING
            INVARIANT(false);
#else
            ReadDeltasAndProcess(i, j, total_io_maximum, 
                    (char*) per_thread_buffers, ENCODED_PER_THREAD_BUFFER_SIZE, 
                    [&](char* data, size_t num_entries) {
                        uint8_t* updates = (uint8_t*) data;
                        int64_t begin = 0;
                        int64_t end = num_entries / sizeof(uint8_t);
//#pragma omp parallel for
//#pragma omp parallel for schedule(dynamic) num_threads(UserArguments::NUM_THREADS)
                        while  (end > begin) {
                            int64_t dst_vid = Decode<int64_t>(updates, begin, end);
                            T message = Decode<T>(updates, begin, end);
                            if (this->latest_flags.Get_Atomic(dst_vid)) continue;
                            this->latest_flags.Set_Atomic(dst_vid);

                            if (!checkEquality(buf[dst_vid], message)) {
                                if(!this->dirty_flags.Get_Atomic(dst_vid)) {
                                    buf[dst_vid] = message;
                                } else {
                                    this->to_flush_flags.Set_Atomic_IfNotSet(dst_vid);
                                    //this->changed_flags.Set_Atomic(upd.dst_vid);
                                }
                            } else {
                                this->to_flush_flags.Clear_Atomic(dst_vid);
                                this->changed_flags.Clear_Atomic(dst_vid);
                            }
                        }
                    });
#endif
        }

        virtual void ReadUpdatesAndConstructWithoutFlush(int i, int j, int64_t& total_io_maximum) {
#ifdef VERSIONED_ARRAY_BLOCKING
            INVARIANT(false);
#else
#pragma omp parallel for schedule(dynamic) num_threads(UserArguments::NUM_THREADS)
            for (int k = 0; k < MAX_NUM_CPU_CORES; k++) {
                INVARIANT (this->deltas[i][j][k] != NULL);
                if (this->deltas[i][j][k]->file_size() == 0) continue;
                uint8_t* updates = (uint8_t*) this->deltas[i][j][k]->CreateMmap(false);

                int64_t begin = 0;
                int64_t end = (this->deltas[i][j][k]->file_size() / sizeof(uint8_t));
                while (end > begin) {                            
                    int64_t dst_vid = Decode<int64_t>(updates, begin, end);
                    T message = Decode<T>(updates, begin, end);
                    buf[dst_vid] = message;
                }
                this->deltas[i][j][k]->DestructMmap();
            }
#endif
        }
 
        virtual void ReadUpdatesAndConstructWithoutFlushOpt(int i, int j, int64_t& total_io_maximum) {
            ReadUpdatesAndConstructWithoutFlushOpt_(i, j, total_io_maximum, buf);
        }
        
        virtual void ReadUpdatesAndConstructWithoutFlushOpt(int i, int j, int64_t& total_io_maximum, VersionedArrayBase<T, op>& prev) {
            T* buf_ = GetData(prev);
            ReadUpdatesAndConstructWithoutFlushOpt_(i, j, total_io_maximum, buf_);
        }
        
        void ReadUpdatesAndConstructWithoutFlushOpt_(int i, int j, int64_t& total_io_maximum, T* buf_) {
#ifdef VERSIONED_ARRAY_BLOCKING
            INVARIANT(false);
#else
            ReadDeltasAndProcess(i, j, total_io_maximum, 
                    (char*) per_thread_buffers, ENCODED_PER_THREAD_BUFFER_SIZE,
                    [&](char* data, size_t num_entries) {
                        uint8_t* updates = (uint8_t*) data;
                        int64_t begin = 0;
                        int64_t end = num_entries / sizeof(uint8_t);
//#pragma omp parallel for
                        while  (end > begin) {
                            int64_t dst_vid = Decode<int64_t>(updates, begin, end);
                            T message = Decode<T>(updates, begin, end);
                            buf_[dst_vid] = message;
                        }
                    });
#endif
        }

        virtual void FlushAll_(Version to) {
            if (to.GetUpdateVersion() != 0) this->tim.start_timer(1);
            //fprintf(stdout, "FlushAll %s\t%ld\t%ld\n", this->array_path.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion());
            
            if (to.GetUpdateVersion() == 0 && to.GetSuperstepVersion() == 0 && !use_main_) {
                this->to_flush_flags.ClearAll();
            } else {
                this->to_flush_flags.InvokeIfMarked([&](node_t idx) {
                    Write(idx, buf[idx], to.GetUpdateVersion(), to.GetSuperstepVersion());
                    });
                this->to_flush_flags.ClearAll();
                FlushPerThreadWriteBuffers(to);
            }
            if (to.GetUpdateVersion() != 0) this->tim.stop_timer(1);
        }

        void Write(int64_t idx, T val, int update_version, int superstep_version) {
#ifdef INCREMENTAL_LOGGING
            if (INCREMENTAL_DEBUGGING_TARGET(idx + PartitionStatistics::my_first_node_id())) {
                fprintf(stdout, "FlushWrite buf[%ld] = %ld into (i=%ld, j=%ld)\t%s\n", (int64_t) idx + PartitionStatistics::my_first_node_id(), (int64_t) val, (int64_t) update_version, (int64_t) superstep_version, this->array_path.c_str());
            }
#endif
            ALWAYS_ASSERT(buf[idx] == val);
            if (update_version == 0 && superstep_version == 0) {
                main[idx] = val;
                ALWAYS_ASSERT (use_main_);
            } else {
                int64_t tid = omp_get_thread_num();
                if (idx_in_per_thread_buffer[tid].idx + sizeof(int64_t) + sizeof(T) > ENCODED_PER_THREAD_BUFFER_SIZE) {
#ifdef VERSIONED_ARRAY_BLOCKING
                    deltas[update_version][superstep_version]->Append(sizeof(uint8_t) * idx_in_per_thread_buffer[tid].idx, (char*) ptr_to_per_thread_buffer[cur[tid]][tid]);
#else
                    if (!first[tid]) reqs_to_wait[tid].get();
                    reqs_to_wait[tid] = Aio_Helper::EnqueueAsynchronousTaskTemp(VersionedArray<T, op>::callAppend, this->deltas[update_version][superstep_version][tid], sizeof(uint8_t) * idx_in_per_thread_buffer[tid].idx, (char*) ptr_to_per_thread_buffer[cur[tid]][tid]); //TODO
#endif
                    idx_in_per_thread_buffer[tid].idx = 0;
                    cur[tid] = (cur[tid] + 1) % 2;
                    first[tid] = false;
                }
#ifdef COMPRESSION_LZ4
                int idx_size = EncodeLZ4<int64_t>(&idx, &ptr_to_per_thread_buffer[cur[tid]][tid][idx_in_per_thread_buffer[tid].idx], ENCODED_PER_THREAD_BUFFER_SIZE - idx_in_per_thread_buffer[tid].idx);
#else
                size_t idx_size = Encode<uint64_t>((uint64_t)idx, &ptr_to_per_thread_buffer[cur[tid]][tid][idx_in_per_thread_buffer[tid].idx]);
                idx_in_per_thread_buffer[tid].idx += idx_size;
                size_t val_size = Encode<T>(val, &ptr_to_per_thread_buffer[cur[tid]][tid][idx_in_per_thread_buffer[tid].idx]);
                idx_in_per_thread_buffer[tid].idx += val_size;
#endif
            }
        }

        void WriteFlush(int update_version, int superstep_version) {
            int64_t tid = omp_get_thread_num();
            if (idx_in_per_thread_buffer[tid].idx == 0) return;
            if (update_version == 0 && superstep_version == 0) return;
#ifdef VERSIONED_ARRAY_BLOCKING
            this->deltas[update_version][superstep_version]->Append(sizeof(DeltaMessage<T>) * idx_in_per_thread_buffer[tid].idx, (char*) ptr_to_per_thread_buffer[cur[tid]][tid]);
#else
            if (!first[tid]) reqs_to_wait[tid].get();
            this->deltas[update_version][superstep_version][tid]->Append(sizeof(DeltaMessage<T>) * idx_in_per_thread_buffer[tid].idx, (char*) ptr_to_per_thread_buffer[cur[tid]][tid]);
#endif
            idx_in_per_thread_buffer[tid].idx = 0;
            cur[tid] = (cur[tid] + 1) % 2;
            first[tid] = true;
        }

        void FlushPerThreadWriteBuffers(Version to) {
            if (to.GetUpdateVersion() != 0) this->tim.start_timer(6);
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
            {
                WriteFlush(to.GetUpdateVersion(), to.GetSuperstepVersion());
            }
            if (to.GetUpdateVersion() != 0) this->tim.stop_timer(6);
        }

        void UpdateElement(int idx, T val) {
            MarkDirty(idx);
            if (buf[idx] != val) {
                MarkChanged(idx);
                buf[idx] = val;
            }
        }

        T* data() { return buf; }
        VersionedArrayBase<T, op>* GetRef() { return dynamic_cast<VersionedArrayBase<T, op>*>(ref); }
        T* GetData(VersionedArrayBase<T, op>& va) {
            VersionedArrayBase<T, op>* vab_temp = &va;
            EncodedVersionedArray<T, op>* va_temp = dynamic_cast<EncodedVersionedArray<T, op>*>(vab_temp);
            return va_temp->data();
        }

    private:
        EncodedVersionedArray<T, op>* ref;

        bool use_main_;

#ifdef VERSIONED_ARRAY_IN_MEMORY
        T* main;
#else
        MemoryMappedArray<T> main; // ver 0,0
#endif

        T* buf;
        T* buf_next;

        uint8_t* per_thread_buffers;
        uint8_t* ptr_to_per_thread_buffer[2][MAX_NUM_CPU_CORES];
        PaddedIdx idx_in_per_thread_buffer[MAX_NUM_CPU_CORES];
        uint8_t* per_thread_buffers_next;
        uint8_t* ptr_to_per_thread_buffer_next[2][MAX_NUM_CPU_CORES];
        PaddedIdx idx_in_per_thread_buffer_next[MAX_NUM_CPU_CORES];

#ifdef COMPRESSION_LZ4
        
#endif

        int cur[MAX_NUM_CPU_CORES];
        int64_t cur_read_idx, cur_read_size;
        int64_t prev_read_idx, prev_read_size;
        bool first[MAX_NUM_CPU_CORES];
        std::future<void> reqs_to_wait[MAX_NUM_CPU_CORES];
};

