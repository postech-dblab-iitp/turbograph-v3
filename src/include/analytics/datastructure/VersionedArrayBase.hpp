#pragma once

#include <tbb/concurrent_queue.h>
#include <cstdlib>
#include <cmath>
#include <future>

#include "Blocking_Turbo_bin_io_handler.hpp"
#include "Turbo_bin_aio_handler.hpp"
#include "Turbo_bin_mmapper.hpp"

#include "MemoryMappedArray.hpp"
#include "TwoLevelBitMap.hpp"

#include "TypeDef.hpp"
#include "util.hpp"
#include "eXDB_dist_internal.hpp"
#include "disk_aio_request.hpp"
#include "turbo_dist_internal.hpp"

#include "RequestRespond.hpp"

#define USE_ENCODING
#define PER_THREAD_BUFFER_SIZE ((PER_THREAD_BUFF_SIZE/sizeof(DeltaMessage<T>)))
//#define PER_THREAD_BUFFER_SIZE (32*1024)
#define ENCODED_PER_THREAD_BUFFER_SIZE (512*1024)

//#define VERSIONED_ARRAY_IN_MEMORY
//#define VERSIONED_ARRAY_BLOCKING
#define MAX_NUM_SUPERSTEPS_IN_A_DIRECTORY 100
//#define DELTA_FILE_SIZE_THRESHOLD (1*1024*1024*1024L)
#define DELTA_FILE_SIZE_THRESHOLD (512*1024*1024L)
#define HYBRID_COST_MODEL
#define IO_LOGGING

template <typename T, Op op>
class VersionedArrayBase {

    public:
        VersionedArrayBase() {}
        /*VersionedArrayBase() : use_main_(false) {
            has_future_job = false;
        }*/
       
        virtual VersionedArrayBase<T, op>* GetRef() = 0;
        virtual bool IsCreated() = 0;

        // Initialize & Copy functions
        virtual void ComputeMemoryUsage() = 0;
        virtual void InitReadOnly(VersionedArrayBase<T, op>& o, bool maintain_prev_ss=false) = 0;
        virtual void CreateReadOnly() = 0;
        virtual void InitializePerThreadBuffers() = 0;
        virtual void InitializeMainAndBuffer(std::string array_path, bool open) = 0;
        virtual int64_t FreeMainAndBuffer(bool rm) = 0;
        virtual void CopyBuffer(VersionedArrayBase<T, op>* prev) = 0;
        virtual void CopyMainBuffer() = 0;
        virtual void CopyMainBufferFromRef(VersionedArrayBase<T, op>* prev) = 0;
        virtual void CopyPrevSSBuffer() {}
        virtual void InitializeAfterAdvanceUpdateVersion() {}


        // Read & Construct From Disk, Merge functions
        virtual void ReadUpdatesAndConstructWithoutFlush(int i, int j, bool construct_next, bool construct_prev, bool mark_changed, VersionedArrayBase<T, op>* prev=NULL) = 0;
        virtual void MergeWithNext(Version from, Version to) = 0;        // OV
        virtual void MergeWithNext(Version from, Version to, VersionedArrayBase<T, op>* prev) = 0;   // IV
        virtual void CheckIsSparse(Version to) {}
        
        //Get Buffer & Buffer Value API, Others
        virtual T GetValue(int64_t idx) = 0;
        virtual T* GetBuffer() = 0;
        virtual T* GetPrevBuffer() = 0;
        int64_t GetMemoryUsage() {
            return (buf_memory_usage + bitmap_memory_usage);
        }

        bool hasVersion(int u, int s) {
            INVARIANT(false); // XXX tslee 2019.07.24 not used
        }

        int64_t CountNumDeltasToMerge(Version from, Version to) {
            int64_t cnt = 0;
            for (int j = from.GetSuperstepVersion(); j <= to.GetSuperstepVersion(); j++) {
                for (int i = from.GetUpdateVersion(); i <= to.GetUpdateVersion(); i++) {
                    if (i >= deltas_offset.size()) continue;
                    if (j >= deltas_offset[i].size()) continue;
                    cnt++;
                }
            }
            return cnt;
        }

        void FinishAsyncJob() {
            if (has_future_job) {
                reqs_to_wait.get();
                has_future_job = false;
            }
        }

        static void call_ConstructNextAsync(VersionedArrayBase<T, op>* va, Version from, Version to) {
            INVARIANT(from.GetSuperstepVersion() == to.GetSuperstepVersion());
            int next_superstep = from.GetSuperstepVersion() + 1;
            if (next_superstep >= va->deltas.size()) {
                va->ClearAllNext();
                return;
            }

            from.SetSuperstepVersion(next_superstep);
            to.SetSuperstepVersion(next_superstep);
            to.SetUpdateVersion(to.GetUpdateVersion() - 1);
            va->ConstructNextAsync(from, to);
        }
        
        void InitializeFlags() {
            int64_t memory_usage = 0;
            toggle_ = 0;
            if (advance_superstep_ && construct_next_ss_array_async_) {
                next_flags[0].Init(buf_length);
                next_flags[1].Init(buf_length);
                first_snapshot_delta_flags[0].Init(buf_length);
                first_snapshot_delta_flags[1].Init(buf_length);
            }
            dirty_flags.Init(buf_length);
            changed_flags.Init(buf_length);
            discard_flags.Init(buf_length);
            to_flush_flags.Init(buf_length);
            maintain_prev_ss_flags.Init(buf_length);
            if (maintain_prev_ss_) {
                prev_dirty_flags.Init(buf_length);
            }
        }
        
        void FreeFlags() {
            if (advance_superstep_ && construct_next_ss_array_async_) {
                next_flags[0].Close();
                next_flags[1].Close();
                dirty_flags.Close();
                discard_flags.Close();
                first_snapshot_delta_flags[0].Close();
                first_snapshot_delta_flags[1].Close();
            }
            changed_flags.Close();
            to_flush_flags.Close();
            maintain_prev_ss_flags.Close();
            if (maintain_prev_ss_) {
                prev_dirty_flags.Close();
            }
            LocalStatistics::register_mem_alloc_info((array_name + " VersionedArray BitMap").c_str(), 0);
        }

        int64_t GetBitMapMemoryUsage() {
            int64_t max_entry = (PartitionStatistics::max_internal_nodes() + UserArguments::VECTOR_PARTITIONS - 1) / UserArguments::VECTOR_PARTITIONS + UserArguments::VECTOR_PARTITIONS;
            int64_t memory_usage = 8 * ((max_entry + 7) / 8);
            return memory_usage;
        }
        
        void InitializeMemoryUsage() {
            LocalStatistics::register_mem_alloc_info((array_name + " VersionedArray").c_str(), buf_memory_usage / (1024 * 1024));
            LocalStatistics::register_mem_alloc_info((array_name + " VersionedArray BitMap").c_str(), bitmap_memory_usage / (1024 * 1024));
        }

        void InitializeDeltas() {
            deltas.clear();
            std::vector<Turbo_bin_aio_handler*> tmp_vec;
            tmp_vec.resize(UserArguments::NUM_THREADS);
            for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                std::string delta_array_path2 = array_path + "_" + std::to_string(0) + "_" + std::to_string(k);
                tmp_vec[k] = new Turbo_bin_aio_handler(delta_array_path2.c_str(), true, true, true, true);
            }
            deltas.push_back(tmp_vec);

            VECTOR<VECTOR<int64_t>> tmp_vec2;
            tmp_vec2.resize(1);
            tmp_vec2[0].resize(UserArguments::NUM_THREADS, 0);
            deltas_offset.push_back(tmp_vec2);
            deltas_dead_range_per_superstep.resize(1);
            deltas_dead_range_per_superstep[0].Set(-1, -1);
            first_snapshot_version.resize(1);
            first_snapshot_version[0] = 0;

            VECTOR<int64_t> tmp;
            tmp.resize(1, 0);
            total_read_io.push_back(tmp);
            total_write_io.push_back(tmp);
            
            VECTOR<double> tmp2;
            tmp.resize(1, 0.0);
            non_overlapped_time.push_back(tmp2);
        }

        void FinalizeDeltas(bool rm, int64_t& total_delta_size) {
            std::vector<std::vector<int64_t>> delta_size_per_superstep;
            delta_size_per_superstep.resize(deltas_offset.size());
            for (int i = 0; i < deltas_offset.size(); i++) {
                delta_size_per_superstep[i].resize(deltas_offset[i].size(), 0);
                for (int j = 0; j < deltas_offset[i].size(); j++) {
                    for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                        if (i == 0) delta_size_per_superstep[i][j] += deltas_offset[i][j][k];
                        else delta_size_per_superstep[i][j] += (deltas_offset[i][j][k] - deltas_offset[i-1][j][k]);
                    }
                }
            }
            for (auto& vec: deltas) {
                for (auto& ptr: vec) {
                    if (ptr == NULL) continue;
                    total_delta_size += ptr->file_size();
                    ptr->Close(rm);
                    //ptr->Close(false);
                    delete ptr;
                }
            }
#ifdef IO_LOGGING 
            std::string print = "Delta size statistics (" + array_path + ") -> \n";
            for (int i = 0; i < delta_size_per_superstep.size(); i++) {
                for (int j = 0; j < delta_size_per_superstep[i].size(); j++) {
                    MPI_Allreduce(MPI_IN_PLACE, &delta_size_per_superstep[i][j], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
                    print += std::to_string(delta_size_per_superstep[i][j]) + ", ";
                }
                print += "\n";
            }
            system_fprintf(0, stdout, "%s\n", print.c_str());
#endif
            deltas.clear();

#ifdef IO_LOGGING
            std::string print_io = "[" + std::to_string(PartitionStatistics::my_machine_id()) + "] Read IO statistics (" + array_path + ") -> \n";
            for (int i = 0; i < total_read_io.size(); i++) {
                for (int j = 0; j < total_read_io[i].size(); j++) {
                    MPI_Allreduce(MPI_IN_PLACE, &total_read_io[i][j], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
                    print_io += std::to_string(total_read_io[i][j]) + ",";
                }
                print_io += "\n";
            }
            print_io += "[" + std::to_string(PartitionStatistics::my_machine_id()) + "] Write IO statistics (" + array_path + ") -> \n";
            for (int i = 0; i < total_write_io.size(); i++) {
                for (int j = 0; j < total_write_io[i].size(); j++) {
                    MPI_Allreduce(MPI_IN_PLACE, &total_write_io[i][j], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
                    print_io += std::to_string(total_write_io[i][j]) + ",";
                }
                print_io += "\n";
            }
            system_fprintf(0, stdout, "%s\n", print_io.c_str());
#endif
        }
        
        void writeVersionedArrayIOWaitTime(JsonLogger*& json_logger) {
            std::string key;
            key = array_name + "_IOWait";
            json_logger->InsertKeyValue(key.c_str(), json_io_wait_per_ss_vector);
            key = array_name + "_Merge";
            json_logger->InsertKeyValue(key.c_str(), json_merge_per_ss_vector);
        }
        
        void logVersionedArrayIOWaitTime() {
            json_io_wait_per_ss_vector.push_back(tim.get_timer(0));
            json_merge_per_ss_vector.push_back(tim.get_timer(1));
            tim.clear_all_timers();
        }

        void aggregateReadIO(JsonLogger*& json_logger) {
#ifdef IO_LOGGING
            std::vector<json11::Json> temp;
            std::string key = array_name + "_ReadIO";
            for (int i = 0; i < total_read_io.size(); i++) {
                for (int j = 0; j < total_read_io[i].size(); j++) {
                    MPI_Allreduce(MPI_IN_PLACE, &total_read_io[i][j], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
                    temp.push_back(total_read_io[i][j]);
                }
            }
            json_logger->InsertKeyValue(key.c_str(), temp);
#endif
        }
        
        void aggregateWriteIO(JsonLogger*& json_logger) {
#ifdef IO_LOGGING
            std::vector<json11::Json> temp;
            std::string key = array_name + "_WriteIO";
            for (int i = 0; i < total_write_io.size(); i++) {
                for (int j = 0; j < total_write_io[i].size(); j++) {
                    MPI_Allreduce(MPI_IN_PLACE, &total_write_io[i][j], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
                    temp.push_back(total_write_io[i][j]);
                }
            }
            json_logger->InsertKeyValue(key.c_str(), temp);
#endif
        }
        
        void aggregateNonOverlappedTime(JsonLogger*& json_logger) {
#ifdef REPORT_PROFILING_TIMERS
            std::vector<json11::Json> temp;
            std::vector<json11::Json> temp_max;
            std::vector<json11::Json> temp_min;
            std::string key = array_name + "_NonOverlappedTime";
            std::string key_max = array_name + "_NonOverlappedTimeMax";
            std::string key_min = array_name + "_NonOverlappedTimeMin";
            std::vector<std::vector<double>> non_overlapped_time_max;
            std::vector<std::vector<double>> non_overlapped_time_min;
            non_overlapped_time_max.resize(non_overlapped_time.size());
            non_overlapped_time_min.resize(non_overlapped_time.size());
            for (int i = 0; i < non_overlapped_time.size(); i++) {
                non_overlapped_time_max[i].resize(non_overlapped_time[i].size(), 0.0);
                non_overlapped_time_min[i].resize(non_overlapped_time[i].size(), 0.0);
                for (int j = 0; j < non_overlapped_time[i].size(); j++) {
                    MPI_Allreduce(&non_overlapped_time[i][j], &non_overlapped_time_max[i][j], 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
                    MPI_Allreduce(&non_overlapped_time[i][j], &non_overlapped_time_min[i][j], 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
                    MPI_Allreduce(MPI_IN_PLACE, &non_overlapped_time[i][j], 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
                    non_overlapped_time[i][j] = non_overlapped_time[i][j] / PartitionStatistics::num_machines();
                    temp.push_back(non_overlapped_time[i][j]);
                    temp_max.push_back(non_overlapped_time_max[i][j]);
                    temp_min.push_back(non_overlapped_time_min[i][j]);
                }
            }
            json_logger->InsertKeyValue(key.c_str(), temp);
            json_logger->InsertKeyValue(key_max.c_str(), temp_max);
            json_logger->InsertKeyValue(key_min.c_str(), temp_min);
#endif
        }

        template <typename CALLER_TYPE, typename FUNPTR_TYPE>
        void ReadDeltasAndProcessUsingDiskAio(int i, int j, size_t buffer_unit_size, CALLER_TYPE* caller, FUNPTR_TYPE* f, void* buf_to_construct, bool construct_next, bool mark_changed=false) {
            /*
             *  TODO - total_io array is not created in ReadOnly VersionedArray?
            for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                total_io[UserArguments::UPDATE_VERSION][UserArguments::SUPERSTEP] += deltas[i][j][k]->file_size();
            }
            */
                
            //fprintf(stdout, "(%d, %d) [ReadDeltasAndProcessUsingDiskAio] %s\n", i, j, array_name.c_str());

#ifdef HYBRID_COST_MODEL
            //fprintf(stdout, "[%ld] %s deltas_dead_range_per_superstep[%d] = [%ld, %ld] contains %d ? %s\n", PartitionStatistics::my_machine_id(), array_name.c_str(), j, deltas_dead_range_per_superstep[j].GetBegin(), deltas_dead_range_per_superstep[j].GetEnd(), i, deltas_dead_range_per_superstep[j].contains(i) ? "true" : "false");
            if (deltas_dead_range_per_superstep[j].contains(i) && i != first_snapshot_version[j]) return;
#endif
            int64_t io_total = 0;
            turbo_timer read_tim;

            read_tim.start_timer(0);
            diskaio::DiskAioInterface* my_io = Turbo_bin_aio_handler::GetMyDiskIoInterface(true);

            ALWAYS_ASSERT(deltas.size() > j);
            ALWAYS_ASSERT(deltas[j].size() == UserArguments::NUM_THREADS);
            ALWAYS_ASSERT(deltas_offset.size() > i);
            ALWAYS_ASSERT(deltas_offset[i].size() > j);
            ALWAYS_ASSERT(deltas_offset[i][j].size() == UserArguments::NUM_THREADS);

            int64_t cur_size_to_read = 0;
            int64_t file_offset;
            int64_t k = 0;
            int64_t prev_offset = (i == 0) ? 0 : deltas_offset[i-1][j][k];
            while (k < UserArguments::NUM_THREADS) {
                INVARIANT(deltas_offset[i][j][k] >= 0);
                INVARIANT(deltas[j][k] != NULL);
                if ((deltas_offset[i][j][k] - prev_offset) == 0) {
                    if (++k >= UserArguments::NUM_THREADS) break;
                    prev_offset = (i == 0) ? 0 : deltas_offset[i-1][j][k];
                    continue;
                }
                
                // Issue Aio
                cur_size_to_read = (deltas_offset[i][j][k] > prev_offset + buffer_unit_size) ? buffer_unit_size : deltas_offset[i][j][k] - prev_offset;
                char* buf = RequestRespond::GetPerThreadBuffer([&]() {
                                Turbo_bin_aio_handler::WaitMyPendingDiskIO(my_io);
                            });
                //fprintf(stdout, "\tReadDeltasAndProcessUsingDiskAio(%d, %d) %s offset = %ld read size = %ld\n", i, j, deltas[j][k]->GetFilePath().c_str(), prev_offset, cur_size_to_read);
                deltas[j][k]->Read(prev_offset, cur_size_to_read, buf, (void*) caller, (void*) f, buf_to_construct, construct_next, mark_changed);
#ifdef IO_LOGGING
                if (total_read_io.size() <= UserArguments::UPDATE_VERSION) {
                    fprintf(stdout, "[%ld] total_read_io.size = %ld, up_v = %d\n", PartitionStatistics::my_machine_id(), total_read_io.size(), UserArguments::UPDATE_VERSION);
                    INVARIANT(false);
                }
                if (total_read_io[UserArguments::UPDATE_VERSION].size() <= UserArguments::SUPERSTEP) {
                    fprintf(stdout, "[%ld] total_read_io[%d].size = %ld, up_v = %d, ss = %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, total_read_io[UserArguments::UPDATE_VERSION].size(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
                    INVARIANT(false);
                }
                std::atomic_fetch_add((std::atomic<int64_t>*) &total_read_io[UserArguments::UPDATE_VERSION][UserArguments::SUPERSTEP], cur_size_to_read);
#endif
                prev_offset += cur_size_to_read;
                io_total += cur_size_to_read;
                
                // If offset is end of file, find the next one.
                if (prev_offset == deltas_offset[i][j][k]) {
                    while (++k < UserArguments::NUM_THREADS) {
                        prev_offset = i == 0 ? 0 : deltas_offset[i-1][j][k];
                        if ((deltas_offset[i][j][k] - prev_offset) != 0) break;
                    }
                }
            }
            read_tim.stop_timer(0);
                
            Turbo_bin_aio_handler::WaitMyPendingDiskIO(my_io);
            //fprintf(stdout, "[%ld] Versioned Array %s IO %.2f MB, %.3f sec, %.3f MB/s\n", PartitionStatistics::my_machine_id(), array_name.c_str(), (double) io_total / (double)(1024 * 1024), read_tim.get_timer(0), (double) io_total / (1024 * 1024 * read_tim.get_timer(0)));
        }
        
        void Init(const std::string& file_name, std::size_t num_entries, bool open, bool use_main, bool is_ov=false, bool maintain_prev_ss=false, bool advance_superstep=true, bool construct_next_ss_array_async=true) {
            is_ov_ = is_ov;
            open_ = open;
            use_main_ = use_main;
            advance_superstep_ = advance_superstep;
            construct_next_ss_array_async_ = construct_next_ss_array_async;
            constructed_already_ = false;
            maintain_prev_ss_ = maintain_prev_ss;
            array_path = file_name;
            //array_path = "/mnt/tmpfs/" + array_path;
            buf_length = num_entries;
            total_io_read = total_io_write = 0;
            current_version.SetUpdateVersion(0);
            current_version.SetSuperstepVersion(-1);
            num_max_discard_deltas_in_a_snapshot = 5;   // XXX
            num_performed_discard_deltas_in_a_snapshot = 0;
            identity_element = idenelem<T>(op);
            hybrid_mode = getenv("HYBRID_COST_MODEL_MODE");
            if (hybrid_mode == NULL)
                system_fprintf(0, stdout, "Run Hybrid Cost Model Mode = ALL\n");
            else
                system_fprintf(0, stdout, "Run Hybrid Cost Model Mode = %s\n", hybrid_mode);
            
            /*if (open) {
                INVARIANT(false);
                // TODO
            }*/
            
            std::vector<std::string> temp_str;
            std::stringstream ss(file_name);
            std::string temp;

            while (getline(ss, temp, '/')) {
                temp_str.push_back(temp);
            }
            array_name = temp_str[temp_str.size() - 2] + "_" + temp_str[temp_str.size() - 1];
            
            ComputeMemoryUsage();
        }

        void Create() {
            InitializeMainAndBuffer(array_path, open_);
            InitializeDeltas();
            InitializeFlags();
            InitializePerThreadBuffers();
            InitializeMemoryUsage();
            if (open_) {
                INVARIANT(false);
                // TODO
            }
        }

        void Close(bool rm) {
            FreeMainAndBuffer(rm);
            FreeFlags();
            int64_t total_delta_size = 0;
            
            if (GetRef() != NULL) {
            /*
                fprintf(stdout, "[%ld] VersionedArray(ReadOnly) %s Elapsed: Construct %.3f (ConstructFromTo %.3f ConstructFromToReadOnly %.3f) ConstructAndFlush %.3f (ConstructAndFlushFromTo %.3f) FlushAll %.3f (FlushPerThreadWriteBuffers %.3f) Total Elapsed: %.3f Total I/O (Read: %ld Write: %ld)\n",
                        PartitionStatistics::my_machine_id(),
                        array_path.c_str(),
                        tim.get_timer(4),
                        tim.get_timer(3),
                        tim.get_timer(2),
                        tim.get_timer(5),
                        tim.get_timer(0),
                        tim.get_timer(1),
                        tim.get_timer(6),
                        tim.get_timer(1) + tim.get_timer(4) + tim.get_timer(5),
                        total_io_read, total_io_write);
            */
                return;
            }
            FinalizeDeltas(rm, total_delta_size);

            /*
            fprintf(stdout, "[%ld] VersionedArray %s Elapsed: Construct %.3f (ConstructFromTo %.3f ConstructFromToReadOnly %.3f) ConstructAndFlush %.3f (ConstructAndFlushFromTo %.3f) FlushAll %.3f (FlushPerThreadWriteBuffers %.3f) Total Elapsed: %.3f Total I/O (Read: %ld Write: %ld) Total {Main, Delta} Size %ld, %ld\n",
                    PartitionStatistics::my_machine_id(),
                    array_path.c_str(),
                    tim.get_timer(4),
                    tim.get_timer(3),
                    tim.get_timer(2),
                    tim.get_timer(5),
                    tim.get_timer(0),
                    tim.get_timer(1),
                    tim.get_timer(6),
                    tim.get_timer(1) + tim.get_timer(4) + tim.get_timer(5),
                    total_io_read, total_io_write,
                    total_main_size, total_delta_size);
            */
        }
        
        void __ConstructAndFlushFromTo(Version from, Version to, VersionedArrayBase<T, op>* prev, bool construct_prev, bool flush) {
            // Four cases
            // {has_future_job ? true : false} X {construct_prev ? true : false}
            // (1) (true , true ): IV
            // (2) (true , false): OV, (or IV for which don't have previous version)
            // (3) (false, true ): IV for which don't have next buffer (ex. OutDegree)
            // (4) (false, false): IV for which don't have next buffer & previous version (ex. Starting Vertices)
            
            /*if (!is_ov_) {
                TwoLevelBitMap<int64_t>::UnionToRight(this->to_flush_flags, this->changed_flags);
                //this->changed_flags.CopyFrom(this->to_flush_flags);
                this->discard_flags.CopyFrom(this->to_flush_flags);
            }*/
            
            if (has_future_job) _ConstructWithNextBufferAndFlushFromTo(from, to, prev, construct_prev, flush);
            else _ConstructAndFlushFromTo(from, to, prev, construct_prev, flush);
        }

        void _ConstructAndFlushFromTo(Version from, Version to, VersionedArrayBase<T, op>* prev, bool construct_prev, bool flush) {
            //fprintf(stdout, "\tReadAndMergeAndFlush %s\t [%ld, %ld] ~ [%ld, %ld] construct_prev %s, flush %s\n", array_name.c_str(), (int64_t) from.GetUpdateVersion(), (int64_t) from.GetSuperstepVersion(), (int64_t) to.GetUpdateVersion(), (int64_t) to.GetSuperstepVersion(), construct_prev ? "true" : "false", flush ? "true" : "false");
#ifdef INCREMENTAL_LOGGING
            if (PartitionStatistics::my_first_node_id() <= TARGET_VID && PartitionStatistics::my_last_node_id() >= TARGET_VID) {
                fprintf(stdout, "\tReadAndMergeAndFlush %s\t [%ld, %ld] ~ [%ld, %ld]\n", array_path.c_str(), (int64_t) from.GetUpdateVersion(), (int64_t) from.GetSuperstepVersion(), (int64_t) to.GetUpdateVersion(), (int64_t) to.GetSuperstepVersion());
            }
#endif
            for (int j = from.GetSuperstepVersion(); j <= to.GetSuperstepVersion(); j++) {
                for (int i = from.GetUpdateVersion(); i <= to.GetUpdateVersion(); i++) {
                    if (i >= deltas_offset.size()) continue;
                    if (j >= deltas_offset[i].size()) continue;
                    if (i == 0 && j == 0) {
                        if (to.GetUpdateVersion() == 0 || !construct_prev) {
                            CopyMainBuffer();
                        } else {
                            CopyMainBufferFromRef(prev);
                        }
                    } 
                    if (i < to.GetUpdateVersion()) { // Update Prev
                        ReadUpdatesAndConstructWithoutFlush(i, j, false, construct_prev, false, prev);
                    }
                    if (i > 0 && i == to.GetUpdateVersion()) { // Update Current
                        if (construct_prev) CopyBuffer(prev);
                        ReadUpdatesAndConstructWithoutFlush(i, j, false, false, true, prev);
                        //ReadUpdatesAndConstructWithoutFlush(i, j, false, false, false, prev);
                    }
                }
            }

            if (flush) FlushAll_(to);

            // ???
            if (to.GetUpdateVersion() == 0) {
                dirty_flags.ClearAll();
            }
        }

        void _ConstructWithNextBufferAndFlushFromTo(Version from, Version to, VersionedArrayBase<T, op>* prev, bool construct_prev, bool flush) {
#ifdef INCREMENTAL_LOGGING
            fprintf(stdout, "\t_ConstructWithNextBufferAndFlushFromTo %s\t [%ld, %ld] ~ [%ld, %ld]\n", array_name.c_str(), (int64_t) from.GetUpdateVersion(), (int64_t) from.GetSuperstepVersion(), (int64_t) to.GetUpdateVersion(), (int64_t) to.GetSuperstepVersion());
#endif
            
            INVARIANT(has_future_job);
            tim.start_timer(0);
            reqs_to_wait.get();
            tim.stop_timer(0);
            has_future_job = false;
            // TODO MergeWithNext(from, to, prev) constructs prev and mark flush flags when construct current
            // But MergeWithNext(from, to) just construct current without mark any flags
            // Is it need to mark any flags when construct current only? (If needed, it should be implemented)
            tim.start_timer(1);
            if (construct_prev) MergeWithNext(from, to, prev);
            else MergeWithNext(from, to);
            tim.stop_timer(1);

            // Flush deltas to version [to.GetUpdateVersion(), to.GetSuperstepVersion()]
            tim.start_timer(2);
            if (flush) FlushAll_(to);
            tim.stop_timer(2);
//#ifdef PRINT_PROFILING_TIMERS
            fprintf(stdout, "[%ld] (%d, %d) %s _ConstructWithNextBufferAndFlushFromTo %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, array_name.c_str(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2));
//#endif

            // ???
            if (to.GetUpdateVersion() == 0) {
                dirty_flags.ClearAll();
            }

            // Read deltas and construct next buffer asynchronously
            if (!construct_next_ss_array_async_) return;
            toggle_ = (toggle_ + 1) % 2;
            reqs_to_wait = Aio_Helper::EnqueueAsynchronousTask(VersionedArrayBase<T, op>::call_ConstructNextAsync, this, from, to);
            has_future_job = true;
        }
        
        void ConstructNextAsync(Version from, Version to) {
            //system_fprintf(0, stdout, "ConstructNextAsync %s [%d, %d] ~ [%d, %d]\n", array_name.c_str(), from.GetUpdateVersion(), from.GetSuperstepVersion(), to.GetUpdateVersion(), to.GetSuperstepVersion());
            ClearAllNext();
            CheckIsSparse(to);
            ALWAYS_ASSERT(from.GetSuperstepVersion() == to.GetSuperstepVersion());  // we only have this usage
            for (int j = from.GetSuperstepVersion(); j <= to.GetSuperstepVersion(); j++) {
                for (int i = from.GetUpdateVersion(); i <= to.GetUpdateVersion(); i++) {
                    if (j >= deltas_offset[i].size()) continue;
                    if (i == 0 && j == 0) CopyMainBuffer();
                    ReadUpdatesAndConstructWithoutFlush(i, j, true, false, false);
               
#ifdef HYBRID_COST_MODEL
                    if (i == GetFirstDeltaSnapshot(j)) {
                        first_snapshot_delta_flags[toggle_].CopyFrom(next_flags[toggle_], true);
                        next_flags[toggle_].ClearAll();
                    }
#endif
                }
            }
            //PostProcessingAfterConstruct();
        }
        
        void CallConstructNextSuperStepArrayAsync(int update_version, int superstep_version, bool run_static_processing=false) {
            //system_fprintf(0, stdout, "Call ConstructNextSuperStepAsync %s [%d, %d], run_static_processing %s\n", array_name.c_str(), update_version, superstep_version, run_static_processing ? "true" : "false");
            if (update_version < 1 || run_static_processing) return;
            INVARIANT(superstep_version == 0);
            Version from, to;
            int ii,jj;
            
            //prev.CopyDeltasFromReference();

            if (update_version >= current_version.GetUpdateVersion() && superstep_version >= current_version.GetSuperstepVersion()) {
                from = current_version;
                from.SetUpdateVersion(from.GetUpdateVersion() + 1);
                to.SetSuperstepVersion(from.GetSuperstepVersion());
                to.SetUpdateVersion(update_version);
            
                int64_t cnt = CountNumDeltasToMerge(from, to);
                if (cnt > 0) {
                   if (is_ov_) {
                        from.SetSuperstepVersion(-1);
                        to.SetSuperstepVersion(-1);
                   }
                   reqs_to_wait = Aio_Helper::EnqueueAsynchronousTask(VersionedArrayBase<T, op>::call_ConstructNextAsync, this, from, to);
                   has_future_job = true;
                }

                ii = 0;
                jj = current_version.GetSuperstepVersion() + 1;
            } else {
                ii = 0;
                jj = 0;
            }

            from.SetSuperstepVersion(jj);
            to.SetSuperstepVersion(superstep_version);
            from.SetUpdateVersion(ii);
            to.SetUpdateVersion(update_version);
                
            int64_t cnt = CountNumDeltasToMerge(from, to);
            if (cnt > 0) {
                if (is_ov_) {
                    from.SetSuperstepVersion(-1);
                    to.SetSuperstepVersion(-1);
                }
                reqs_to_wait = Aio_Helper::EnqueueAsynchronousTask(VersionedArrayBase<T, op>::call_ConstructNextAsync, this, from, to);
                has_future_job = true;
            }

            return;
        }

        void FlushAll(int update_version, int superstep_version, bool apply_hybrid_model=true) {
            //fprintf(stdout, "[%ld] FlushAll %s [%d, %d]\n", PartitionStatistics::my_machine_id(), array_name.c_str(), update_version, superstep_version);
            Version to(update_version, superstep_version);
            FlushAll_(to, apply_hybrid_model);
        }
        
        void FlushAll_(Version to, bool apply_hybrid_model=true) {
#ifdef NO_LOGGING_VERSIONED_ARRAY
          if (UserArguments::QUERY_ONGOING) return;
#endif
            if (constructed_already_) return;
            //system_fprintf(0, stdout, "FlushAll %s\t%ld\t%ld\n", this->array_path.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion());
#ifdef HYBRID_COST_MODEL
            //if (to.GetUpdateVersion() == 0 || UserArguments::INC_STEP == -1 || UserArguments::SUPERSTEP == 0) { // TODO change this to to.GetUpdateVersion() == 0
            if (to.GetUpdateVersion() == 0 || UserArguments::INC_STEP == -1) { // TODO change this to to.GetUpdateVersion() == 0
                CreateDelta(to);
                return;
            }

            turbo_timer tim;
            
            int best_strategy;
            
            tim.start_timer(0);
            if (is_ov_) {
                // Cost[0] = Discard OV = Union of (B, C, to_flush)
                // Cost[3] = Create OV = Union of (C)
                // Cost[4] = Merge OV = Union of (C, to_flush)
                char toggle = (toggle_ + 1) % 2;
                //costs[3] = to_flush_flags.GetTotalEstimation();
                costs[3] = to_flush_flags.GetTotal();
                if (this->costs[1] + this->costs[2] == 0) {
                    costs[0] = costs[3];
                } else {
                    costs[0] = TwoLevelBitMap<int64_t>::GetTotalOfUnion(this->to_flush_flags, this->next_flags[toggle], this->first_snapshot_delta_flags[toggle]);
                    //costs[0] = TwoLevelBitMap<int64_t>::GetTotalEstimationOfUnion(this->to_flush_flags, this->next_flags[toggle], this->first_snapshot_delta_flags[toggle]);
                }
                if (this->costs[2] == 0) {
                    costs[4] = costs[3];
                } else {
                    costs[4] = TwoLevelBitMap<int64_t>::GetTotalOfUnion(this->next_flags[toggle], this->to_flush_flags);
                    //costs[4] = TwoLevelBitMap<int64_t>::GetTotalEstimationOfUnion(this->next_flags[toggle], this->to_flush_flags);
                }
                best_strategy = FindBestStrategyForFlushForOV(apply_hybrid_model);
            } else {
                best_strategy = FindBestStrategyForFlush();
            }
            tim.stop_timer(0);
            if ((UserArguments::UPDATE_VERSION != 0) && ((UserArguments::UPDATE_VERSION % 50) == 0)) {
                std::string mode = std::string(hybrid_mode);
                //if (mode != "CreateOrMerge" && mode != "CreateDelta") {
                    best_strategy = 2;
                //}
            }
            tim.start_timer(1);
            switch (best_strategy) {
                case 0: DiscardDeltas(to);
                        num_performed_discard_deltas_in_a_snapshot++;
                        break;
                case 1: CreateDelta(to);
                        break;
                case 2: MergeDeltas(to);
                        break;
                default: INVARIANT(false);
            }
            tim.stop_timer(1);
            tim.start_timer(2);
            if (!is_ov_) discard_flags.ClearAll();
            // XXX maybe we don't need this anymore check it
            /*if (!is_ov_) {
                next_flags.ClearAll();
                first_snapshot_delta_flags.ClearAll();
            }*/
            tim.stop_timer(2);
//#ifdef PRINT_PROFILING_TIMERS
            fprintf(stdout, "[%ld] (%d, %d) Best Strategy for %s = %d, %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, array_name.c_str(), best_strategy, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2));
//#endif
#else
            CreateDelta(to);
#endif
        }

        void CreateDelta(Version to) {
            //system_fprintf(0, stdout, "[%ld] %s CreateDelta to [%ld, %ld], total %ld\n", PartitionStatistics::my_machine_id(), array_name.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion(), this->to_flush_flags.GetTotal());
            turbo_timer createdelta_timer;
            if (to.GetUpdateVersion() == 0 && to.GetSuperstepVersion() == 0 && !use_main_ && !is_ov_) {
                this->to_flush_flags.ClearAll();
            } else {
                createdelta_timer.start_timer(0);
                //sleep(5);
                this->to_flush_flags.InvokeIfMarked([&](node_t idx) {
                        T val = GetValue(idx);
                        Write(idx, val, to.GetUpdateVersion(), to.GetSuperstepVersion());
                }, false, UserArguments::NUM_THREADS, [&]() {
                        createdelta_timer.start_timer(1);
                        Turbo_bin_aio_handler::WaitMyPendingDiskIO(false, 0);   
                        createdelta_timer.stop_timer(1);
                });
                //sleep(5);
                createdelta_timer.stop_timer(0);
                createdelta_timer.start_timer(2);
                this->to_flush_flags.ClearAll();
                FlushPerThreadWriteBuffers(to);
                createdelta_timer.stop_timer(2);
            }
            fprintf(stdout, "[%ld] %s CreateDelta to [%ld, %ld], total %ld, %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), array_name.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion(), this->to_flush_flags.GetTotal(), createdelta_timer.get_timer(0), createdelta_timer.get_timer(1), createdelta_timer.get_timer(2));
#ifdef PRINT_PROFILING_TIMERS
            //fprintf(stdout, "[%ld] %s CreateDelta to [%ld, %ld], total %ld, %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), array_name.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion(), this->to_flush_flags.GetTotal(), createdelta_timer.get_timer(0), createdelta_timer.get_timer(1), createdelta_timer.get_timer(2));
#endif
        }

#ifdef HYBRID_COST_MODEL
        void DiscardDeltas(Version to) {
            if (is_ov_) {
                // For OV
                char toggle = (toggle_ + 1) % 2;
                TwoLevelBitMap<int64_t>::UnionToRight(this->first_snapshot_delta_flags[toggle], this->next_flags[toggle]);
                TruncateDeltaFileIfNeeded(to.GetUpdateVersion(), to.GetSuperstepVersion(), true);
                //system_fprintf(0, stdout, "[%ld] %s DiscardDelta to [%ld, %ld], total %ld %ld\n", PartitionStatistics::my_machine_id(), array_name.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion(), this->next_flags[toggle].GetTotal(), TwoLevelBitMap<int64_t>::GetTotalOfUnion(this->next_flags[toggle], this->to_flush_flags));
                TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                        T val = GetValue(idx);
                        if (val == identity_element) return;
                        Write(idx, val, to.GetUpdateVersion(), to.GetSuperstepVersion());
                }, this->next_flags[toggle], this->to_flush_flags);
                this->to_flush_flags.ClearAll();
                FlushPerThreadWriteBuffers(to);
            } else {
                // For IV
                if (to.GetUpdateVersion() == 0 && to.GetSuperstepVersion() == 0 && !use_main_ && !is_ov_) {
                    this->to_flush_flags.ClearAll();
                } else {
                    TruncateDeltaFileIfNeeded(to.GetUpdateVersion(), to.GetSuperstepVersion(), true);
                    this->discard_flags.InvokeIfMarked([&](node_t idx) {
                            T val = GetValue(idx);
                            Write(idx, val, to.GetUpdateVersion(), to.GetSuperstepVersion());
                            }, false, UserArguments::NUM_THREADS, [&]() {
                            Turbo_bin_aio_handler::WaitMyPendingDiskIO(false, 0);   
                            });
                    this->discard_flags.ClearAll();
                    this->to_flush_flags.ClearAll();
                    FlushPerThreadWriteBuffers(to);
                }
            }
            
            deltas_dead_range_per_superstep[to.GetSuperstepVersion()].Set(0, to.GetUpdateVersion() - 1);
            first_snapshot_version[to.GetSuperstepVersion()] = to.GetUpdateVersion();
            //system_fprintf(0, stdout, "[%ld] %s DiscardDelta to [%d, %d], dead_range [%ld, %ld]\n", PartitionStatistics::my_machine_id(), array_name.c_str(), to.GetUpdateVersion(), to.GetSuperstepVersion(), deltas_dead_range_per_superstep[to.GetSuperstepVersion()].GetBegin(), deltas_dead_range_per_superstep[to.GetSuperstepVersion()].GetEnd());
        }

        void MergeDeltas(Version to) {
            // In case of OV, we only see next_flag for merge
            // In case of IV, we flush (next_flags Union to_flush_flags)
            int u_v = to.GetUpdateVersion(), s_v = to.GetSuperstepVersion();
            if (is_ov_) {
                TruncateDeltaFileIfNeeded(to.GetUpdateVersion(), to.GetSuperstepVersion());
                /*to.SetUpdateVersion(to.GetUpdateVersion() - 1);
                for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                    deltas_offset[u_v-2][s_v][k] = deltas_offset[u_v-1][s_v][k];
                }
                this->next_flags[toggle_].InvokeIfMarked([&](node_t idx) {
                        Write(idx, to.GetUpdateVersion(), to.GetSuperstepVersion());
                }, false, UserArguments::NUM_THREADS, [&]() {
                        Turbo_bin_aio_handler::WaitMyPendingDiskIO(false, 0);   
                });*/
                char toggle = (toggle_ + 1) % 2;
                //fprintf(stdout, "MergeDeltas %s next_flags %ld flush_flags %ld\n", array_name.c_str(), this->next_flags[toggle].GetTotal(), this->to_flush_flags.GetTotal());
                TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                        T val = GetValue(idx);
                        Write(idx, val, to.GetUpdateVersion(), to.GetSuperstepVersion());
                }, this->next_flags[toggle], this->to_flush_flags);
                this->to_flush_flags.ClearAll();
            } else {
                TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
                        T val = GetValue(idx);
                        Write(idx, val, to.GetUpdateVersion(), to.GetSuperstepVersion());
                }, this->next_flags[toggle_], this->to_flush_flags);
                this->to_flush_flags.ClearAll();
            }
            FlushPerThreadWriteBuffers(to);
            
            int64_t dead_begin, dead_end; 
            dead_end = to.GetUpdateVersion() - 1;
            
            if (deltas_dead_range_per_superstep[to.GetSuperstepVersion()].GetBegin() == -1) {
                dead_begin = 1;
            } else {
                dead_begin = deltas_dead_range_per_superstep[to.GetSuperstepVersion()].GetBegin();
            }
            deltas_dead_range_per_superstep[to.GetSuperstepVersion()].Set(dead_begin, dead_end);
            //system_fprintf(0, stdout, "%s deltas_dead_range[%d] = [%d, %d]\n", array_name.c_str(), to.GetSuperstepVersion(), dead_begin, dead_end);
        }

        int GetFirstDeltaSnapshot(int superstep) {
            //INVARIANT(superstep < first_snapshot_version.size());
            if (superstep >= first_snapshot_version.size())
                return 0;
            return first_snapshot_version[superstep];
            
            /*if (deltas_dead_range_per_superstep[superstep].GetBegin() == 0) {
                return deltas_dead_range_per_superstep[superstep].GetEnd() + 1;
            } else {
                return 0;
            }*/
        }

        void TruncateDeltaFileIfNeeded(int update_version, int superstep_version, bool force_truncate=false) {
            int64_t total_file_size = 0;
            for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) total_file_size += deltas[superstep_version][tid]->file_size();

            int64_t truncate_begin, truncate_end; 
            truncate_begin = force_truncate ? 0 : GetFirstDeltaSnapshot(superstep_version) + 1;
            truncate_end = update_version - 1;
            if (truncate_begin > truncate_end) return;
            //fprintf(stdout, "[%ld] (%ld, %ld) %s TruncateDeltaFile from %ld to %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, array_name.c_str(), truncate_begin, truncate_end);

            for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
                int64_t truncate_size = force_truncate ? 0 : deltas_offset[truncate_begin-1][superstep_version][tid];
                deltas[superstep_version][tid]->Truncate(truncate_size);
                for (int i = truncate_begin; i <= truncate_end; i++)
                    deltas_offset[i][superstep_version][tid] = truncate_size;
            }
        }
#endif
        
        void Write(int64_t idx, T val, int update_version, int superstep_version) {
            //T val = GetValue(idx);
            int64_t tid = omp_get_thread_num();
            //fprintf(stdout, "FlushWrite buf[%ld] = %s into (i=%ld, j=%ld)\t%s\n", (int64_t) idx + PartitionStatistics::my_first_node_id(), std::to_string(val).c_str(), (int64_t) update_version, (int64_t) superstep_version, this->deltas[superstep_version][tid]->GetFilePath().c_str());
#ifdef INCREMENTAL_LOGGING
            //if (INCREMENTAL_DEBUGGING_TARGET(idx + PartitionStatistics::my_first_node_id())) {
                int64_t tid = omp_get_thread_num();
                fprintf(stdout, "FlushWrite buf[%ld] = %s into (i=%ld, j=%ld)\t%s\n", (int64_t) idx + PartitionStatistics::my_first_node_id(), std::to_string(val).c_str(), (int64_t) update_version, (int64_t) superstep_version, this->deltas[superstep_version][tid]->GetFilePath().c_str());
                //fprintf(stdout, "FlushWrite buf[%ld] = %s, %s into (i=%ld, j=%ld)\t%s\n", (int64_t) idx + PartitionStatistics::my_first_node_id(), std::to_string(GET_VAL_64BIT(val)).c_str(), std::to_string(GET_CNT_64BIT(val)).c_str(), (int64_t) update_version, (int64_t) superstep_version, this->deltas[superstep_version][tid]->GetFilePath().c_str());
            //}
#endif
            //ALWAYS_ASSERT(buf[idx] == val);
            ALWAYS_ASSERT(idx >= 0);
            ALWAYS_ASSERT(idx < PartitionStatistics::my_num_internal_nodes());

            if (update_version == 0 && superstep_version == 0 && use_main_) {
                main[idx] = val;
            } else {
                DeltaMessage<T> upd(idx, val);
                int64_t tid = omp_get_thread_num();
                ALWAYS_ASSERT (tid >= 0 && tid < UserArguments::NUM_THREADS);
                ALWAYS_ASSERT (tid >= 0 && tid < UserArguments::NUM_THREADS);

                if (idx_in_per_thread_buffer[tid].idx == PER_THREAD_BUFFER_SIZE) {
                    this->deltas[superstep_version][tid]->Append(sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE, (char*) ptr_to_per_thread_buffer[tid], (void*) &VersionedArrayBase<T, op>::ReturnPerThreadBuffer,
                        [&](diskaio::DiskAioInterface* my_io) {
                            if (RequestRespond::GetRemainingBuffersInPerThreadBufferInPortion() < 0.2f && my_io->GetNumOngoing() > TURBO_BIN_AIO_MAX_PENDING_REQS) {
                                Turbo_bin_aio_handler::WaitMyPendingDiskIO(my_io, 1);
                            }
                        });
#ifdef IO_LOGGING
                    /*if (total_write_io.size() <= update_version) {
                        fprintf(stdout, "[%ld] %s total_write_io.size = %ld, up_v = %d\n", PartitionStatistics::my_machine_id(), array_name.c_str(), total_write_io.size(), update_version);
                        INVARIANT(false);
                    }
                    if (total_write_io[update_version].size() <= superstep_version) {
                        fprintf(stdout, "[%ld] %s total_write_io[%d].size = %ld, up_v = %d, ss = %d\n", PartitionStatistics::my_machine_id(), array_name.c_str(), update_version, total_write_io[update_version].size(), update_version, superstep_version);
                        INVARIANT(false);
                    }*/
                    //std::atomic_fetch_add((std::atomic<int64_t>*) &total_write_io[update_version][superstep_version], (int64_t) (sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE));
#endif
                    ptr_to_per_thread_buffer[tid] = (DeltaMessage<T>*) RequestRespond::GetPerThreadBuffer([&]() {
                            Turbo_bin_aio_handler::WaitMyPendingDiskIO(false, 0);   
                        });
                    idx_in_per_thread_buffer[tid].idx = 0;
                }
                ptr_to_per_thread_buffer[tid][idx_in_per_thread_buffer[tid].idx] = upd;
                idx_in_per_thread_buffer[tid].idx++;
            }
        }
        
        void WriteFlush(int update_version, int superstep_version) {
            int64_t tid = omp_get_thread_num();
            if (idx_in_per_thread_buffer[tid].idx == 0) {
                this->deltas_offset[update_version][superstep_version][tid] = this->deltas[superstep_version][tid]->file_size();
                return;
            }
            
            DeltaMessage<T> upd;
            upd.dst_vid = -1;
            upd.message = idenelem<T>(op);
            if((sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE) % 4096 != 0) {
                fprintf(stdout, "%s sizeof = %ld, PER_TH = %ld\n", array_name.c_str(), sizeof(DeltaMessage<T>), PER_THREAD_BUFFER_SIZE);
            }
            INVARIANT((sizeof(DeltaMessage<T>) * PER_THREAD_BUFFER_SIZE) % 4096 == 0);
            while ((sizeof(DeltaMessage<T>) * idx_in_per_thread_buffer[tid].idx) % 4096 != 0) {
                ptr_to_per_thread_buffer[tid][idx_in_per_thread_buffer[tid].idx] = upd;
                idx_in_per_thread_buffer[tid].idx++;
            }
        
            INVARIANT(this->deltas[superstep_version][tid] != NULL);

            this->deltas[superstep_version][tid]->Append(sizeof(DeltaMessage<T>) * idx_in_per_thread_buffer[tid].idx, (char*) ptr_to_per_thread_buffer[tid], (void*) &VersionedArrayBase<T, op>::ReturnPerThreadBuffer,
                [&](diskaio::DiskAioInterface* my_io) {
                    if (RequestRespond::GetRemainingBuffersInPerThreadBufferInPortion() < 0.2f && my_io->GetNumOngoing() > TURBO_BIN_AIO_MAX_PENDING_REQS) {
                    //if (RequestRespond::GetRemainingBuffersInPerThreadBufferInPortion() < 0.2f) {
                        Turbo_bin_aio_handler::WaitMyPendingDiskIO(my_io, 1);
                    }
                });
#ifdef IO_LOGGING
            if (total_write_io.size() <= update_version) {
                fprintf(stdout, "[%ld] %s total_write_io.size = %ld, up_v = %d\n", PartitionStatistics::my_machine_id(), array_name.c_str(), total_write_io.size(), update_version);
                INVARIANT(false);
            }
            if (total_write_io[update_version].size() <= superstep_version) {
                fprintf(stdout, "[%ld] %s total_write_io[%d].size = %ld, up_v = %d, ss = %d\n", PartitionStatistics::my_machine_id(), array_name.c_str(), update_version, total_write_io[update_version].size(), update_version, superstep_version);
                INVARIANT(false);
            }
            std::atomic_fetch_add((std::atomic<int64_t>*) &total_write_io[update_version][superstep_version], (int64_t) (sizeof(DeltaMessage<T>) * idx_in_per_thread_buffer[tid].idx));
#endif
            ptr_to_per_thread_buffer[tid] = (DeltaMessage<T>*) RequestRespond::GetPerThreadBuffer([&]() {
                Turbo_bin_aio_handler::WaitMyPendingDiskIO(false, 0);   });
            idx_in_per_thread_buffer[tid].idx = 0;
            this->deltas_offset[update_version][superstep_version][tid] = this->deltas[superstep_version][tid]->file_size();
            //system_fprintf(0, stdout, "%s WriteFlush size of (%ld, %ld, %ld) = %ld\n", array_name.c_str(), update_version, superstep_version, tid, this->deltas_offset[update_version][superstep_version][tid]);
        }

        void FlushPerThreadWriteBuffers(Version to) {
            if (to.GetUpdateVersion() != 0) this->tim.start_timer(6);
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
            {
                WriteFlush(to.GetUpdateVersion(), to.GetSuperstepVersion());
            }
            if (to.GetUpdateVersion() != 0) this->tim.stop_timer(6);
        }
        
        ReturnStatus FindVersionRangeToConstruct (int update_version, int superstep_version, Version& from, Version& to) {
            int from_update_version, from_superstep_version;
            if (update_version > current_version.GetUpdateVersion()) {
                if (superstep_version > current_version.GetSuperstepVersion()) {
                    INVARIANT(false); // XXX - not implemented
                    from = current_version;
                    from.SetUpdateVersion(from.GetUpdateVersion() + 1);
                    from.SetSuperstepVersion(0); //added
                    to.SetUpdateVersion(update_version);
                    to.SetSuperstepVersion(from.GetSuperstepVersion());

                    int64_t cnt = CountNumDeltasToMerge(from, to);
                    // Do ConstructAndFlush for partial range
                    // For example, O = delta which already read, X = delta, not read yet, T = target version
                    // We need to read deltas [2, 0] ~ [2, 2] first, and then [0, 3] ~ [2, 4]
                    // --------
                    // |OOOXX
                    // |OOOXX
                    // |XXXXT
                    // TODO ? (need to implement?)

                    from_update_version = 0;
                    from_superstep_version = current_version.GetSuperstepVersion() + 1;
                } else if (superstep_version == current_version.GetSuperstepVersion()) {
                    from_update_version = current_version.GetUpdateVersion() + 1;
                    from_superstep_version = current_version.GetSuperstepVersion();
                } else {
                    // Construct from initial version(0, 0)
                    from_update_version = 0;
                    from_superstep_version = 0;
                }
            } else if (update_version == current_version.GetUpdateVersion()) {
                if (superstep_version == current_version.GetSuperstepVersion()) {
                    to.SetUpdateVersion(update_version);
                    to.SetSuperstepVersion(superstep_version);
                    return DONE;
                } else if (superstep_version < current_version.GetSuperstepVersion()) {
                    return DONE;
                }
                if(!(current_version.GetSuperstepVersion() == superstep_version || current_version.GetSuperstepVersion() + 1 == superstep_version)) {
                    fprintf(stdout, "[%ld] %s cur_ss = %d, ss = %ld\n", PartitionStatistics::my_machine_id(), array_name.c_str(), current_version.GetSuperstepVersion(), superstep_version);
                }
                INVARIANT(current_version.GetSuperstepVersion() == superstep_version || current_version.GetSuperstepVersion() + 1 == superstep_version);
                if (!advance_superstep_) 
                    from_update_version = current_version.GetUpdateVersion() - 1;
                else 
                    from_update_version = 0;
                from_superstep_version = superstep_version;
            } else { // update_version < current_version.GetUpdateVersion()
                INVARIANT(false); // XXX Is this really happen?
                INVARIANT(superstep_version == 0);
                from_update_version = 0;
                from_superstep_version = 0;
            }
            
            // Set version range to construct
            from.SetUpdateVersion(from_update_version);
            from.SetSuperstepVersion(from_superstep_version);
            to.SetUpdateVersion(update_version);
            to.SetSuperstepVersion(superstep_version);
            
            return OK;
        }
        
        void ConstructAndFlush(Range<int64_t> idx_range, int update_version, int superstep_version, bool construct_prev=false, VersionedArrayBase<T, op>* prev=NULL, bool flush=false) {
            int target_step = UserArguments::INC_STEP;
            //fprintf(stdout, "[%ld] (%d, %d, %d) ConstructAndFlush %s cur_ver [%d, %d] -> to [%d, %d] advance_superstep_ %s constructed_already_ %s\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, array_name.c_str(), current_version.GetUpdateVersion(), current_version.GetSuperstepVersion(), update_version, superstep_version, advance_superstep_ ? "true" : "false", constructed_already_ ? "true" : "false");
            turbo_timer temp_tim;
            temp_tim.start_timer(0);
            if (!advance_superstep_ && superstep_version > 0) return;
             
            // In Static Processing, do not need to construct, just flush
            // TODO remove condition related to INC_STEP (will be processed in Vector class)
            if (UserArguments::INC_STEP == -1 && !constructed_already_) {
                Version to (update_version, superstep_version);
                if (flush) FlushAll_(to);
                return;
            }
            temp_tim.stop_timer(0);
            INVARIANT(update_version >= current_version.GetUpdateVersion());
            
            temp_tim.start_timer(1);
            // Find version range which need to construct
            Version from, to;
            if (FindVersionRangeToConstruct(update_version, superstep_version, from, to) == DONE) return;
            temp_tim.stop_timer(1);

            temp_tim.start_timer(2);
            __ConstructAndFlushFromTo(from, to, prev, construct_prev, flush);
            temp_tim.stop_timer(2);
            //fprintf(stdout, "[%ld] %s ConstructAndFlush to [%d, %d] %.3f %.3f %.3f INC_STEP = %d\n", PartitionStatistics::my_machine_id(), array_name.c_str(), update_version, superstep_version, temp_tim.get_timer(0), temp_tim.get_timer(1), temp_tim.get_timer(2), target_step);

            // Set current version for cur, prev(if needed) versioned array
            if (current_version.GetSuperstepVersion() < superstep_version) {
                current_version.SetUpdateVersion(update_version);
                current_version.SetSuperstepVersion(superstep_version);
                if (construct_prev) prev->SetCurrentVersion(update_version - 1, superstep_version);
            }
        }
        
        // update_version++, superstep_version == 0
        void AdvanceUpdateVersion(int next_u) {
            for (int u_v = deltas_offset.size(); u_v <= next_u; u_v++) {
                VECTOR<VECTOR<int64_t>> tmp_vec;
                tmp_vec.resize(1);
                tmp_vec[0].resize(UserArguments::NUM_THREADS);
                for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                    tmp_vec[0][k] = deltas_offset[u_v-1][0][k];
                }
                deltas_offset.push_back(tmp_vec);

                VECTOR<int64_t> tmp;
                tmp.resize(1, 0);
                total_read_io.push_back(tmp);
                total_write_io.push_back(tmp);
                
                VECTOR<double> tmp2;
                tmp.resize(1, 0.0);
                non_overlapped_time.push_back(tmp2);
            }
            
            current_version.SetUpdateVersion(next_u);
            current_version.SetSuperstepVersion(-1);
            num_performed_discard_deltas_in_a_snapshot = 0;
            InitializeAfterAdvanceUpdateVersion();
        }

        void AdvanceSuperstepVersion(int update_version) {
            if (!advance_superstep_ && current_version.GetSuperstepVersion() == 0) return;
            
            if (update_version == 0 || deltas.size() == UserArguments::SUPERSTEP + 1) {
                INVARIANT(deltas.size() == UserArguments::SUPERSTEP + 1);
                std::string delta_array_path = array_path + "_" + std::to_string(deltas.size());
                std::vector<Turbo_bin_aio_handler*> tmp_vec;
                for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                    std::string delta_array_path2 = array_path + "_" + std::to_string(deltas.size()) + "_" + std::to_string(k);
                    tmp_vec.push_back(new Turbo_bin_aio_handler(delta_array_path2.c_str(), true, true, true, true));
                }
                deltas.push_back(tmp_vec);
                deltas_dead_range_per_superstep.push_back(Range<int64_t>(-1, -1));
                first_snapshot_version.push_back(0);
                
                non_overlapped_time[update_version].push_back(0.0);
            }
            
            total_read_io[update_version].push_back(0);
            total_write_io[update_version].push_back(0);

            if (update_version == 0) {
                INVARIANT(deltas_offset[update_version].size() == UserArguments::SUPERSTEP + 1);
            }
            for (int i = 0; i <= update_version; i++) {
                while (deltas_offset[i].size() <= UserArguments::SUPERSTEP + 1) {
                    int64_t last_value = 0;
                    
                    VECTOR<int64_t> tmp_vec2;
                    for (int k = 0; k < UserArguments::NUM_THREADS; k++) {
                        if (i >= 1) {
                            last_value = deltas_offset[i-1][deltas_offset[i].size()][k];
                        }
                        tmp_vec2.push_back(last_value);
                    }
                    deltas_offset[i].push_back(tmp_vec2);
                }
                INVARIANT(deltas_offset[i].size() >= UserArguments::SUPERSTEP);
            }
            if (update_version == 0) {
                INVARIANT(deltas_offset[update_version].size() == UserArguments::SUPERSTEP + 2);
            }
            CopyPrevSSBuffer();
        }

        inline void MarkToFlushAll() { to_flush_flags.SetAll(); }
        inline void MarkToFlush(int64_t idx) { to_flush_flags.Set_Atomic(idx); }
        inline void MarkToFlushUnsafe(int64_t idx) { to_flush_flags.Set(idx); }
        inline void ClearToFlush(int64_t idx) { to_flush_flags.Clear_Atomic(idx); }
        inline void ClearAllFlush() { to_flush_flags.ClearAll(); }

        inline void MarkChangedAll() { changed_flags.SetAll(); }
        inline void MarkChanged(int64_t idx) { changed_flags.Set_Atomic(idx); }
        inline void MarkChangedUnsafe(int64_t idx) { changed_flags.Set(idx); }
        inline void ClearChanged(int64_t idx) { changed_flags.Clear_Atomic(idx); }
        inline void ClearAllChanged() { changed_flags.ClearAll(); }
        inline bool IsChanged(int64_t idx) { return changed_flags.Get(idx); }
        
        inline void MarkDirtyAll() { dirty_flags.SetAll(); }
        inline void MarkDirty(int64_t idx) { dirty_flags.Set_Atomic(idx); }
        inline void MarkDirtyUnsafe(int64_t idx) { dirty_flags.Set(idx); }
        inline void MarkDirtyIfNotDirty(int64_t idx) { if (!dirty_flags.Get(idx)) dirty_flags.Set_Atomic(idx); }
        inline void ClearDirty(int64_t idx) { dirty_flags.Clear_Atomic(idx); }
        inline void ClearAllDirty() { dirty_flags.ClearAll(); }
        inline bool IsDirty(int64_t idx) { return dirty_flags.Get(idx); }
        void InvokeIfDirty(const std::function<void(node_t)>& f) { dirty_flags.InvokeIfMarked(f); }

        inline void ClearAllNext() {
            next_flags[toggle_].ClearAll();
            first_snapshot_delta_flags[toggle_].ClearAll();
        }
        
        inline void ConvertNextFlagsToList() {
            next_flags[toggle_].ConvertToList(true);
            first_snapshot_delta_flags[toggle_].ConvertToList(true);
        }
        
        inline void RevertNextFlags() {
            next_flags[toggle_].RevertList(true);
            first_snapshot_delta_flags[toggle_].RevertList(true);
        }
        
        inline void RevertNextFlagsAll() {
            next_flags[0].RevertList(true);
            next_flags[1].RevertList(true);
            first_snapshot_delta_flags[0].RevertList(true);
            first_snapshot_delta_flags[1].RevertList(true);
        }
        
        inline void MarkDiscard(int64_t idx) { discard_flags.Set_Atomic(idx); }
        inline void MarkDiscardAll() { discard_flags.SetAll(); }
        inline void MarkDiscardUnsafe(int64_t idx) { discard_flags.Set(idx); }
        inline void ClearDiscard(int64_t idx) { discard_flags.Clear_Atomic(idx); }
        inline void ClearAllDiscard() { discard_flags.ClearAll(); }
        inline bool IsDiscard(int64_t idx) { return discard_flags.Get(idx); }
        void CopyFlagsBeforeFlush() {
            if (!maintain_prev_ss_) return;
            if (UserArguments::UPDATE_VERSION == 0 || UserArguments::INC_STEP == -1) {
                prev_dirty_flags.CopyFrom(to_flush_flags);
            }
        }
        
        void PrintFlags() {
            fprintf(stdout, "PrintFlags %s\n", array_path.c_str());
            for (int64_t idx = 0; idx < buf_length; idx++) {
                fprintf(stdout, "\tV[%ld]\tDirty: %ld, Changed: %ld\n", idx, dirty_flags.Get(idx), changed_flags.Get(idx));
            }
        }
        int64_t GetNumberOfUpdateVersions() { return (deltas_offset.size()); }
        int64_t GetNumberOfSuperstepVersions(int update_version) { 
            ALWAYS_ASSERT (deltas_offset.size() > update_version);
            return (deltas_offset[update_version].size()); 
        }
        
        void SetAdvanceSuperstep(bool advance_superstep) {
            advance_superstep_ = advance_superstep;
        }
        void SetConstructNextSuperStepArrayAsync(bool construct_next_ss_array_async) {
            construct_next_ss_array_async_ = construct_next_ss_array_async;
        }
        void SetConstructedAlready(bool constructed_already) {
            constructed_already_ = constructed_already;
        }
        void SetCurrentVersion(int u, int s) {
            current_version.SetUpdateVersion(u);
            current_version.SetSuperstepVersion(s);
        }
        Version GetCurrentVersion() { return current_version; }
        TwoLevelBitMap<int64_t>* GetDirtyFlags() { return &dirty_flags; }
        TwoLevelBitMap<int64_t>* GetChangedFlags() { return &changed_flags; }
        TwoLevelBitMap<int64_t>* GetMaintainFlags() { return &maintain_prev_ss_flags; }
        TwoLevelBitMap<int64_t>* GetNextFlags(bool t) { 
            char toggle = t ? (toggle_ + 1) % 2 : toggle_;
            return &next_flags[toggle]; 
        }
        TwoLevelBitMap<int64_t>* GetFirstSnapshotFlags(bool t) { 
            char toggle = t ? (toggle_ + 1) % 2 : toggle_;
            return &first_snapshot_delta_flags[toggle]; 
        }
        int64_t GetBufLength() { return buf_length; }
        std::string GetArrayName() { return array_name; }
        std::string GetArrayPath() { return array_path; }

        size_t GetTotalDeltaSizeInDisk(Version from, Version to) {
            return 0;
            size_t sum = 0;
            for (int i = from.GetUpdateVersion(); i <= to.GetUpdateVersion(); i++) {
                for (int j = from.GetSuperstepVersion(); j <= to.GetSuperstepVersion(); j++) {
                    if (j >= deltas_offset[i].size()) continue;
                    for (int x = 0; x < deltas_offset[i][j].size(); x++) {
                        if (i == 0) sum += deltas_offset[i][j][x];
                        else sum += (deltas_offset[i][j][x] - deltas_offset[i-1][j][x]);
                    }
                }
            }
            return sum;
        }

        static void ReturnPerThreadBuffer(diskaio::DiskAioRequest* req){
            char* data = (char*) req->GetBuf();
            RequestRespond::ReturnPerThreadBuffer(data);
        }

        //TODO
        void CopyDeltasFromReference() {
            INVARIANT(GetRef() != NULL);
            deltas.clear();
            deltas_offset.clear();
            deltas_dead_range_per_superstep.clear();
            
            for (int i = 0; i < GetRef()->deltas_offset.size(); i++) {
                VECTOR<VECTOR<int64_t>> tmp_vec;
                VECTOR<VECTOR<int64_t>>& vec = GetRef()->deltas_offset[i];

                tmp_vec.resize(vec.size());
                for (int j = 0; j < vec.size(); j++) {
                    for (int k = 0; k < vec[j].size(); k++) {
                        tmp_vec[j].push_back(vec[j][k]);
                    }
                }
                deltas_offset.push_back(tmp_vec);
            }

            for (int i = 0; i < GetRef()->deltas.size(); i++) {
                std::vector<Turbo_bin_aio_handler*> tmp_vec;
                std::vector<Turbo_bin_aio_handler*>& vec = GetRef()->deltas[i];

                for (int j = 0; j < vec.size(); j++) {
                    tmp_vec.push_back(vec[j]);
                }
                deltas.push_back(tmp_vec);
            }
            
            for (int i = 0; i < GetRef()->deltas_dead_range_per_superstep.size(); i++) {
                Range<int64_t> dead_range = GetRef()->deltas_dead_range_per_superstep[i];
                deltas_dead_range_per_superstep.push_back(dead_range);
            }
            first_snapshot_version = GetRef()->first_snapshot_version;
        }
        
        void WaitForIoRequests(bool read, bool write) {
            Turbo_bin_aio_handler::WaitForIoRequests(read, write);
        }

        std::vector<std::vector<Turbo_bin_aio_handler*>>& GetDeltas() {
            return deltas;
        }
        
        std::vector<std::vector<std::vector<int64_t>>>& GetDeltaOffset() {
            return deltas_offset;
        }


        // Compute
        // costs[0] = |A| = Approximated to |B|
        // costs[1] = |B| = first_snapshot_delta_flags.GetTotal(); OR Compute_B()
        // costs[2] = |C| = Compute_C()
        // costs[3] = |D| = to_flush_flags.GetTotal();
        // costs[4] = |E| = next_flags.GetTotal()
        // costs[5] = |F| = (next_flags U first_snapshot_delta_flags).GetTotal();
        void ComputeIoCostModels() {
            int64_t CostForDiscardDeltas = 2 * this->costs[0];
            int64_t CostForCreateDelta = this->costs[3] + this->costs[1] + this->costs[2];
            int64_t CostForMergeDeltas = this->costs[4] + this->costs[1] + this->costs[4];
            //system_fprintf(0, stdout, "(%ld, %ld) [ComputeIoCostModels](%s) // %ld %ld %ld %ld %ld // %ld %ld %ld\n", (int64_t) UserArguments::UPDATE_VERSION, (int64_t) UserArguments::SUPERSTEP, this->array_name.c_str(), (int64_t) this->costs[0], (int64_t) this->costs[1], (int64_t) this->costs[2], (int64_t) this->costs[3], (int64_t) this->costs[4], (int64_t) CostForDiscardDeltas, (int64_t) CostForCreateDelta, (int64_t) CostForMergeDeltas);
        }

        int FindBestStrategyForFlush() {
            // Costs = (Write Cost at current (i, j)) + (Read Cost at (i+1, j))
            int64_t read_cost_weight = (UserArguments::UPDATE_VERSION - GetFirstDeltaSnapshot(UserArguments::SUPERSTEP + 1));
            //int64_t CostForDiscardDeltas = (this->costs[0]) + read_cost_weight * (this->costs[0]);
            //int64_t CostForCreateDelta = (this->costs[3]) + read_cost_weight * (this->costs[1] + this->costs[2] + this->costs[3]);
            //int64_t CostForMergeDeltas = (this->costs[4]) + read_cost_weight * (this->costs[1] + this->costs[4]);
            int64_t ReadCostForDiscardDeltas = this->costs[0];
            int64_t WriteCostForDiscardDeltas = this->costs[0];
            int64_t ReadCostForCreateDelta = this->costs[6];
            int64_t WriteCostForCreateDelta = this->costs[3];
            int64_t ReadCostForMergeDeltas = read_cost_weight * this->costs[4];
            int64_t WriteCostForMergeDeltas = this->costs[4];
            //int64_t ReadCostForCreateDelta = this->costs[3];
            //int64_t WriteCostForCreateDelta = this->costs[1] + this->costs[6] + this->costs[3];
            //int64_t ReadCostForMergeDeltas = this->costs[4];
            //int64_t WriteCostForMergeDeltas = this->costs[1] + this->costs[4];
            for (int i = 0; i < 5; i++) this->costs[i] = 0;

						int64_t CostForDiscardDeltas = ReadCostForDiscardDeltas + WriteCostForDiscardDeltas;
						int64_t CostForMergeDeltas = ReadCostForMergeDeltas + WriteCostForMergeDeltas;
						int64_t CostForCreateDelta = ReadCostForCreateDelta + WriteCostForCreateDelta;
            
//#ifdef PRINT_PROFILING_TIMERS
						system_fprintf(0, stdout, "[%ld] (%ld, %ld) [ComputeIoCostModels](%s) // %ld %ld %ld %ld %ld // %ld %ld %ld\n", PartitionStatistics::my_machine_id(), (int64_t) UserArguments::UPDATE_VERSION, (int64_t) UserArguments::SUPERSTEP, this->array_name.c_str(), (int64_t) this->costs[0], (int64_t) this->costs[1], (int64_t) this->costs[2], (int64_t) this->costs[3], (int64_t) this->costs[4], (int64_t) CostForDiscardDeltas, (int64_t) CostForCreateDelta, (int64_t) CostForMergeDeltas);
//#endif
            
            if (hybrid_mode == NULL) {

            } else {
                std::string mode = std::string(hybrid_mode);
                if (mode == "CreateDelta") {
										return 1;
                } else if (mode == "CreateOrMerge") {
                  return 1;
										//LOG_ASSERT(false);
                } else if (mode == "MergeDelta") {
										return 2;
                } else if (mode == "DiscardDelta") {
										return 0;
                }
            }
						if (ReadCostForCreateDelta > WriteCostForMergeDeltas) {
							if (ReadCostForMergeDeltas > WriteCostForDiscardDeltas) {
								if (num_performed_discard_deltas_in_a_snapshot >= num_max_discard_deltas_in_a_snapshot) {
									return 2;
								}
								return 0;
							} else {
								return 2;
							}
						} else {
							return 1;
						}
        }
        
        // For OV, 
        int FindBestStrategyForFlushForOV(bool apply_hybrid_model) {
            // Costs = (Write Cost at current (i, j)) + (Read Cost at (i+1, j))
            
            int64_t read_cost_weight = (UserArguments::UPDATE_VERSION - GetFirstDeltaSnapshot(UserArguments::SUPERSTEP));
            //int64_t CostForDiscardDeltas = (this->costs[0]) + read_cost_weight * (this->costs[0]);
            //int64_t CostForCreateDelta = (this->costs[3]) + read_cost_weight * (this->costs[1] + this->costs[2] + this->costs[3]);
            //int64_t CostForMergeDeltas = (this->costs[4]) + read_cost_weight * (this->costs[1] + this->costs[4]);
            int64_t ReadCostForDiscardDeltas = this->costs[0];
            int64_t WriteCostForDiscardDeltas = this->costs[0];
            int64_t ReadCostForCreateDelta = this->costs[6];
            int64_t WriteCostForCreateDelta = this->costs[3];
            int64_t ReadCostForMergeDeltas = read_cost_weight * this->costs[4];
            int64_t WriteCostForMergeDeltas = this->costs[4];
            //int64_t ReadCostForCreateDelta = this->costs[3];
            //int64_t WriteCostForCreateDelta = this->costs[1] + this->costs[6] + this->costs[3];
            //int64_t ReadCostForMergeDeltas = this->costs[4];
            //int64_t WriteCostForMergeDeltas = this->costs[1] + this->costs[4];
            for (int i = 0; i < 5; i++) this->costs[i] = 0;
						
						int64_t CostForDiscardDeltas = ReadCostForDiscardDeltas + WriteCostForDiscardDeltas;
						int64_t CostForMergeDeltas = ReadCostForMergeDeltas + WriteCostForMergeDeltas;
						int64_t CostForCreateDelta = ReadCostForCreateDelta + WriteCostForCreateDelta;

//#ifdef PRINT_PROFILING_TIMERS
            fprintf(stdout, "[%ld] (%ld, %ld) [ComputeIoCostModels](%s) // %ld %ld %ld %ld %ld // %ld %ld %ld\n", PartitionStatistics::my_machine_id(), (int64_t) UserArguments::UPDATE_VERSION, (int64_t) UserArguments::SUPERSTEP, this->array_name.c_str(), (int64_t) this->costs[0], (int64_t) this->costs[1], (int64_t) this->costs[2], (int64_t) this->costs[3], (int64_t) this->costs[4], (int64_t) CostForDiscardDeltas, (int64_t) CostForCreateDelta, (int64_t) CostForMergeDeltas);
//#endif

						// At the end of Step 4, we don't want to merge deltas for OV; so we choose 'CreateDelta'
            if (!apply_hybrid_model) return 1;
            if (hybrid_mode == NULL) {

            } else {
                std::string mode = std::string(hybrid_mode);
                if (mode == "CreateDelta") {
										return 1;
                } else if (mode == "CreateOrMerge") {
										return 1;
									//LOG_ASSERT(false);
                } else if (mode == "MergeDelta") {
										return 2;
                } else if (mode == "DiscardDelta") {
										return 0;
                }
            }
            
						if (ReadCostForCreateDelta > WriteCostForMergeDeltas) {
							if (ReadCostForMergeDeltas > WriteCostForDiscardDeltas) {
								if (num_performed_discard_deltas_in_a_snapshot >= num_max_discard_deltas_in_a_snapshot) {
									return 2;
								}
								return 0;
							} else {
								return 2;
							}
						} else {
							return 1;
						}
        }

        int64_t GetSizeOfDelta(int64_t u, int64_t s) {
            if (UserArguments::SUPERSTEP < 0) return 0;
            if (deltas_offset.size() <= u) return 0;
            if (deltas_offset[u].size() <= s) return 0;
            ALWAYS_ASSERT(s >= 0);
            ALWAYS_ASSERT(deltas_offset.size() > u);
            ALWAYS_ASSERT(deltas_offset[u].size() > s);
            ALWAYS_ASSERT(deltas_offset[u][s].size() == UserArguments::NUM_THREADS);
            int64_t result = 0;
            for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
                result += deltas_offset[u][s][tid] - ((u == 0) ? 0L : deltas_offset[u - 1][s][tid]);
            }
            return result;
        }

        int64_t Compute_A() {
        }
        int64_t Compute_B(Version to=Version()) {
            int64_t s;
            if (to.GetSuperstepVersion() == -1) {
                s = is_ov_ ? UserArguments::SUPERSTEP : UserArguments::SUPERSTEP + 1;
            } else {
                s = is_ov_ ? to.GetSuperstepVersion() : to.GetSuperstepVersion() + 1;
            }
            int64_t init_delta_snapshot = GetFirstDeltaSnapshot(s);
            //system_fprintf(0, stdout, "[0] %s Compute_B GetSizeOfDelta[%d][%d] = %ld\n", array_name.c_str(), init_delta_snapshot, s, GetSizeOfDelta(init_delta_snapshot, s) / sizeof(DeltaMessage<T>));
            int64_t result = GetSizeOfDelta(init_delta_snapshot, s) / sizeof(DeltaMessage<T>);
            return result;
        }
        int64_t Compute_C(Version to=Version()) {
            int64_t s, u;
            if (to.GetSuperstepVersion() == -1 && to.GetUpdateVersion() == -1) {
                s = is_ov_ ? UserArguments::SUPERSTEP : UserArguments::SUPERSTEP + 1;
                u = UserArguments::UPDATE_VERSION;
            } else {
                INVARIANT(to.GetUpdateVersion() != -1);
                INVARIANT(to.GetSuperstepVersion() != -1);
                s = is_ov_ ? to.GetSuperstepVersion() : to.GetSuperstepVersion() + 1;
                u = to.GetUpdateVersion();
            }
            int64_t init_delta_snapshot = GetFirstDeltaSnapshot(s);
            int64_t result = 0;
            for (int64_t u_idx = init_delta_snapshot + 1; u_idx <= u; u_idx++) {
                if (deltas_dead_range_per_superstep[s].contains(u_idx)) continue;
                //system_fprintf(0, stdout, "[0] %s Compute_C GetSizeOfDelta[%d][%d] = %ld\n", array_name.c_str(), u_idx, s, GetSizeOfDelta(u_idx, s) / sizeof(DeltaMessage<T>));
                result += GetSizeOfDelta(u_idx, s) / sizeof(DeltaMessage<T>);
            }
            return result;
        }
        int64_t Compute_D() {

        }
        int64_t Compute_E() {

        }
        int64_t Compute_F() {

        } 
        int64_t Compute_X(Version to=Version()) {
            int64_t s, u;
            if (to.GetSuperstepVersion() == -1 && to.GetUpdateVersion() == -1) {
                s = is_ov_ ? UserArguments::SUPERSTEP : UserArguments::SUPERSTEP + 1;
                u = UserArguments::UPDATE_VERSION;
            } else {
                INVARIANT(to.GetUpdateVersion() != -1);
                INVARIANT(to.GetSuperstepVersion() != -1);
                s = is_ov_ ? to.GetSuperstepVersion() : to.GetSuperstepVersion() + 1;
                u = to.GetUpdateVersion();
            }
            int64_t init_delta_snapshot = GetFirstDeltaSnapshot(s);
            int64_t result = 0;
            for (int64_t u_idx = init_delta_snapshot + 1; u_idx <= u; u_idx++) {
                if (deltas_dead_range_per_superstep[s].contains(u_idx)) continue;
                result += ((u - u_idx) * (GetSizeOfDelta(u_idx, s) / sizeof(DeltaMessage<T>)));
            }
            return result;
        }


    public:
        std::string array_path;
        std::string array_name;

        VECTOR<VECTOR<Turbo_bin_aio_handler*>> deltas;  // ...[superstep][thread]
        VECTOR<VECTOR<VECTOR<int64_t>>> deltas_offset;  // ...[snapshot][superstep][thread]
        VECTOR<Range<int64_t>> deltas_dead_range_per_superstep;
        VECTOR<VECTOR<int64_t>> total_read_io;
        VECTOR<VECTOR<int64_t>> total_write_io;
        VECTOR<VECTOR<double>> non_overlapped_time;
        VECTOR<int64_t> first_snapshot_version;

        int64_t buf_length;
        Version current_version;
        TwoLevelBitMap<int64_t> first_snapshot_delta_flags[2];
        TwoLevelBitMap<int64_t> next_flags[2];
        TwoLevelBitMap<int64_t> dirty_flags;
        TwoLevelBitMap<int64_t> prev_dirty_flags;
        TwoLevelBitMap<int64_t> discard_flags;
        TwoLevelBitMap<int64_t> changed_flags;
        TwoLevelBitMap<int64_t> to_flush_flags;
        TwoLevelBitMap<int64_t> maintain_prev_ss_flags;
        //BitMap<int64_t> latest_flags;
        //BitMap<int64_t> changed_flags;
        //BitMap<int64_t> to_flush_flags;
        bool is_ov_;
        bool open_;
        bool use_main_ = false;
        bool advance_superstep_;
        bool construct_next_ss_array_async_;
        bool constructed_already_;
        bool has_future_job = false;
        bool do_lazy_merge = false;
        bool maintain_prev_ss_;

        char toggle_;

        T identity_element;
        int64_t num_max_discard_deltas_in_a_snapshot;
        int64_t num_performed_discard_deltas_in_a_snapshot;
        int64_t buf_memory_usage = 0, bitmap_memory_usage = 0;

        turbo_timer tim;
        int64_t total_io_read, total_io_write;
        std::future<void> reqs_to_wait;
	
        tbb::concurrent_queue<DeltaMessage<T>*> task_page_queue_t;
        DeltaMessage<T>* ptr_to_per_thread_buffer[MAX_NUM_CPU_CORES];
        PaddedIdx idx_in_per_thread_buffer[MAX_NUM_CPU_CORES];
        
        MemoryMappedArray<T> main; // ver 0,0

        int64_t costs[7] = {0};
        char* hybrid_mode;
        std::vector<json11::Json> json_io_wait_per_ss_vector;
        std::vector<json11::Json> json_merge_per_ss_vector;
};

