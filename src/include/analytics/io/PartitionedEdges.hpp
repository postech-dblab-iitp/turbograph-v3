#pragma once

#include <iostream>
#include <thread>
#include <atomic>
#include <queue>
#include <string>
#include <sys/statvfs.h>

#include "analytics/core/TypeDef.hpp"
#include "analytics/core/TG_DistributedVector.hpp"
#include "omp.h"
#include "math.h"
#include "analytics/io/Blocking_Turbo_bin_io_handler.hpp"
#include "analytics/io/Splittable_Turbo_bin_io_handler.hpp"
#include "analytics/io/Turbo_bin_mmapper.hpp"
#include "analytics/datastructure/FixedSizeBufferPool.hpp"
#include "analytics/util/atom.hpp"
#include "analytics/util/timer.hpp"
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"

template <typename FileNodeType, typename Degree_t>
class PartitionedEdges {
    typedef EdgePairEntry<FileNodeType> FileEdge;
    typedef Degree_t degree_t;

    public:
    turbo_timer total_read_timer;
    turbo_timer total_write_timer;
    PartitionedEdges() {}

    PartitionedEdges(int64_t num_chunks, int64_t num_threads, std::string partitioned_file_path,std::string partitioned_file_path2, FixedSizeConcurrentBufferPool* concurrent_buffer_pool, int64_t buffer_size) {
        //TODO remove usesless pointers
        std::string gfile_name = std::string("tslee");
        google::InitGoogleLogging(gfile_name.c_str());
        FLAGS_logtostderr = 0;
        FLAGS_log_dir="/mnt/sdb1/glog/tslee/"; //TODO
        num_chunks_ = num_chunks;
        num_threads_ = num_threads;
        buffer_size_ = buffer_size;
        partitioned_file_path_ = partitioned_file_path;
        partitioned_file_path2_ = partitioned_file_path2;
        concurrent_buffer_pool_ = concurrent_buffer_pool;

        optr_ = new FileEdge**[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            optr_[i] = new FileEdge*[num_chunks];
        }

        oreader_ = new Turbo_bin_mmapper*[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            oreader_[i] = new Turbo_bin_mmapper[num_chunks_];
        }

        io_reader_ = new Turbo_bin_io_handler*[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            io_reader_[i] = new Turbo_bin_io_handler[num_chunks_];
        }

        io_reader2_ = new Turbo_bin_io_handler*[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            io_reader2_[i] = new Turbo_bin_io_handler[num_chunks_];
        }

        ofile_size_ = new std::size_t*[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            ofile_size_[i] = new std::size_t[num_chunks_];
        }

        io_file_size_ = new std::size_t*[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            io_file_size_[i] = new std::size_t[num_chunks_];
        }

        marking_table = new int*[num_chunks_];
        for(int64_t i = 0; i < num_chunks_; i++) {
            marking_table[i] = new int[num_chunks_];
            for(int64_t j = 0; j < num_chunks_; j++)
                marking_table[i][j] = 0;
        }
    }

    ~PartitionedEdges() {
        //delete readers_grid_?
        for(int64_t i = 0; i < num_chunks_; i++) {
            delete[] ofile_size_[i];
            delete[] oreader_[i];
            delete[] optr_[i];
        }
        delete[] ofile_size_;
        delete[] oreader_;
        delete[] optr_;
    }

    ReturnStatus Open(const char* file_name) {
        reader_.OpenFileAndMemoryMap(file_name, true);
        //ptr_ = (Entry_t*) reader_.data();
        file_size_ = reader_.file_size();
        //ALWAYS_ASSERT(ptr_ != NULL);
    }

    ReturnStatus OpenGrid(int64_t row, int64_t col, bool write_enabled = false, bool remove_mode = false) {
        int64_t edge_cnt = 0;
        std::string file_name = "partition_" + std::to_string(row) + "_" + std::to_string(col);
        ReturnStatus result;
        result = oreader_[row][col].OpenFileAndMemoryMap((partitioned_file_path_ + file_name).c_str(), write_enabled);
        std::size_t temp_size = oreader_[row][col].file_size();
        ofile_size_[row][col] = temp_size;
        edge_cnt += temp_size;
        if(temp_size != 0)
            optr_[row][col] = (FileEdge*) oreader_[row][col].data();
        return result;
    }

    ReturnStatus OpenGrid_IO(int64_t row, int64_t col, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false, bool o_direct_mode = false, int marking = 0) {
        ALWAYS_ASSERT(marking == 0 || marking == 1);
        std::string file_name = "partition_" + std::to_string(row) + "_" + std::to_string(col);
        std::size_t temp_size;
        ReturnStatus result;
        if(marking == 0)
            result = io_reader_[row][col].OpenFile((partitioned_file_path_ + file_name).c_str(), create_if_not_exist, write_enabled, delete_if_exist, o_direct_mode);
        else if(marking == 1) 
            result = io_reader_[row][col].OpenFile((partitioned_file_path2_ + file_name).c_str(), create_if_not_exist, write_enabled, delete_if_exist, o_direct_mode);
        temp_size = io_reader_[row][col].file_size();
        io_file_size_[row][col] = temp_size;
        ALWAYS_ASSERT(temp_size % 512 == 0);
        return result;
    }

    ReturnStatus OpenGrid_IO2(int64_t row, int64_t col, bool create_if_not_exist = true, bool write_enabled = false, bool delete_if_exist = false, bool o_direct_mode = false, int marking = 0) {
        ALWAYS_ASSERT(marking == 0 || marking == 1);
        std::string file_name = "partition_" + std::to_string(row) + "_" + std::to_string(col);
        ReturnStatus result;
        if(marking == 0)
            result = io_reader2_[row][col].OpenFile((partitioned_file_path2_ + file_name).c_str(), create_if_not_exist, write_enabled, delete_if_exist, o_direct_mode);
        else if(marking == 1)
            result = io_reader2_[row][col].OpenFile((partitioned_file_path_ + file_name).c_str(), create_if_not_exist, write_enabled, delete_if_exist, o_direct_mode);
        return result;
    }

    ReturnStatus Close() {
        reader_.Close();
        return ReturnStatus::OK;
    }

    ReturnStatus CloseGrid(int64_t row, int64_t col, bool rm = false) {
        oreader_[row][col].Close(rm);
        return ReturnStatus::OK;
    }

    ReturnStatus CloseAll() {
        for(int64_t i = 0; i < num_chunks_; i++) {
            for(int64_t j = 0; j < num_chunks_; j++) {
                std::string file_name = partitioned_file_path2_ + "partition_" + std::to_string(i) + "_" + std::to_string(j);
                remove(file_name.c_str());
            }
        }
    }

    ReturnStatus CloseGrid_IO(int64_t row, int64_t col, bool rm = false) {
        io_reader_[row][col].Close(rm);
        return ReturnStatus::OK;
    }

    ReturnStatus CloseGrid_IO2(int64_t row, int64_t col, bool rm = false) {
        io_reader2_[row][col].Close(rm);
        return ReturnStatus::OK;
    }

    int64_t countOutDegreeAndProcess(int row, int col, TG_DistributedVector<degree_t, PLUS>& vector_for_count) {
        countDegreeAndProcess(row, col, [&](FileEdge edge) {
                if (edge.src_vid_ != -1) {
                    vector_for_count.Update(edge.src_vid_, 1);
                    return 1;
                } else {
                    return 0;
                }
            });
    }
    
    int64_t countInDegreeAndProcess(int row, int col, TG_DistributedVector<degree_t, PLUS>& vector_for_count) {
        countDegreeAndProcess(row, col, [&](FileEdge edge) {
                if (edge.dst_vid_ != -1) {
                    vector_for_count.Update(edge.dst_vid_, 1);
                    return 1;
                } else {
                    return 0;
                }
            });
    }

    int64_t countDegreeAndProcess(int row, int col, std::function<int(FileEdge)> count) {
        int buffering_degree = 6;
        std::size_t processed_size = 0;
        std::size_t real_processed_size = 0;
        int64_t edge_size = 0;
        int cur = 0;
        int prev_cur;

        std::size_t* start_offset = new std::size_t[buffering_degree];
        std::size_t* buffer_rw_size = new std::size_t[buffering_degree];
        std::queue<std::future<void>> read_reqs_to_wait;
        FileEdge** buffer = new FileEdge*[buffering_degree];
        SimpleContainer* containers = new SimpleContainer[buffering_degree];

        processed_size = 0;
        cur = 0;
        prev_cur = buffering_degree - 1;
        OpenGrid_IO(row, col, false, true, false);
        std::size_t file_size = io_file_size_[row][col];
        if(file_size != 0) {
            for(int j = 0; j < buffering_degree; j++) {
                containers[j] = concurrent_buffer_pool_->Alloc();
                buffer[j] = (FileEdge*)containers[j].data;
                start_offset[j] = 0;
                buffer_rw_size[j] = buffer_size_;
            }

            for(int j = 0; j < buffering_degree; j++) {
                if(j != 0) start_offset[j] = start_offset[j - 1] + buffer_rw_size[j - 1];
                buffer_rw_size[j] = std::min(buffer_rw_size[j], file_size - start_offset[j]);
                ALWAYS_ASSERT(start_offset[j] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[j] % sizeof(FileEdge) == 0);
                if(buffer_rw_size[j] > 0) read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[row][col], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
            }

            while(processed_size < file_size) {
                read_reqs_to_wait.front().get();
                read_reqs_to_wait.pop();
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
#pragma omp parallel num_threads(num_threads_)
                {
                    int64_t per_thread_edge_size = 0;
#pragma omp for
                    for(int64_t j = 0; j < (buffer_rw_size[cur] / sizeof(FileEdge)); j++) {
                        per_thread_edge_size += count(buffer[cur][j]);
                    }
                    std::atomic_fetch_add((std::atomic<int64_t>*)&edge_size, (int64_t)per_thread_edge_size);
                }

                real_processed_size += buffer_rw_size[cur];
                processed_size = start_offset[cur] + buffer_rw_size[cur];

                start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[row][col], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));
                prev_cur = cur;
                cur = (cur + 1) % buffering_degree;
                ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
            }
            while(!read_reqs_to_wait.empty()) {
                read_reqs_to_wait.front().get();
                read_reqs_to_wait.pop();
            }
            for (int j = 0; j < buffering_degree; j++)
                concurrent_buffer_pool_->Dealloc(containers[j]);
        }

        CloseGrid_IO(row, col);
        return edge_size;
    }

    ReturnStatus calculateLocalDegreeTable(Splittable_Turbo_bin_io_handler* degree_table_, int64_t num_threads_, int64_t total_edge, int64_t total_vertices, int64_t num_chunks_, int64_t machine_num) {
        int64_t edge_cnt = 0;

        node_t start_vid, end_vid;
        start_vid = 0;
        end_vid = -1;
        std::vector<degree_t> degree_table_buffer_;
        omp_set_num_threads(num_threads_);
        for(int64_t i = 0; i < num_chunks_; i++) {
            //open buffer and append
            start_vid = end_vid + 1;
            end_vid = ceil((double)(total_vertices * (i + 1)) / num_chunks_) - 1;
            if(end_vid == total_vertices) end_vid--;

            node_t section_size = end_vid - start_vid + 1;

            degree_table_buffer_.resize(section_size);
            memset((void*)&degree_table_buffer_[0], 0, section_size * sizeof(degree_t));

            for(int64_t j = 0; j < num_chunks_; j++) {
                OpenGrid(i, j);
                ALWAYS_ASSERT(ofile_size_[i][j] % sizeof(FileEdge) == 0);
                int64_t edge_size = ofile_size_[i][j] / sizeof(FileEdge);
#pragma omp parallel for
                for(int64_t k = 0; k < edge_size; k++){
                    FileNodeType src = optr_[i][j][k].src_vid_;
                    if(src != -1) {
                        src = src - start_vid;
                        ALWAYS_ASSERT(src <= section_size && src >= 0);
                        std::atomic_fetch_add((std::atomic<degree_t>*)&degree_table_buffer_[src], (degree_t)1);
                        ALWAYS_ASSERT(degree_table_buffer_[src] <= total_edge);
                    }
                }
                CloseGrid(i, j);
            }

            for(int64_t j = 0; j < num_chunks_; j++) {
                OpenGrid(j, i);
                ALWAYS_ASSERT(ofile_size_[j][i] % sizeof(FileEdge) == 0);
                int64_t edge_size = ofile_size_[j][i] / sizeof(FileEdge);
#pragma omp parallel for
                for(int64_t k = 0; k < edge_size; k++) {
                    FileNodeType dst = optr_[j][i][k].dst_vid_;
                    if(dst != -1) {
                        dst = dst - start_vid;
                        ALWAYS_ASSERT(dst <= section_size && dst >= 0);
                        std::atomic_fetch_add((std::atomic<degree_t>*)&degree_table_buffer_[dst], (degree_t)1);
                        ALWAYS_ASSERT(degree_table_buffer_[dst] <= total_edge);
                    }
                }
                CloseGrid(j, i);
            }

            degree_table_->Append(section_size * sizeof(degree_t), (char*)&degree_table_buffer_[0]);
        }

        return ReturnStatus::OK;
    }

    ReturnStatus calculateLocalInOutDegreeTable(Splittable_Turbo_bin_io_handler* indegree_table_, Splittable_Turbo_bin_io_handler* outdegree_table_, int64_t num_threads_, int64_t total_edge, int64_t total_vertices, int64_t num_chunks_, int machine_num) { // like buffer
        int64_t edge_cnt = 0;
        std::vector<degree_t> degree_table_buffer_;
        node_t start_vid, end_vid;
        start_vid = 0;
        end_vid = -1;
        omp_set_num_threads(num_threads_);

        for(int64_t i = 0; i < num_chunks_; i++) {
            start_vid = end_vid + 1;
            end_vid = ceil((double)(total_vertices * (i + 1)) / num_chunks_) - 1;
            if(end_vid == total_vertices) end_vid--;

            node_t section_size = end_vid - start_vid + 1;

            degree_table_buffer_.resize(section_size);
            memset((void*)&degree_table_buffer_[0], 0, section_size * sizeof(degree_t));

            for(int64_t j = 0; j < num_chunks_; j++) {
                OpenGrid(i, j);
                int64_t edge_size = ofile_size_[i][j] / sizeof(FileEdge);
                //if(edge_size == 0) std::cout<<i<<", "<<j<<std::endl;
#pragma omp parallel for
                for(int64_t k = 0; k < edge_size; k++){
                    FileNodeType src = optr_[i][j][k].src_vid_ - start_vid;
                    ALWAYS_ASSERT(src <= section_size && src >= 0);
                    std::atomic_fetch_add((std::atomic<degree_t>*)&degree_table_buffer_[src], (degree_t)1);
                    //degree_table_buffer_[src]++;
                }

                CloseGrid(i, j);
            }
            outdegree_table_->Append(section_size * sizeof(degree_t), (char*)&degree_table_buffer_[0]);

            degree_table_buffer_.resize(section_size);
            memset((void*)&degree_table_buffer_[0], 0, section_size * sizeof(degree_t));

            for(int64_t j = 0; j < num_chunks_; j++) {
                OpenGrid(j, i);
                int64_t edge_size = ofile_size_[j][i] / sizeof(FileEdge);
                //if(edge_size == 0) std::cout<<j<<", "<<i<<std::endl;
#pragma omp parallel for
                for(int64_t k = 0; k < edge_size; k++) {
                    FileNodeType dst = optr_[j][i][k].dst_vid_ - start_vid;
                    ALWAYS_ASSERT(dst <= section_size && dst >= 0);
                    std::atomic_fetch_add((std::atomic<degree_t>*)&degree_table_buffer_[dst], (degree_t)1);
                    //degree_table_buffer_[dst]++;
                }

                CloseGrid(j, i);
            }
            indegree_table_->Append(section_size * sizeof(degree_t), (char*)&degree_table_buffer_[0]);
        }

        return ReturnStatus::OK;
    }

    ReturnStatus relabelingEdges_byOffset(std::vector<int64_t> & range, std::vector<int64_t> & offset, int64_t num_threads_, int64_t max_vid, int64_t num_chunks_) {
        omp_set_num_threads(num_threads_);
        node_t start_vid, end_vid;
        start_vid = 0;
        end_vid = -1;
        int64_t zerovertices = 0;
        turbo_timer timer;
        int base_offset;
        int range_size = range.size();

        for(int64_t i = 0; i < num_chunks_; i++) {
            start_vid = end_vid + 1;
            end_vid = ceil((double)(max_vid * (i + 1)) / num_chunks_) - 1;
            if(end_vid == max_vid) end_vid--;
            for(int j = 0; j < range_size; j++) {
                if(start_vid <= range[j]) {
                    base_offset = j;
                    break;
                }
            }

            for(int64_t j = 0; j < num_chunks_; j++) {
                OpenGrid(i, j, true);
                int64_t edge_size = ofile_size_[i][j] / sizeof(FileEdge);
#pragma omp parallel num_threads(num_threads_)
                {
                    int offset_per_thread;
#pragma omp for
                    for(int64_t k = 0; k < edge_size; k++) {
                        FileNodeType src = optr_[i][j][k].src_vid_;
                        if(src != -1) {
                            for(int off = base_offset; off < range_size; off++) {
                                if(src <= range[off]) {
                                    offset_per_thread = off;
                                    break;
                                }
                                else {
                                    if(off == range_size - 1) {
                                        fprintf(stdout, "src = %lld, range[off] = %lld\n", (int64_t)src, range[off]);
                                        ALWAYS_ASSERT(false);
                                    }
                                }
                            }
                            optr_[i][j][k].src_vid_ = src + offset[offset_per_thread];
                        }
                    }
                }
                CloseGrid(i, j);
            }
            for(int64_t j = 0; j < num_chunks_; j++) {
                OpenGrid(j, i, true);
                int64_t edge_size = ofile_size_[j][i] / sizeof(FileEdge);
#pragma omp parallel num_threads(num_threads_)
                {
                    int offset_per_thread;
#pragma omp for
                    for(int64_t k = 0; k < edge_size; k++) {
                        FileNodeType dst = optr_[j][i][k].dst_vid_;
                        if(dst != -1) {
                            for(int off = base_offset; off < range_size; off++) {
                                if(dst <= range[off]) {
                                    offset_per_thread = off;
                                    break;
                                }
                                else {
                                    if(off == range_size - 1) { // dst > max range
                                        fprintf(stdout, "dst = %lld, range[off] = %lld\n", (int64_t)dst, range[off]);
                                        ALWAYS_ASSERT(false);
                                    }
                                }
                            }
                            optr_[j][i][k].dst_vid_ = dst + offset[offset_per_thread];
                        }
                    }
                }
                CloseGrid(j, i);
            }
        }
    }

    ReturnStatus relabelingEdges_byVector3(int64_t num_threads_, int index, TG_DistributedVector<node_t, PLUS>& oldtonew_vid_mapping) {
        int buffering_degree = 2;
        //if(PartitionStatistics::my_machine_id() == 0)
        //	fprintf(stdout, "\t[%lldth iter] Iteration start\n", index);
        turbo_timer total_tmr;
        total_tmr.start_timer(0);
        read_timer.reset_timer(0);
        write_timer.reset_timer(0);
        std::size_t processed_size = 0;
        std::size_t total_file_size = 0;
        std::size_t real_processed_size = 0;
        int cur = 0;
        int prev_cur;
        turbo_timer tmr;

        std::size_t* start_offset = new std::size_t[buffering_degree];
        std::size_t* buffer_rw_size = new std::size_t[buffering_degree];
        std::queue<std::future<void>> read_reqs_to_wait;
        std::queue<std::future<void>> write_reqs_to_wait;
        FileEdge** buffer = new FileEdge*[buffering_degree];
        SimpleContainer* containers = new SimpleContainer[buffering_degree];

        for(int i = 0; i < num_chunks_; i++) {
            processed_size = 0;
            cur = 0;
            prev_cur = buffering_degree - 1;
            OpenGrid_IO(index, i, false, true, false, true, marking_table[index][i]);
            OpenGrid_IO2(index, i, true, true, false, true, marking_table[index][i]);
            marking_table[index][i] = !marking_table[index][i];
            std::size_t file_size = io_file_size_[index][i];
            total_file_size += file_size;
            for(int j = 0; j < buffering_degree; j++) {
                containers[j] = concurrent_buffer_pool_->Alloc();
                buffer[j] = (FileEdge*)containers[j].data;
                start_offset[j] = 0;
                buffer_rw_size[j] = buffer_size_;
            }

            for(int j = 0; j < buffering_degree; j++) {
                if(j != 0)
                    start_offset[j] = start_offset[j - 1] + buffer_rw_size[j - 1];
                buffer_rw_size[j] = std::min(buffer_rw_size[j], file_size - start_offset[j]);
                ALWAYS_ASSERT(start_offset[j] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[j] % sizeof(FileEdge) == 0);
                if(buffer_rw_size[j] > 0)
                    read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
            }

            while(processed_size < file_size) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                tmr.start_timer(0);
#pragma omp parallel num_threads(num_threads_)
                {
                    node_t vid;
                    int64_t idx;
#pragma omp for
                    for(int64_t j = 0; j < (buffer_rw_size[cur] / sizeof(FileEdge)); j++) {
                        vid = (node_t)buffer[cur][j].src_vid_;
                        if(vid != -1) {
                            oldtonew_vid_mapping.Read(vid, idx);
                            buffer[cur][j].src_vid_ = (FileNodeType)idx;
                        }
                    }
                }
                tmr.stop_timer(0);

                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                //if(PartitionStatistics::my_machine_id() == 0)
                //	fprintf(stdout, "\t[%lldth file] Start offset = %lld, size to write = %lld\n", i, start_offset[cur], buffer_rw_size[cur]);
                LOG(INFO) << "Enqueue Write " << io_reader_[index][i].fdval() << " offset : " << start_offset[cur];
                write_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader2_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));
                //write_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader2_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));

                containers[cur] = concurrent_buffer_pool_->Alloc();
                buffer[cur] = (FileEdge*)containers[cur].data;

                real_processed_size += buffer_rw_size[cur];
                processed_size = start_offset[cur] + buffer_rw_size[cur];

                start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

                prev_cur = cur;
                cur = (cur + 1) % buffering_degree;
                ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
            }
            while(!read_reqs_to_wait.empty()) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
            }

            for (int j = 0; j < buffering_degree; j++)
                concurrent_buffer_pool_->Dealloc(containers[j]);
        }

        while(!write_reqs_to_wait.empty()) {
            write_reqs_to_wait.front().get();
            write_reqs_to_wait.pop();
        }

        for(auto i = 0; i < num_chunks_; i++) {
            CloseGrid_IO(index, i);
            CloseGrid_IO2(index, i);
        }

        for(int i = 0; i < num_chunks_; i++) {
            processed_size = 0;
            cur = 0;
            prev_cur = buffering_degree - 1;
            OpenGrid_IO(i, index, false, true, false, true, marking_table[i][index]);
            OpenGrid_IO2(i, index, true, true, false, true, marking_table[i][index]);
            marking_table[i][index] = !marking_table[i][index];
            std::size_t file_size = io_file_size_[i][index];
            total_file_size += file_size;
            for(int j = 0; j < buffering_degree; j++) {
                containers[j] = concurrent_buffer_pool_->Alloc();
                buffer[j] = (FileEdge*)containers[j].data;
                start_offset[j] = 0;
                buffer_rw_size[j] = buffer_size_;
            }

            for(int j = 0; j < buffering_degree; j++) {
                if(j != 0)
                    start_offset[j] = start_offset[j - 1] + buffer_rw_size[j - 1];
                buffer_rw_size[j] = std::min(buffer_rw_size[j], file_size - start_offset[j]);
                ALWAYS_ASSERT(start_offset[j] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[j] % sizeof(FileEdge) == 0);
                if(buffer_rw_size[j] > 0)
                    read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
            }

            while(processed_size < file_size) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                tmr.start_timer(0);
#pragma omp parallel num_threads(num_threads_)
                {
                    int64_t idx;
                    node_t vid;
#pragma omp for
                    for(int64_t j = 0; j < (buffer_rw_size[cur] / sizeof(FileEdge)); j++) {
                        vid = (node_t)buffer[cur][j].dst_vid_;
                        if(vid != -1) {
                            oldtonew_vid_mapping.Read(vid, idx);
                            buffer[cur][j].dst_vid_ = (FileNodeType)idx;
                        }
                    }
                }
                tmr.stop_timer(0);

                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                //if(PartitionStatistics::my_machine_id() == 0)
                //	fprintf(stdout, "\t[%lldth file] Start offset = %lld, size to write = %lld\n", i, start_offset[cur], buffer_rw_size[cur]);
                write_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader2_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));
                //write_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader2_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));

                containers[cur] = concurrent_buffer_pool_->Alloc();
                buffer[cur] = (FileEdge*)containers[cur].data;

                real_processed_size += buffer_rw_size[cur];
                processed_size = start_offset[cur] + buffer_rw_size[cur];

                start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

                prev_cur = cur;
                cur = (cur + 1) % buffering_degree;
                ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
            }
            while(!read_reqs_to_wait.empty()) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
            }

            for(int j = 0; j < buffering_degree; j++)
                concurrent_buffer_pool_->Dealloc(containers[j]);
        }

        while(!write_reqs_to_wait.empty()) {
            write_reqs_to_wait.front().get();
            write_reqs_to_wait.pop();
        }

        total_tmr.stop_timer(0);
        //fprintf(stdout, "\t[%lld] [%lldth iter] Total elapsed = %.3f, Cpu elapsed time = %.3f, Disk I/O cost = %lld, Disk Read elapsed time = %.3f, Disk Write elapsed time = %.3f, Processed file size = %lld\n", PartitionStatistics::my_machine_id(), index, total_tmr.get_timer(0), tmr.get_timer(0), total_file_size, read_timer.get_timer(0), write_timer.get_timer(0), real_processed_size);
        delete[] containers;
        delete[] buffer;
        delete buffer_rw_size;
        delete start_offset;

        for(auto i = 0; i < num_chunks_; i++) {
            CloseGrid_IO(i, index);
            CloseGrid_IO2(i, index);
        }

        return ReturnStatus::OK;
    }

    ReturnStatus relabelingEdges_byVector2(int64_t num_threads_, int index, TG_DistributedVector<node_t, PLUS>& oldtonew_vid_mapping) {
        int buffering_degree = 2;
        turbo_timer total_tmr;
        total_tmr.start_timer(0);
        read_timer.reset_timer(0);
        write_timer.reset_timer(0);
        std::size_t processed_size = 0;
        std::size_t total_file_size = 0;
        std::size_t real_processed_size = 0;
        int cur = 0;
        int prev_cur;
        turbo_timer tmr;

        std::size_t* start_offset = new std::size_t[buffering_degree];
        std::size_t* buffer_rw_size = new std::size_t[buffering_degree];
        std::queue<std::future<void>> read_reqs_to_wait;
        std::queue<std::future<void>> write_reqs_to_wait;
        FileEdge** buffer = new FileEdge*[buffering_degree];
        SimpleContainer* containers = new SimpleContainer[buffering_degree];

        for(int i = 0; i < num_chunks_; i++) {
            processed_size = 0;
            cur = 0;
            prev_cur = buffering_degree - 1;
            OpenGrid_IO(index, i, false, true, false, true);
            std::size_t file_size = io_file_size_[index][i];
            total_file_size += file_size;
            for(int j = 0; j < buffering_degree; j++) {
                containers[j] = concurrent_buffer_pool_->Alloc();
                buffer[j] = (FileEdge*)containers[j].data;
                start_offset[j] = 0;
                buffer_rw_size[j] = buffer_size_;
            }

            for(int j = 0; j < buffering_degree; j++) {
                if(j != 0)
                    start_offset[j] = start_offset[j - 1] + buffer_rw_size[j - 1];
                buffer_rw_size[j] = std::min(buffer_rw_size[j], file_size - start_offset[j]);
                ALWAYS_ASSERT(start_offset[j] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[j] % sizeof(FileEdge) == 0);
                if(buffer_rw_size[j] > 0)
                    read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
            }

            while(processed_size < file_size) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                tmr.start_timer(0);
#pragma omp parallel num_threads(num_threads_)
                {
                    node_t vid;
                    int64_t idx;
#pragma omp for
                    for(int64_t j = 0; j < (buffer_rw_size[cur] / sizeof(FileEdge)); j++) {
                        vid = (node_t)buffer[cur][j].src_vid_;
                        if(vid != -1) {
                            oldtonew_vid_mapping.Read(vid, idx);
                            buffer[cur][j].src_vid_ = (FileNodeType)idx;
                        }
                    }
                }
                tmr.stop_timer(0);

                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                //if(PartitionStatistics::my_machine_id() == 0)
                //	fprintf(stdout, "\t[%lldth file] Start offset = %lld, size to write = %lld\n", i, start_offset[cur], buffer_rw_size[cur]);
                LOG(INFO) << "Enqueue Write " << io_reader_[index][i].fdval() << " offset : " << start_offset[cur];
                write_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));
                //write_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));

                containers[cur] = concurrent_buffer_pool_->Alloc();
                buffer[cur] = (FileEdge*)containers[cur].data;

                real_processed_size += buffer_rw_size[cur];
                processed_size = start_offset[cur] + buffer_rw_size[cur];

                start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

                prev_cur = cur;
                cur = (cur + 1) % buffering_degree;
                ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
            }
            while(!read_reqs_to_wait.empty()) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
            }

            for (int j = 0; j < buffering_degree; j++)
                concurrent_buffer_pool_->Dealloc(containers[j]);
        }

        while(!write_reqs_to_wait.empty()) {
            write_reqs_to_wait.front().get();
            write_reqs_to_wait.pop();
        }

        for(int i = 0; i < num_chunks_; i++) {
            processed_size = 0;
            cur = 0;
            prev_cur = buffering_degree - 1;
            if(i != index) {
                OpenGrid_IO(i, index, false, true, false, true);
            }
            std::size_t file_size = io_file_size_[i][index];
            total_file_size += file_size;
            for(int j = 0; j < buffering_degree; j++) {
                containers[j] = concurrent_buffer_pool_->Alloc();
                buffer[j] = (FileEdge*)containers[j].data;
                start_offset[j] = 0;
                buffer_rw_size[j] = buffer_size_;
            }

            for(int j = 0; j < buffering_degree; j++) {
                if(j != 0)
                    start_offset[j] = start_offset[j - 1] + buffer_rw_size[j - 1];
                buffer_rw_size[j] = std::min(buffer_rw_size[j], file_size - start_offset[j]);
                ALWAYS_ASSERT(start_offset[j] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[j] % sizeof(FileEdge) == 0);
                if(buffer_rw_size[j] > 0)
                    read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
            }

            while(processed_size < file_size) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                tmr.start_timer(0);
#pragma omp parallel num_threads(num_threads_)
                {
                    int64_t idx;
                    node_t vid;
#pragma omp for
                    for(int64_t j = 0; j < (buffer_rw_size[cur] / sizeof(FileEdge)); j++) {
                        vid = (node_t)buffer[cur][j].dst_vid_;
                        if(vid != -1) {
                            oldtonew_vid_mapping.Read(vid, idx);
                            buffer[cur][j].dst_vid_ = (FileNodeType)idx;
                        }
                    }
                }
                tmr.stop_timer(0);

                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                //if(PartitionStatistics::my_machine_id() == 0)
                //	fprintf(stdout, "\t[%lldth file] Start offset = %lld, size to write = %lld\n", i, start_offset[cur], buffer_rw_size[cur]);
                write_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));
                //write_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));

                containers[cur] = concurrent_buffer_pool_->Alloc();
                buffer[cur] = (FileEdge*)containers[cur].data;

                real_processed_size += buffer_rw_size[cur];
                processed_size = start_offset[cur] + buffer_rw_size[cur];

                start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][index], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

                prev_cur = cur;
                cur = (cur + 1) % buffering_degree;
                ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
            }
            while(!read_reqs_to_wait.empty()) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
            }

            for(int j = 0; j < buffering_degree; j++)
                concurrent_buffer_pool_->Dealloc(containers[j]);
        }

        while(!write_reqs_to_wait.empty()) {
            write_reqs_to_wait.front().get();
            write_reqs_to_wait.pop();
        }

        total_tmr.stop_timer(0);
        //fprintf(stdout, "\t[%lld] [%lldth iter] Total elapsed = %.3f, Cpu elapsed time = %.3f, Disk I/O cost = %lld, Disk Read elapsed time = %.3f, Disk Write elapsed time = %.3f, Processed file size = %lld\n", PartitionStatistics::my_machine_id(), index, total_tmr.get_timer(0), tmr.get_timer(0), total_file_size, read_timer.get_timer(0), write_timer.get_timer(0), real_processed_size);
        delete[] containers;
        delete[] buffer;
        delete buffer_rw_size;
        delete start_offset;

        for(auto i = 0; i < num_chunks_; i++) {
            if(i != index) {
                CloseGrid_IO(index, i);
                CloseGrid_IO(i, index);
            }
            else {
                CloseGrid_IO(i, i);
            }
        }

        return ReturnStatus::OK;
    }

    ReturnStatus relabelingEdges_byVector2_SrcOnly(int64_t num_threads_, int index, TG_DistributedVector<node_t, PLUS>& oldtonew_vid_mapping) {
        int buffering_degree = 2;
        turbo_timer total_tmr;
        total_tmr.start_timer(0);
        read_timer.reset_timer(0);
        write_timer.reset_timer(0);
        std::size_t processed_size = 0;
        std::size_t total_file_size = 0;
        std::size_t real_processed_size = 0;
        int cur = 0;
        int prev_cur;
        turbo_timer tmr;

        std::size_t* start_offset = new std::size_t[buffering_degree];
        std::size_t* buffer_rw_size = new std::size_t[buffering_degree];
        std::queue<std::future<void>> read_reqs_to_wait;
        std::queue<std::future<void>> write_reqs_to_wait;
        FileEdge** buffer = new FileEdge*[buffering_degree];
        SimpleContainer* containers = new SimpleContainer[buffering_degree];

        for(int i = 0; i < num_chunks_; i++) {
            processed_size = 0;
            cur = 0;
            prev_cur = buffering_degree - 1;
            OpenGrid_IO(index, i, false, true, false, false);
            //OpenGrid_IO(index, i, false, true, false, true);
            std::size_t file_size = io_file_size_[index][i];
            total_file_size += file_size;
            for(int j = 0; j < buffering_degree; j++) {
                containers[j] = concurrent_buffer_pool_->Alloc();
                buffer[j] = (FileEdge*)containers[j].data;
                start_offset[j] = 0;
                buffer_rw_size[j] = buffer_size_;
            }

            for(int j = 0; j < buffering_degree; j++) {
                if(j != 0)
                    start_offset[j] = start_offset[j - 1] + buffer_rw_size[j - 1];
                buffer_rw_size[j] = std::min(buffer_rw_size[j], file_size - start_offset[j]);
                ALWAYS_ASSERT(start_offset[j] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[j] % sizeof(FileEdge) == 0);
                if(buffer_rw_size[j] > 0)
                    read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[j], start_offset[j], buffer_rw_size[j]));
            }

            while(processed_size < file_size) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                tmr.start_timer(0);
#pragma omp parallel num_threads(num_threads_)
                {
                    node_t vid;
                    int64_t idx;
#pragma omp for
                    for(int64_t j = 0; j < (buffer_rw_size[cur] / sizeof(FileEdge)); j++) {
                        vid = (node_t)buffer[cur][j].src_vid_;
                        if(vid != -1) {
                            oldtonew_vid_mapping.Read(vid, idx);
                            if(idx < 0)
                                fprintf(stdout, "[%lld] vid = %lld, idx = %lld\n", PartitionStatistics::my_machine_id(), vid, idx);
                            ALWAYS_ASSERT(idx >= 0);
                            //idx = oldtonew_vid_mapping.Read(vid);
                            buffer[cur][j].src_vid_ = (FileNodeType)idx;
                        }
                    }
                }
                tmr.stop_timer(0);

                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                //if(PartitionStatistics::my_machine_id() == 0)
                //	fprintf(stdout, "\t[%lldth file] Start offset = %lld, size to write = %lld\n", i, start_offset[cur], buffer_rw_size[cur]);
                LOG(INFO) << "Enqueue Write " << io_reader_[index][i].fdval() << " offset : " << start_offset[cur];
                write_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));
                //write_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_writeBuffertoDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));

                containers[cur] = concurrent_buffer_pool_->Alloc();
                buffer[cur] = (FileEdge*)containers[cur].data;

                real_processed_size += buffer_rw_size[cur];
                processed_size = start_offset[cur] + buffer_rw_size[cur];

                start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                ALWAYS_ASSERT(start_offset[cur] % sizeof(FileEdge) == 0);
                ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));
                //read_reqs_to_wait.push(Aio_Helper::async_pool_tslee.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[index][i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

                prev_cur = cur;
                cur = (cur + 1) % buffering_degree;
                ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
            }
            while(!read_reqs_to_wait.empty()) {
                tmr.start_timer(1);
                read_reqs_to_wait.front().get();
                tmr.stop_timer(1);
                read_reqs_to_wait.pop();
            }

            for (int j = 0; j < buffering_degree; j++)
                concurrent_buffer_pool_->Dealloc(containers[j]);
        }

        while(!write_reqs_to_wait.empty()) {
            write_reqs_to_wait.front().get();
            write_reqs_to_wait.pop();
        }

        total_tmr.stop_timer(0);
        //fprintf(stdout, "\t[%lld] [%lldth iter] Total elapsed = %.3f, Cpu elapsed time = %.3f, Disk I/O cost = %lld, Disk Read elapsed time = %.3f, Disk Write elapsed time = %.3f, Processed file size = %lld\n", PartitionStatistics::my_machine_id(), index, total_tmr.get_timer(0), tmr.get_timer(0), total_file_size, read_timer.get_timer(0), write_timer.get_timer(0), real_processed_size);
        delete[] containers;
        delete[] buffer;
        delete buffer_rw_size;
        delete start_offset;

        for(auto i = 0; i < num_chunks_; i++) {
            if(i != index) {
                CloseGrid_IO(index, i);
                CloseGrid_IO(i, index);
            }
            else {
                CloseGrid_IO(i, i);
            }
        }

        return ReturnStatus::OK;
    }

    ReturnStatus makeSendPartition(GraphPreprocessArgs args_, std::vector<int64_t> range){
        if (args_.partitioning_scheme != 1) {
            node_t col_size = args_.total_vertices / args_.machine_count;
            makeSendPartitionWrapper(args_, [&](FileNodeType vid) {
                    return vid / col_size;
                });
        } else {
            makeSendPartitionWrapper(args_, [&](FileNodeType vid) {
                    for (auto i = 0; i < args_.machine_count; i++) {
                        if (vid <= range[i]) return i;
                    }
                    //fprintf(stderr, "Error vertex : %lld, max range : %lld\n", edge.src_vid_, range[args_.machine_count - 1]);
                    throw std::runtime_error("");
                });
        }
        return ReturnStatus::OK;
    }

    ReturnStatus makeSendPartitionWrapper(GraphPreprocessArgs args_, std::function<int64_t(FileNodeType)> dst_machine){
        //if(partitioned_file_path_ != partitioned_file_path2_) //XXX what is this?
        //    CloseAll();
        int64_t num_threads_ = args_.num_threads;
        int64_t total_partition = args_.machine_count;
        size_t buffer_size = args_.buffer_size;
        std::string destination_file_path = args_.temp_path;
        int64_t total_vertices = args_.total_vertices;
        bool remove_mode = args_.remove_mode;

        int buffering_degree = 2;
        int64_t buffer_full_size = buffer_size / sizeof(FileEdge);
		std::queue<std::future<void>>* write_reqs_to_wait = new std::queue<std::future<void>>[num_threads_];

        std::vector<std::vector<std::vector<SimpleContainer>>> write_containers(num_threads_);
        std::vector<std::vector<Blocking_Turbo_bin_io_handler>> send_files(total_partition);
        std::vector<std::vector<std::vector<FileEdge*>>> write_edge_buffer(num_threads_);
        std::vector<std::vector<std::vector<int>>> write_edge_buffer_index(num_threads_);

        for (auto i = 0; i < total_partition; i++) {
            send_files[i].resize(total_partition);
            for (auto j = 0; j < total_partition; j++) {
                std::string file_name = std::string(outedges_send_partition_file_name)+std::to_string(i)+std::string("_")+std::to_string(j);
                truncate((destination_file_path + "/" + file_name).c_str(), 0);
                remove((destination_file_path + "/" + file_name).c_str());
                send_files[i][j].OpenFile((destination_file_path + file_name).c_str(), true, true, true);
            }
        }

        for (auto i = 0; i < num_threads_; i++) {
            write_containers[i].resize(total_partition);
            write_edge_buffer[i].resize(total_partition);
            write_edge_buffer_index[i].resize(total_partition);
            for (auto j = 0; j < total_partition; j++) {
                write_containers[i][j].resize(total_partition);
                write_edge_buffer[i][j].resize(total_partition);
                write_edge_buffer_index[i][j].resize(total_partition, 0);
                for (auto k = 0; k < total_partition; k++) {
                    write_containers[i][j][k] = concurrent_buffer_pool_->Alloc();
                    write_edge_buffer[i][j][k] = (FileEdge*)write_containers[i][j][k].data;
                }
            }
        }
        
        size_t* start_offset = new size_t[buffering_degree];
        size_t* buffer_rw_size = new size_t[buffering_degree];
        std::queue<std::future<void>> read_reqs_to_wait;
        FileEdge** read_buffer = new FileEdge*[buffering_degree];
        SimpleContainer* read_containers = new SimpleContainer[buffering_degree];

        int64_t total_read_size = 0, total_write_size = 0, total_dummy_edge_size = 0;

        for (int64_t i = 0; i < num_chunks_; i++) {
            for (int64_t j = 0; j < num_chunks_; j++) {
                size_t processed_size = 0;
                size_t real_processed_size = 0;
                int cur = 0;
                int prev_cur = buffering_degree - 1;
                
                OpenGrid_IO(i, j, false, true, false);
                size_t file_size = io_file_size_[i][j];
                
                if (file_size == 0) { 
                    CloseGrid_IO(i, j, true);
                    continue; 
                }

                for (int k = 0; k < buffering_degree; k++) {
                    read_containers[k] = concurrent_buffer_pool_->Alloc();
                    read_buffer[k] = (FileEdge*)read_containers[k].data;
                    start_offset[k] = 0;
                    buffer_rw_size[k] = buffer_size;
                }

                for(int k = 0; k < buffering_degree; k++) {
                    if(k != 0) start_offset[k] = start_offset[k - 1] + buffer_rw_size[k - 1];
                    buffer_rw_size[k] = std::min(buffer_rw_size[k], file_size - start_offset[k]);
                    INVARIANT(start_offset[k] % sizeof(FileEdge) == 0);
                    INVARIANT(buffer_rw_size[k] % sizeof(FileEdge) == 0);
                    if(buffer_rw_size[k] > 0) {
                        total_read_size += buffer_rw_size[k];
                        read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][j], (char *)&read_buffer[k][0], start_offset[k], buffer_rw_size[k]));
                    }
                }

                while(processed_size < file_size) {
                    if (!read_reqs_to_wait.empty()) {
                        read_reqs_to_wait.front().get();
                        read_reqs_to_wait.pop();
                    }
#pragma omp parallel num_threads(num_threads_)
                    {
                        FileEdge edge;
                        int thread_num = omp_get_thread_num();
                        int64_t machine, machine_dst, index;
                        int64_t write_size = 0, dummy_edge_size = 0;
#pragma omp for
                        for(int64_t idx = 0; idx < (buffer_rw_size[cur] / sizeof(FileEdge)); idx++) {
                            edge = read_buffer[cur][idx];
                            if(!(edge.src_vid_ == -1 && edge.dst_vid_ == -1)) {
                                INVARIANT(edge.src_vid_ <= total_vertices && edge.src_vid_ >= 0);
                                INVARIANT(edge.dst_vid_ <= total_vertices && edge.dst_vid_ >= 0);
                                machine = dst_machine(edge.src_vid_);
                                machine_dst = dst_machine(edge.dst_vid_);
                                INVARIANT(machine < total_partition && machine >= 0);
                                INVARIANT(machine_dst < total_partition && machine_dst >= 0);

                                index = write_edge_buffer_index[thread_num][machine][machine_dst];
                                write_edge_buffer[thread_num][machine][machine_dst][index] = edge;
                                write_edge_buffer_index[thread_num][machine][machine_dst]++;

                                if (write_edge_buffer_index[thread_num][machine][machine_dst] == buffer_full_size) {
                                    write_reqs_to_wait[thread_num].push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoFile, this, &send_files[machine][machine_dst], (char *)&write_edge_buffer[thread_num][machine][machine_dst][0], buffer_size, write_containers[thread_num][machine][machine_dst]));
                                    write_size += buffer_size;
                                    write_containers[thread_num][machine][machine_dst] = concurrent_buffer_pool_->Alloc();
                                    write_edge_buffer[thread_num][machine][machine_dst] = (FileEdge*)write_containers[thread_num][machine][machine_dst].data;
                                    write_edge_buffer_index[thread_num][machine][machine_dst] = 0;
                                }
                            } else {
                                dummy_edge_size += sizeof(FileEdge);
                            }
                        }
                        std::atomic_fetch_add((std::atomic<int64_t>*)&total_write_size, (int64_t)write_size);
                        std::atomic_fetch_add((std::atomic<int64_t>*)&total_dummy_edge_size, (int64_t)dummy_edge_size);
                    }

                    real_processed_size += buffer_rw_size[cur];
                    processed_size = start_offset[cur] + buffer_rw_size[cur];

                    start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
                    buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
                    INVARIANT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
                    INVARIANT(start_offset[cur] % sizeof(FileEdge) == 0);
                    INVARIANT(buffer_rw_size[cur] % sizeof(FileEdge) == 0);
                    if (buffer_rw_size[cur] > 0) {
                        read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_readBufferfromDisk, this, &io_reader_[i][j], (char *)&read_buffer[cur][0], start_offset[cur], buffer_rw_size[cur]));
                        total_read_size += buffer_rw_size[cur];
                    }
                    prev_cur = cur;
                    cur = (cur + 1) % buffering_degree;
                    ALWAYS_ASSERT(cur >= 0 && cur < buffering_degree);
                }
                //while(!read_reqs_to_wait.empty()) {
                //    read_reqs_to_wait.front().get();
                //    read_reqs_to_wait.pop();
                //} //XXX maybe needless
                for (int k = 0; k < buffering_degree; k++)
                    concurrent_buffer_pool_->Dealloc(read_containers[k]);
                CloseGrid_IO(i, j, true);
            }
        }
        
        for (int64_t thread_num = 0; thread_num < num_threads_; thread_num++) {
            for (int64_t i = 0; i < total_partition; i++) {
                for (int64_t j = 0; j < total_partition; j++) {
                    write_reqs_to_wait[thread_num].push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoFile, this, &send_files[i][j], (char*)&write_edge_buffer[thread_num][i][j][0], write_edge_buffer_index[thread_num][i][j] * sizeof(FileEdge), write_containers[thread_num][i][j]));
                    total_write_size += (write_edge_buffer_index[thread_num][i][j] * sizeof(FileEdge));
                }
            }
        }
#pragma omp parallel num_threads(num_threads_)
        {
            int thread_num = omp_get_thread_num();
            while(!write_reqs_to_wait[thread_num].empty()) {
                write_reqs_to_wait[thread_num].front().get();
                write_reqs_to_wait[thread_num].pop();
            }
        }


        for(int64_t i = 0; i < total_partition; i++) {
            for(int64_t j = 0; j < total_partition; j++) {
                send_files[i][j].Close();
            }
        }
        //fprintf(stdout, "[%ld] Total read size = %ld, Total write size + Total dummy edge size = %ld + %ld = %ld\n", PartitionStatistics::my_machine_id(), total_read_size, total_write_size, total_dummy_edge_size, total_write_size + total_dummy_edge_size);
        
        /*
        int buffering_degree = 2;
        int64_t buffer_full_size = buffer_size / sizeof(FileEdge);
		std::queue<std::future<void>>* write_reqs_to_wait = new std::queue<std::future<void>>[num_threads_];

        std::vector<std::vector<SimpleContainer>> write_containers(num_threads_);
        std::vector<Blocking_Turbo_bin_io_handler> send_files(total_partition);
        std::vector<std::vector<FileEdge*>> write_edge_buffer(num_threads_);
        std::vector<std::vector<int>> write_edge_buffer_index(num_threads_);

        for (auto i = 0; i < total_partition; i++) {
            std::string file_name = std::string(outedges_send_partition_file_name)+std::to_string(i);
            truncate((destination_file_path + "/" + file_name).c_str(), 0);
            remove((destination_file_path + "/" + file_name).c_str());
            send_files[i].OpenFile((destination_file_path + file_name).c_str(), true, true, true);
        }

        for (auto i = 0; i < num_threads_; i++) {
            write_containers[i].resize(total_partition);
            write_edge_buffer[i].resize(total_partition);
            write_edge_buffer_index[i].resize(total_partition, 0);
            for (auto j = 0; j < total_partition; j++) {
                write_containers[i][j] = concurrent_buffer_pool_->Alloc();
                write_edge_buffer[i][j] = (FileEdge*)write_containers[i][j].data;
            }
        }
        for(int64_t i = 0; i < num_chunks_; i++) {
            for(int64_t j = 0; j < num_chunks_; j++) {
                if(OpenGrid(i, j) == ReturnStatus::OK) {
                    int64_t edge_size = ofile_size_[i][j] / sizeof(FileEdge);
#pragma omp parallel num_threads(num_threads_)
                    {
                        int thread_num = omp_get_thread_num();
                        int64_t machine, index;
                        FileEdge edge;
#pragma omp for
                        for(int64_t k = 0; k < edge_size; k++) {
                            //read edge and push into buffer
                            edge = optr_[i][j][k];
                            if(!(edge.src_vid_ == -1 && edge.dst_vid_ == -1)) {
                                if(edge.src_vid_ > total_vertices || edge.dst_vid_ > total_vertices || edge.src_vid_ < 0 || edge.dst_vid_ < 0) {
                                    fprintf(stderr, "Wrong src = %lld, dst = %lld in relabeld_file\n", (int64_t)edge.src_vid_, (int64_t)edge.dst_vid_);
                                }
                                INVARIANT(edge.src_vid_ <= total_vertices && edge.src_vid_ >= 0);
                                INVARIANT(edge.dst_vid_ <= total_vertices && edge.dst_vid_ >= 0);
                                machine = dst_machine(edge.src_vid_);
                                INVARIANT(machine <= total_partition);
                                index = write_edge_buffer_index[thread_num][machine];

                                write_edge_buffer[thread_num][machine][index] = edge;
                                write_edge_buffer_index[thread_num][machine]++;

                                if(write_edge_buffer_index[thread_num][machine] == buffer_full_size) {
#pragma omp critical
                                    {
                                        write_reqs_to_wait[thread_num].push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoFile, this, &send_files[machine], (char *)&write_edge_buffer[thread_num][machine][0], buffer_size, write_containers[thread_num][machine]));
                                        write_edge_buffer_index[thread_num][machine] = 0;
                                        write_containers[thread_num][machine] = concurrent_buffer_pool_->Alloc();
                                        write_edge_buffer[thread_num][machine] = (FileEdge*)write_containers[thread_num][machine].data;
                                    }
                                }
                            }
                        }
                    }
                }
                CloseGrid(i, j, true);
            }
        }
        
        for (int64_t thread_num = 0; thread_num < num_threads_; thread_num++) {
            for (int64_t i = 0; i < total_partition; i++) {
                write_reqs_to_wait[thread_num].push(Aio_Helper::async_pool.enqueue(PartitionedEdges::call_writeBuffertoFile, this, &send_files[i], (char*)&write_edge_buffer[thread_num][i][0], write_edge_buffer_index[thread_num][i] * sizeof(FileEdge), write_containers[thread_num][i]));
            }
        }
#pragma omp parallel num_threads(num_threads_)
        {
            int thread_num = omp_get_thread_num();
            while(!write_reqs_to_wait[thread_num].empty()) {
                write_reqs_to_wait[thread_num].front().get();
                write_reqs_to_wait[thread_num].pop();
            }
        }
        
        for(int64_t i = 0; i < total_partition; i++) {
            send_files[i].Close();
        }
        */


        //delete pointers, temp_arr, temp_index, containers
        //delete[] read_buffer;
        //delete start_offset;
        //delete buffer_rw_size;
        //delete read_containers;

        return ReturnStatus::OK;
    }

    static void call_writeBuffertoFile(PartitionedEdges* pointer, Blocking_Turbo_bin_io_handler* handler,  char* buffer, std::size_t size_to_append, SimpleContainer cont) {
        pointer->writeBuffertoFile(handler, buffer, size_to_append, cont);
    }

    void writeBuffertoFile(Blocking_Turbo_bin_io_handler* handler, char* buffer, std::size_t size_to_append, SimpleContainer cont) {
        //write buffer to file or send through network
        handler->Append(size_to_append, buffer);
        concurrent_buffer_pool_->Dealloc(cont);
    }

    static void call_writeBuffertoDisk(PartitionedEdges* pointer, Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_write, SimpleContainer cont) {
        pointer->writeBuffertoDisk(handler, buffer, start_offset, size_to_write, cont);
    }

    void writeBuffertoDisk(Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_write, SimpleContainer cont) {
        //			lock.lock();
        write_timer.start_timer(0);
        total_write_timer.start_timer(0);
        turbo_timer tmr;
        tmr.start_timer(0);
        LOG(INFO) << "Real Write " << handler->fdval() << " offset : " << start_offset;
        handler->Write(start_offset, size_to_write, buffer);
        tmr.stop_timer(0);
        char buf[256];
        sprintf(&buf[0], "\t[%lld] Write Disk speed = %.3fMB/sec\n", PartitionStatistics::my_machine_id(), size_to_write / (tmr.get_timer(0) * 1024 * 1024L));
        write_timer.stop_timer(0);
        total_write_timer.stop_timer(0);
        concurrent_buffer_pool_->Dealloc(cont);
        //		lock.unlock();
    }

    static void call_readBufferfromDisk(PartitionedEdges* pointer, Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_read) {
        pointer->readBufferfromDisk(handler, buffer, start_offset, size_to_read);
    }
    static void call_readBufferfromDisk_Direct(PartitionedEdges* pointer, Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_read) {
        pointer->readBufferfromDisk_Direct(handler, buffer, start_offset, size_to_read);
    }

    void readBufferfromDisk(Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_read) {
        read_timer.start_timer(0);
        total_read_timer.start_timer(0);
        turbo_timer tmr;
        tmr.start_timer(0);
        //fprintf(stdout, "[%lld] Read from %ld + %ld\n", PartitionStatistics::my_machine_id(), start_offset, size_to_read);
        handler->Read(start_offset, size_to_read, buffer);
        tmr.stop_timer(0);
        char buf[256];
        sprintf(&buf[0], "\t[%lld] Read Disk speed = %.3fMB/sec\n", PartitionStatistics::my_machine_id(), size_to_read / (tmr.get_timer(0) * 1024 * 1024L));
        LOG(INFO) << std::string(buf);
        read_timer.stop_timer(0);
        total_read_timer.stop_timer(0);
    }

    //XXX remove?
    void readBufferfromDisk_Direct(Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_read) {
        struct iocb cb;
        struct iocb* iocbs = &cb;
        struct io_event events[1];
        io_context_t ctx;
        int fd;
        int res;

        read_timer.start_timer(0);
        total_read_timer.start_timer(0);
        turbo_timer tmr;
        tmr.start_timer(0);

        memset(&ctx, 0, sizeof(ctx));
        memset(&cb, 0, sizeof(cb));
        fd = handler->fdval();
        if(io_setup(1, &ctx) < 0) {
            printf("io_setup error\n");
            exit(-1);
        }
        io_prep_pread(&cb, fd, &buffer[0], size_to_read, start_offset);
        res = io_submit(ctx, 1, &iocbs);
        if(res < 0) {
            printf("io_submit error\n");
            exit(-1);
        }
        res = io_getevents(ctx, 0, 1, events, NULL);
        res = io_destroy(ctx);
        if(res < 0) {
            printf("io_destroy Error: %d\n", res);
            exit(-4);
        }

        //handler->Read(start_offset, size_to_read, buffer);
        tmr.stop_timer(0);
        char buf[256];
        sprintf(&buf[0], "\t[%lld] Read Disk speed = %.3fMB/sec\n", PartitionStatistics::my_machine_id(), size_to_read / (tmr.get_timer(0) * 1024 * 1024L));
        LOG(INFO) << std::string(buf);
        read_timer.stop_timer(0);
        total_read_timer.stop_timer(0);
    }
    private:
        std::string partitioned_file_path_;
        std::string partitioned_file_path2_;
        int64_t num_chunks_;
        int64_t buffer_size_;
        Turbo_bin_mmapper** oreader_;
        Turbo_bin_io_handler** io_reader_;
        Turbo_bin_io_handler** io_reader2_;
        FileEdge*** optr_;
        std::size_t** ofile_size_;
        std::size_t** io_file_size_;
        FixedSizeConcurrentBufferPool* concurrent_buffer_pool_;
        turbo_timer read_timer;
        turbo_timer write_timer;
        atom lock;
        int** marking_table;
        int64_t num_threads_;

        const char* outedges_send_partition_file_name = "send_";
        const char* inedges_send_partition_file_name = "send_dst_";
};

