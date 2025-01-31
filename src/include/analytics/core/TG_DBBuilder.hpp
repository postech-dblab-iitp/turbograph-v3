#pragma once

/*
 * Design of the TG_DBBuilder
 *
 * This class is a variant of the class BinaryCollector for building DB. The 
 * difference is that the final result is stored directly as the DB, which is
 * a list of slotted pages, rather than as a sorted edge list. The API is the 
 * same as that of the BinaryCollector class.
 */

#include <vector>
#include <fstream>
#include <string>
#include "Splittable_Turbo_bin_io_handler.hpp"
#include "VidRangePerPage.hpp"
#include "turbo_dist_internal.hpp"
#include "tbb/atomic.h"
#include "tbb/task_group.h"

template <typename InputNodeType, typename Degree_t, typename FileNodeType>
class GraphPreprocessor;

template <typename T>
class TG_DBBuilder {
  public:
    static const size_t BUFFER_COUNT = 2; //DO_NOT_CHANGE

	TG_DBBuilder() {}
    TG_DBBuilder(std::string output_filename,
                 std::string vidrangeperpage_filename,
                 size_t unit_size) : output_file_name(output_filename),
                                     vidrangeperpage_file_name(vidrangeperpage_filename),
                                     buffer_size(unit_size) {
		num_pages_in_buffer = (buffer_size / TBGPP_PAGE_SIZE); //XXX (buffer_size * sizeof(T)) / TBGPP_PAGE_SIZE
		buffer_size = TBGPP_PAGE_SIZE * num_pages_in_buffer;
		ALWAYS_ASSERT(num_pages_in_buffer >= 1);
		page_buffer.resize(num_pages_in_buffer * BUFFER_COUNT); 
		write_from[0] = buf_begin = page_buffer.data();
		write_from[1] = &page_buffer[num_pages_in_buffer];
		for(int64_t i = 0; i < num_pages_in_buffer * BUFFER_COUNT; i++) {
			for(int64_t j = 0; j < TBGPP_PAGE_SIZE / sizeof(node_t); j++) {
				page_buffer[i][j] = 0;
			}
		}

        range_to_append.Set(-1, -1);

        write_tasks.run([&]{ file.OpenFile(output_file_name.c_str(), true, true, true); });
        vidrangeperpage.OpenVidRangePerPage(1, vidrangeperpage_file_name);
    }
    ~TG_DBBuilder() { }
    
    void forward(T edge) {
		node_t prev_src;
        INVARIANT(page_idx >= 0 && page_idx < num_pages_in_buffer * BUFFER_COUNT);
        INVARIANT(page_buffer.size() == num_pages_in_buffer * BUFFER_COUNT);
		Page& page = page_buffer[page_idx];
		
		if(page.NumEntry() == 0) {
			prev_src = -1;
		} else {
			prev_src = page.GetSlot(page.NumEntry() -1)->src_vid;
		}

		if (prev_src != edge.src_vid_) {
			int64_t free_space = 0;
			if (page.NumEntry() != 0) free_space = page.GetSlot(page.NumEntry() -1)->end_offset;
			if ((void *)&page[free_space + 1] >= (void *)page.GetSlot(page.NumEntry())) {
                vidrangeperpage.push_back(0, range_to_append); 
                range_to_append.Set(-1, -1);
				page_idx = (page_idx + 1) % (num_pages_in_buffer * BUFFER_COUNT);
				if (page_idx % num_pages_in_buffer == 0) {
            		write_tasks.wait();
            		write_tasks.run([&]{ write(); });
					iteration++;
				}
                INVARIANT(page_idx >= 0 && page_idx < num_pages_in_buffer * BUFFER_COUNT);
                INVARIANT(page_buffer.size() == num_pages_in_buffer * BUFFER_COUNT);
				page = page_buffer[page_idx];
				free_space = 0;
			}
			INVARIANT(page[free_space] == 0);
			page[free_space] = edge.dst_vid_;
			page.NumEntry()++;
			page.GetSlot(page.NumEntry() -1)->src_vid = edge.src_vid_;
			page.GetSlot(page.NumEntry() -1)->end_offset = free_space + 1;
            if (range_to_append.GetBegin() == -1) range_to_append.SetBegin(edge.src_vid_);
            range_to_append.SetEnd(edge.src_vid_);
            edge_cnt++;
		} else {
			int64_t free_space = page.GetSlot(page.NumEntry() -1)->end_offset;
			if((void *)&page[free_space + 1] >= (void *)page.GetSlot(page.NumEntry() -1)) {
                vidrangeperpage.push_back(0, range_to_append); 
                range_to_append.Set(-1, -1);
				page_idx = (page_idx + 1) % (num_pages_in_buffer * BUFFER_COUNT);
				if(page_idx % num_pages_in_buffer == 0) {
                    write_tasks.wait();
                    system_fprintf(0, stdout, "call write()\n");
                    write_tasks.run([&]{ write(); });
					iteration++;
				}
                INVARIANT(page_idx >= 0 && page_idx < num_pages_in_buffer * BUFFER_COUNT);
                INVARIANT(page_buffer.size() == num_pages_in_buffer * BUFFER_COUNT);
				page = page_buffer[page_idx];
				page.NumEntry()++;
				free_space = 0;
				page.GetSlot(page.NumEntry() -1)->src_vid = edge.src_vid_;
			}
			ALWAYS_ASSERT(page[free_space] == 0);
			page[free_space] = edge.dst_vid_;
			page.GetSlot(page.NumEntry() -1)->end_offset = free_space +1;
            if (range_to_append.GetBegin() == -1) range_to_append.SetBegin(edge.src_vid_);
            range_to_append.SetEnd(edge.src_vid_);
            edge_cnt++;
		}
    }
    bool close() {
        if (!(range_to_append.GetBegin() == -1 && range_to_append.GetEnd() == -1))
            vidrangeperpage.push_back(0, range_to_append);
        vidrangeperpage.SaveOne();
		flush();
        file.Close();
		page_buffer.resize(0);
    }
    int64_t get_count() {
        return edge_cnt;
    }
  private:
    void write() {
		//write_from = buf_begin + (((page_idx + num_pages_in_buffer) % (num_pages_in_buffer * BUFFER_COUNT)) * sizeof(Page));
		int64_t temp_idx = (cur == 0) ? 0 : num_pages_in_buffer; 
		//fprintf(stdout, "[%lld] %lldth File Append\n", PartitionStatistics::my_machine_id(), ++written_count);
		//written_size += buffer_size;
        file.Append(buffer_size, (char*)write_from[cur]);
		cur = (cur + 1) % BUFFER_COUNT;
        //file.write((char*)write_from, buffer_size);
		//XXX clean all pages
		for(int64_t i = temp_idx; i < temp_idx + num_pages_in_buffer; i++) {
			for(int64_t j = 0; j < TBGPP_PAGE_SIZE / sizeof(node_t); j++) {
				page_buffer[i][j] = 0;
			}
		}
    }
    void flush() {
        write_tasks.wait();
		//write_from = buf_begin + (((page_idx / num_pages_in_buffer) * num_pages_in_buffer) * sizeof(Page));
        Page* write_until = &page_buffer[page_idx + 1];
		INVARIANT(write_until >= write_from[cur]);
		size_t size_to_flush = (size_t)((write_until - write_from[cur]) * TBGPP_PAGE_SIZE);
		INVARIANT(size_to_flush <= buffer_size);
		written_size += size_to_flush;
		file.Append(size_to_flush, (char*)write_from[cur]);
		//file.write((char*)write_from, write_until - write_from);
		//fprintf(stdout, "[%lld] write size = %lld\n", PartitionStatistics::my_machine_id(), write_until - write_from);
    }

    std::string output_file_name;
    std::string vidrangeperpage_file_name;
	std::vector<Page> page_buffer;
    tbb::task_group write_tasks;
    Turbo_bin_io_handler file;
    VidRangePerPage vidrangeperpage;
    Range<node_t> range_to_append;
    Page* write_from[BUFFER_COUNT];
	Page* buf_begin;
    size_t buffer_size;
	size_t written_size;
	int64_t num_pages_in_buffer;
	int64_t page_idx = 0;
	int64_t iteration = 0;
	int64_t edge_cnt = 0;
	int64_t written_count = 0;
	int cur = 0;
};

template <typename T>
class AbstractCollector {
  public:
    AbstractCollector(std::string out_filepath, size_t unit_size, int machine_num_) : out_file_path(out_filepath),
                                        buffer_size(unit_size), machine_num(machine_num_),
										sub_range(GraphPreprocessor<node_t, degree_t, file_t>::getSubchunkVidRange()) {	}
	AbstractCollector() {}
    ~AbstractCollector() {
        //close();
    }
    void init() {
		total_vertices = PartitionStatistics::my_num_internal_nodes() * PartitionStatistics::num_machines();
		num_subchunks_per_edge_chunk = PartitionStatistics::num_subchunks_per_edge_chunk();
        num_builders = UserArguments::VECTOR_PARTITIONS * num_subchunks_per_edge_chunk;
		builders.resize(num_builders);
		size_t builder_buffer_size = buffer_size / (builders.size());
		for(int i = 0; i < builders.size(); i++) {
			std::string output_filename = out_file_path + "/edgedb/edgedb" + std::to_string(machine_num * UserArguments::VECTOR_PARTITIONS * num_subchunks_per_edge_chunk + i);
            std::string vidrangeperpage_filename = out_file_path + "/edgedbVidRangePerPage" + std::to_string(machine_num * UserArguments::VECTOR_PARTITIONS * num_subchunks_per_edge_chunk + i);
			builders[i] = new TG_DBBuilder<T>(output_filename, vidrangeperpage_filename, builder_buffer_size);
		}
		edge_cnt = 0;
    }
    void forward(T edge) {
		int64_t dst_file = dst_vid_to_file_num(edge.dst_vid_);
        builders[dst_file]->forward(edge);
		edge_cnt++;
    }
    bool close() {
        int64_t temp_edge_count = 0;
		for(int i = 0; i < num_builders; i++) {
            temp_edge_count += builders[i]->get_count();
			builders[i]->close();
		}
		INVARIANT(temp_edge_count == edge_cnt);
		/*timer.start_timer(0);
		merge_db();
		timer.stop_timer(0);*/

		for(int i = 0; i < num_builders; i++) {
			delete builders[i];
		}
		builders.resize(0);
//		fprintf(stdout, "[%lld] Total edge cnt = %lld, merge_db elapsed time = %.2f\n", PartitionStatistics::my_machine_id(), edge_cnt, timer.get_timer(0));
    }

  private:
	int64_t dst_vid_to_file_num(int64_t dst_vid) {
		int idx = machine_num * UserArguments::VECTOR_PARTITIONS;
		for(int j = 0; j < UserArguments::VECTOR_PARTITIONS; j++) {
			for(int i = 0; i < num_subchunks_per_edge_chunk; i++) {
				if(dst_vid <= sub_range[j + idx][i]) return (j * num_subchunks_per_edge_chunk + i);
			}
		}
		//fprintf(stdout, "dst_vid = %lld, idx = %lld, Subchu...[idx][num_subchunks_per_edge_chunk-1] = %lld\n", dst_vid, j, sub_range[j][num_subchunks_per_edge_chunk-1]);
		INVARIANT(false);
		abort();
	}

/*	void merge_db() {
		size_t total_file_size = 0;
		const size_t transfer_unit = TBGPP_PAGE_SIZE;

		std::string fpid_path = out_file_path;
		fpid_path += "/FirstPageIds.txt";
		remove(fpid_path.c_str());
		std::ofstream fpid;
		fpid.open(fpid_path.c_str());

		std::string new_path = out_file_path + "/edgedb";
		{ 
			remove(new_path.c_str());
			mode_t old_umask;                                                                
			old_umask = umask(0); 
			int is_error = mkdir((new_path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
			if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
				fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (new_path).c_str(), errno);
				umask(old_umask);  
				return;
			}
			umask(old_umask);
		}    

		Turbo_bin_io_handler out_file((new_path + "/edgedb").c_str(), true, true);
		char* buffer = new char[transfer_unit];
		for (auto i = 0; i < num_builders; i++) {
			Turbo_bin_io_handler in_file((new_path+std::to_string(i)).c_str(), false, false);
			size_t file_size = in_file.file_size();
			size_t data_left = file_size;
			while (data_left) {
				size_t to_read = MIN(data_left, transfer_unit);
				in_file.Read(file_size - data_left, to_read, buffer);
				out_file.Append(to_read, buffer);
				data_left -= to_read;
			}
			fpid << i << " " << (total_file_size / transfer_unit) << std::endl;
			total_file_size += file_size;
			in_file.Close(true);
		}
		out_file.Close();
		fpid.close();
		delete[] buffer;
	}*/

	int machine_num, num_builders;
	std::vector<TG_DBBuilder<T>*> builders;
    std::string file_name;
    std::string out_file_path;
    Turbo_bin_io_handler out_file;
    Turbo_bin_io_handler in_file;
    size_t buffer_size;
	int64_t total_vertices;
	int64_t edge_cnt;
	std::vector<std::vector<int64_t>>& sub_range;
	int64_t num_subchunks_per_edge_chunk;
	turbo_timer timer;
};


