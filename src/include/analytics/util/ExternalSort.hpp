#pragma once

/*
 * Design of the ExternalSort
 * 
 * This class implements an external sort algorithm for GraphPreprocessor (BBP). 
 * Among the edges distributed across multiple machines, edges with source vertex 
 * ids included in the vertex id range assigned to the machine are transmitted 
 * through the network (what GraphRedistributor does). The transferred edges are 
 * stored in several sorted runs. In the ExternalSort class, all edges are sorted 
 * through an external sort algorithm to create one large output file.
 *
 * The core functions provided are as follows:
 *     - sorted_run:
 *         In this function, multiple unsorted edge list files are read into the 
 *         buffer (the number of files to be read will be determined by the size 
 *         of each edge file and the size of the buffer), and sorted to create 
 *         one sorted run. Note that this function is deprecated currently.
 *     - merge:
 *         In this function, we utilize KWayMergeTree to sort multiple sorted
 *         runs. Edges are extracted one by one from KWayMergeTree, and the 
 *         extracted edges are appended to the write buffer through the forward() 
 *         function. When the buffer is full, the buffer is appended to the 
 *         output file. To increase parallelism, we use a double buffering
 *         strategy.
 *     - merge_and_builddb:
 *         Instead of storing the sorted edge list to disk as a result as 
 *         in the merge() function, this function stores the result to disk 
 *         in the form of a list of slotted pages.
 */ 

#include <string>
#include <thread>
#include <queue>

#include "analytics/core/TG_DBBuilder.hpp"
#include "analytics/core/TypeDef.hpp"
#include "analytics/io/ParallelBinaryHandlers.hpp"
#include "analytics/io/EdgePairListReader.hpp"
#include "analytics/datastructure/KWayMergeTree.hpp"
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"
#include "tbb/concurrent_queue.h"
#include "tbb/parallel_sort.h"
#include "tbb/parallel_pipeline.h"
#include "tbb/task_group.h"

#define TIMEDIFF_MILLISEC(begin, end) ((double)std::chrono::duration_cast<std::chrono::milliseconds>((end) - (begin)).count())
#define TIMEDIFF_MICROSEC(begin, end) ((double)std::chrono::duration_cast<std::chrono::microseconds>((end) - (begin)).count())
template <typename InputNodeType, typename Degree_t, typename FileNodeType>
class GraphPreprocessor;

struct ExternalSortArgs {
	size_t buffer_count;
	size_t buffer_size;
    size_t block_counts;
    size_t in_block_counts;
    size_t* block_count;
	int machine_num;
	std::string out_file_path;
	std::string subchunk_vid_range_path;
	std::string temp_path;
    std::string prefix;
	bool delete_original;
	bool validation_mode;
};

template <typename InputT, typename OutputT>
class ExternalSort {
	typedef EdgePairEntry<InputT> InEdge;
	typedef EdgePairEntry<OutputT> OutEdge;
  public:
	static ExternalSortArgs sorted_run(std::vector<std::string>& files,
	                                   ExternalSortArgs args) {
		auto total_begin_time = std::chrono::steady_clock::now();
		tbb::concurrent_queue<BufferSet<InEdge>*> buffer_queue;
		for (int i = 0; i < args.buffer_count; i++)
			buffer_queue.push(new BufferSet<InEdge>(args.buffer_size / args.buffer_count));

		//ReadEdge<InputT> input_f(files, &buffer_queue, args.buffer_size / args.buffer_count);
		//SortEdge<InputT> sort_f;
		//WriteEdge<InputT> output_f(args.temp_path, &buffer_queue);

		auto begin_time = std::chrono::steady_clock::now();
    tbb::filter<void, void*> input_f(tbb::filter_mode::serial_in_order, 
        ReadEdge(files, &buffer_queue, args.buffer_size / args.buffer_count));
    tbb::filter<void*, void*> sort_f(tbb::filter_mode::serial_out_of_order,
        SortEdge());
    tbb::filter<void*, void> output_f(tbb::filter_mode::serial_out_of_order,
        WriteEdge(args.temp_path, &buffer_queue));

    
    tbb::filter<void, void> f = input_f & sort_f & output_f;
    tbb::parallel_pipeline(args.buffer_count, f);

		/*tbb::parallel_pipeline run;
		run.add_filter(input_f);
		run.add_filter(sort_f);
		run.add_filter(output_f);
		run.run(args.buffer_count);
		run.clear();*/
    //args.block_counts = output_f.get_count();
    glob_t gl;
    int64_t sorted_run_block_count = 0;
    if(glob((std::string(args.temp_path.c_str()) + "/sorted_run*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
      sorted_run_block_count = gl.gl_pathc;

    args.block_counts = sorted_run_block_count;
		auto end_time = std::chrono::steady_clock::now();
		BufferSet<InEdge>* temp;
		while (buffer_queue.try_pop(temp))
			delete temp;
		auto total_end_time = std::chrono::steady_clock::now();
		system_fprintf(0, stdout, "Generate Sorted Run END, # sorted runs = %ld, Total Elapsed : %.3f, sort time %.3f\n", sorted_run_block_count, TIMEDIFF_MILLISEC(total_begin_time, total_end_time) / 1000, TIMEDIFF_MICROSEC(begin_time, end_time) / 1000000);
		return args;
	}

	static ExternalSortArgs merge(ExternalSortArgs args) {
		auto begin_time2 = std::chrono::steady_clock::now();
        int64_t block_counts;
        if (args.prefix == "") block_counts = args.block_counts;
        else block_counts = args.in_block_counts;
		if (block_counts == 0) {
			fprintf(stdout, "[%lld] No blocks to merge!\n", PartitionStatistics::my_machine_id());
			return args;
		}
		size_t buffer_size = args.buffer_size / 2;
        int64_t duplicate_total = 0, self_total = 0;
        std::string out_filename = args.out_file_path + "/" + args.prefix + "tbgpp_" + std::to_string(args.machine_num);
        BinaryCollector<OutEdge> merged_file(out_filename, buffer_size);
        auto** merged_file_input = merged_file.get_input();
        auto** sorted_runs = new BinaryListDispenser<InEdge>*[block_counts];
        InEdge cur_edge;
        InEdge prev_edge;
        OutEdge out_edge;
        std::vector<InEdge**> tree_players(block_counts);
        int64_t duplicate = 0;
        int64_t self = 0;

        double buffer_alloc_time = 0.0;
        for (int64_t i = 0; i < block_counts; i++) {
            auto begin_time1 = std::chrono::steady_clock::now();
            std::string in_filename = args.temp_path
                + "/" + args.prefix + "sorted_run_" + std::to_string(i);
            sorted_runs[i] = new BinaryListDispenser<InEdge>(in_filename, buffer_size / block_counts,
                    !args.validation_mode);
            auto end_time1 = std::chrono::steady_clock::now();
            buffer_alloc_time += TIMEDIFF_MILLISEC(begin_time1, end_time1);
            sorted_runs[i]->forward();
            tree_players[i] = sorted_runs[i]->get_output();
        }
        prev_edge.src_vid_ = (InputT)(int64_t)-1;
        prev_edge.dst_vid_ = (InputT)(int64_t)-1;

		auto begin_time3 = std::chrono::steady_clock::now();
        KWayMergeTree<InEdge, std::greater<InEdge>> tree(tree_players);
        if (block_counts == 1) {
            auto** sorted_run_output = sorted_runs[0]->get_output();
            do {
                cur_edge = **sorted_run_output;
                //if (prev_edge == cur_edge) duplicate++;
                //if (cur_edge.src_vid_ == cur_edge.dst_vid_) self++; //TODO self edges are already removed in Range Partitioning Phase
                //if (prev_edge != cur_edge) { //XXX tslee: for HashMap
                if (prev_edge != cur_edge && cur_edge.src_vid_ != cur_edge.dst_vid_) {
                    out_edge.src_vid_ = (OutputT)cur_edge.src_vid_;
                    out_edge.dst_vid_ = (OutputT)cur_edge.dst_vid_;
                    ALWAYS_ASSERT(prev_edge < cur_edge);
                    **merged_file_input = out_edge;
                    prev_edge = cur_edge;
                    merged_file.forward();
                }
            } while (sorted_runs[0]->forward());
        } else {
            while (!tree.empty()) {
                tree.update_top();
                cur_edge = tree.top_data();

                if (prev_edge == cur_edge) duplicate++;
                if (cur_edge.src_vid_ == cur_edge.dst_vid_) self++;
                //if (prev_edge != cur_edge) { //XXX tslee: for HashMap
                if (prev_edge != cur_edge && cur_edge.src_vid_ != cur_edge.dst_vid_) {
                    out_edge.src_vid_ = (OutputT)cur_edge.src_vid_;
                    out_edge.dst_vid_ = (OutputT)cur_edge.dst_vid_;
                    ALWAYS_ASSERT(prev_edge < cur_edge);
                    **merged_file_input = out_edge;
                    prev_edge = cur_edge;
                    merged_file.forward();
                }
                if (!sorted_runs[tree.top_id()]->forward()) tree.remove_top();
            }
        }
        merged_file.close();
        //std::atomic_fetch_add((std::atomic<int64_t>*)&duplicate_total, (int64_t)duplicate);
        //std::atomic_fetch_add((std::atomic<int64_t>*)&self_total, (int64_t)self);
        //MPI_Allreduce(MPI_IN_PLACE, &duplicate, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
        //MPI_Allreduce(MPI_IN_PLACE, &self, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
        //if(args.machine_num == 0)
        //    fprintf(stdout, "Total duplicate edges = %lld, Total self edges = %lld\n", duplicate, self);
        for (int64_t i = 0; i < block_counts; i++) {
            sorted_runs[i]->close();
            delete sorted_runs[i];
        }
        delete[] sorted_runs;
		auto end_time3 = std::chrono::steady_clock::now();
		auto end_time2 = std::chrono::steady_clock::now();
		system_fprintf(0, stdout, "Merge END, Elapsed : %.3f(%.3f - %.3f), sort time %.3f\n", TIMEDIFF_MILLISEC(begin_time2, end_time2) / 1000 - buffer_alloc_time / 1000, TIMEDIFF_MILLISEC(begin_time2, end_time2) / 1000, buffer_alloc_time / 1000, TIMEDIFF_MILLISEC(begin_time3, end_time3) / 1000);
        fprintf(stdout, "[%lld] Duplicate edges = %lld, Self edges = %lld in merge phase\n", args.machine_num, duplicate_total, self_total);

		return args;
	}
	static ExternalSortArgs merge_and_builddb(ExternalSortArgs args) {
		ExternalSort::calc_subchunk_vid_range(args);

        //int num_threads = std::min((int)PartitionStatistics::num_machines(), 8);
		size_t buffer_size = args.buffer_size / (2 * 16);
        int64_t total_edge_cnt = 0;
        double merge_time = 0.0, concat_time = 0.0;
		//std::string out_filename = args.out_file_path + "/edgedb";

        //XXX args.block_count[] ? args.block_counts
//#pragma omp parallel for num_threads(8)
        for (int64_t j = 0; j < PartitionStatistics::num_machines(); j++) {
            if (args.block_counts == 0) {
                fprintf(stdout, "[%lld] No blocks to merge!\n", PartitionStatistics::my_machine_id());
                continue;
            }
            AbstractCollector<OutEdge> merged_file(args.out_file_path, buffer_size * sizeof(InEdge), j);
            merged_file.init();
            auto** sorted_runs = new BinaryListDispenser<InEdge>*[args.block_counts];
            InEdge cur_edge;
            InEdge prev_edge;
            OutEdge out_edge;
            std::vector<InEdge**> tree_players(args.block_counts);
            int64_t duplicate = 0;
            int64_t self = 0;
            int64_t edge_cnt = 0;
            for (int64_t i = 0; i < args.block_counts; i++) {
                std::string in_filename = args.temp_path
                    + "/sorted_run_" + std::to_string(i);
                    //+ "/sorted_run_" + std::to_string(j) + "-" + std::to_string(i);
                sorted_runs[i] = new BinaryListDispenser<InEdge>(in_filename, buffer_size / args.block_counts, true);
                sorted_runs[i]->forward();
                tree_players[i] = sorted_runs[i]->get_output();
            }
            prev_edge.src_vid_ = (InputT)(int64_t)-1;
            prev_edge.dst_vid_ = (InputT)(int64_t)-1;
            auto begin_time1 = std::chrono::steady_clock::now();

            KWayMergeTree<InEdge, std::greater<InEdge>> tree(tree_players); //XXX great 바꿔서 merge 할때 sort cost 없애자
            if (args.block_counts == 1) {
                auto** sorted_run_output = sorted_runs[0]->get_output();
                do {
                    cur_edge = **sorted_run_output;
                    //if (prev_edge == cur_edge) duplicate++;
                    //if (cur_edge.src_vid_ == cur_edge.dst_vid_) self++;
                    if (prev_edge != cur_edge && cur_edge.src_vid_ != cur_edge.dst_vid_) {
                        out_edge.src_vid_ = (OutputT)cur_edge.src_vid_;
                        out_edge.dst_vid_ = (OutputT)cur_edge.dst_vid_;
                        ALWAYS_ASSERT(prev_edge < cur_edge);
                        prev_edge = cur_edge;
                        merged_file.forward(out_edge);
                        //edge_cnt++;
                    }
                } while (sorted_runs[0]->forward());
            } else {
                while (!tree.empty()) {
                    tree.update_top();
                    cur_edge = tree.top_data();
                    //if (prev_edge == cur_edge) duplicate++;
                    //if (cur_edge.src_vid_ == cur_edge.dst_vid_) self++;
                    if (prev_edge != cur_edge && cur_edge.src_vid_ != cur_edge.dst_vid_) {
                        out_edge.src_vid_ = (OutputT)cur_edge.src_vid_;
                        out_edge.dst_vid_ = (OutputT)cur_edge.dst_vid_;
                        ALWAYS_ASSERT(prev_edge < cur_edge);
                        prev_edge = cur_edge;
                        merged_file.forward(out_edge);
                        //edge_cnt++;
                    }
                    if (!sorted_runs[tree.top_id()]->forward()) tree.remove_top();
                }
            }

            merged_file.close();

            for (int64_t i = 0; i < args.block_counts; i++) {
                sorted_runs[i]->close();
                delete sorted_runs[i];
            }
            delete[] sorted_runs;
            //std::atomic_fetch_add((std::atomic<int64_t>*)&total_edge_cnt, (int64_t)edge_cnt);
            auto end_time1 = std::chrono::steady_clock::now();
            merge_time += TIMEDIFF_MICROSEC(begin_time1, end_time1) / 1000000L;
        }
        auto begin_time2 = std::chrono::steady_clock::now();
		ExternalSort::finalize_builddb(args);
        auto end_time2 = std::chrono::steady_clock::now();
        concat_time += TIMEDIFF_MICROSEC(begin_time2, end_time2) / 1000000L;
		system_fprintf(0, stdout, "BuildDB END, Elapsed : %.3f, %.3f\n", merge_time, concat_time);
		return args;
	}

	static void validate(ExternalSortArgs args) {
		if (args.block_count == 0) {
			std::cout << "No blocks to validate!" << std::endl;
			return;
		}
		OutEdge cur_edge;
		OutEdge prev_edge;
		size_t buffer_size = args.buffer_size / 2;
		std::string out_filename = args.out_file_path + "/tbgpp_" + std::to_string(args.machine_num);
		BinaryDispenser<OutEdge> merged_file(out_filename, buffer_size, false);
		auto** sorted_runs = new BinaryListDispenser<InEdge>*[args.block_count];
		auto** merged_file_output = merged_file.get_output();
		auto*** sorted_run_outputs = new InEdge**[args.block_count];
		auto* sorted_run_remaining = new bool[args.block_count];
		for (int64_t i = 0; i < args.block_count; i++) {
			std::string in_filename = args.temp_path
			                          + "/sorted_run_" + std::to_string(i);
			sorted_runs[i] =
			    new BinaryListDispenser<InEdge>(in_filename,
			                                    buffer_size / args.block_count,
			                                    true);
			sorted_run_remaining[i] = sorted_runs[i]->forward();
			sorted_run_outputs[i] = sorted_runs[i]->get_output();
		}
		prev_edge.src_vid_ = (InputT)(int64_t)-1;
		prev_edge.dst_vid_ = (InputT)(int64_t)-1;

		while (merged_file.forward()) {
			bool check = false;
			cur_edge = **merged_file_output;
			if (!(cur_edge > prev_edge))
				std::cout << "MERGE ERROR: Duplicate or Sort error, Current Edge" << std::endl;
			if (cur_edge.src_vid_ == cur_edge.dst_vid_)
				std::cout << "MERGE ERROR: Self Edge" << std::endl;
			prev_edge = cur_edge;

			for (auto i = 0; i < args.block_count; i++)
				while (sorted_run_remaining[i]) {
					auto temp = **(sorted_run_outputs[i]);
					if (temp.src_vid_ > cur_edge.src_vid_ ||
					        ((temp.src_vid_ == cur_edge.src_vid_) && temp.dst_vid_ > cur_edge.dst_vid_)) break;
					else if ((temp.src_vid_ == cur_edge.src_vid_)
					         && (temp.dst_vid_ == cur_edge.dst_vid_))
						check = true;
					else if (temp.src_vid_ != temp.dst_vid_) {
						std::cout << "EXTSORT ERROR: Single existence in Sorted Run or Sort Error" << std::endl;
						std::cout << "ASSERTING: (" << cur_edge.src_vid_ << ", " << cur_edge.dst_vid_ << "), "
						          << "DETECTED: (" << temp.src_vid_ << ", " << temp.dst_vid_ << ")" << std::endl;
						throw "ASSERT FAILURE";
					}
					sorted_run_remaining[i] = sorted_runs[i]->forward();
				}
			if (!check) {
				std::cout << "MERGE ERROR: Exists only in Merged file" << std::endl;
				throw "ASSERT FAILURE";
			}
		}
		for (int64_t i = 0; i < args.block_count; i++) {
			sorted_runs[i]->close();
			delete sorted_runs[i];
		}
		delete[] sorted_runs;
		delete[] sorted_run_outputs;
		delete[] sorted_run_remaining;
	}
	
    static void calc_subchunk_vid_range(ExternalSortArgs args) {
		std::vector<std::vector<int64_t>>& sub_range = GraphPreprocessor<node_t, degree_t, file_t>::getSubchunkVidRange();
		std::vector<std::vector<int64_t>>& local_histogram = GraphPreprocessor<node_t, degree_t, file_t>::getHistogram();
		std::vector<int64_t> SumOfInDegreePerEdgeChunk;
		int64_t num_bins = local_histogram[0].size();
		int num_edge_chunks = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS;
		int num_subchunks_per_edge_chunk = 2;
		PartitionStatistics::num_subchunks_per_edge_chunk() = num_subchunks_per_edge_chunk;

		SumOfInDegreePerEdgeChunk.resize(num_edge_chunks, 0);
		sub_range.resize(num_edge_chunks);
		//std::vector<std::vector<node_t>> SubchunkDegreeSum;
		//SubchunkDegreeSum.resize(num_edge_chunks);

		int64_t idx2 = 0;
		// Find Vid Range for Subchunks for each Edge Chunk
		for(int i = 0; i < num_edge_chunks; i++) {
			sub_range[i].resize(num_subchunks_per_edge_chunk);

			int partition_id = i / UserArguments::VECTOR_PARTITIONS;
			int chunk_id = i % UserArguments::VECTOR_PARTITIONS;
			int64_t start_bin = ceil((double)(local_histogram[partition_id].size() * chunk_id) / UserArguments::VECTOR_PARTITIONS);
			int64_t end_bin = ceil((double)(local_histogram[partition_id].size() * (chunk_id + 1)) / UserArguments::VECTOR_PARTITIONS);
            //fprintf(stdout, "start_bin = %lld, end_bin = %lld, local_histogram[0].size() = %ld\n", start_bin, end_bin, local_histogram[0].size());
			for(int64_t idx = start_bin; idx < end_bin; idx++) {
				SumOfInDegreePerEdgeChunk[i] += local_histogram[partition_id][idx];
			}
			//fprintf(stdout, "SumOfInDegreePerEdgeChunks[%lld] = %lld\n", i, SumOfInDegreePerEdgeChunk[i]);

			int64_t num_edges_per_subchunk = (SumOfInDegreePerEdgeChunk[i] + num_subchunks_per_edge_chunk - 1) / num_subchunks_per_edge_chunk;

			int64_t acc_sum = 0;
			int64_t cur_subchunk_idx = 0;
			int64_t degree_sum = 0;

			for(int64_t idx = start_bin; idx < end_bin; idx++) {
				acc_sum += local_histogram[partition_id][idx];
				degree_sum += local_histogram[partition_id][idx];
				if(acc_sum >= num_edges_per_subchunk * (cur_subchunk_idx + 1)) {
					sub_range[i][cur_subchunk_idx] = ceil((double)((idx2 + idx + 1) * HISTOGRAM_BIN_WIDTH - 1) / 64) * 64;
					cur_subchunk_idx++;
					if(cur_subchunk_idx == num_subchunks_per_edge_chunk) {
						sub_range[i][cur_subchunk_idx - 1] = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(partition_id, chunk_id).GetEnd();
						break;
					}
				}
			}
			if(cur_subchunk_idx < num_subchunks_per_edge_chunk) {
				for(int j = cur_subchunk_idx; j < num_subchunks_per_edge_chunk; j++) {
					//fprintf(stdout, "%lld\n", i);
					sub_range[i][j] = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(partition_id, chunk_id).GetEnd();
				}
			}
			if(chunk_id == UserArguments::VECTOR_PARTITIONS - 1) {
				idx2 += local_histogram[partition_id].size();
			}
		}
		
        std::ofstream subrange_file((args.subchunk_vid_range_path + "/tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt").c_str());
        for(int i = 0; i < num_edge_chunks; i++) {
            for(int j = 0; j < num_subchunks_per_edge_chunk; j++) {
                if(i == 0 && j == 0) {
                    INVARIANT(sub_range[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
                    subrange_file << 0 << "\t" << sub_range[i][j] << "\n";
                } else if(j == 0) {
                    INVARIANT(sub_range[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
                    subrange_file << sub_range[i - 1][num_subchunks_per_edge_chunk - 1] + 1 << "\t" << sub_range[i][j] << "\n";
                } else {
                    INVARIANT(sub_range[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
                    subrange_file << sub_range[i][j - 1] + 1 << "\t" << sub_range[i][j] << "\n";
                }
            }
        }
		subrange_file.close();
	}
    
    static void finalize_builddb(ExternalSortArgs args) {
		size_t total_file_size = 0;
		const size_t transfer_unit = TBGPP_PAGE_SIZE;
        int64_t num_files = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS * 2; //XXX num_subchunks need to be parameterized

        std::string fpid_path = args.out_file_path + "/FirstPageIds.txt";
		remove(fpid_path.c_str());
		std::ofstream fpid;
		fpid.open(fpid_path.c_str());

		Turbo_bin_io_handler out_file((args.out_file_path + "/edgedb/edgedb").c_str(), true, true);
		char* buffer = new char[transfer_unit];
		for (auto i = 0; i < num_files; i++) {
			Turbo_bin_io_handler in_file((args.out_file_path+"/edgedb/edgedb"+std::to_string(i)).c_str(), false, false);
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
    }

  private:
	template <typename T>
	struct BufferSet {
		BufferSet(size_t size) : max_size(size),
			cur_size(0) {
			buffer = new T[size];
		}
		~BufferSet() {
			delete[] buffer;
		}
		T* buffer;
		size_t cur_size;
		size_t max_size;
	};

	class ReadEdge {
	  public:
		ReadEdge(std::vector<std::string>& file_list,
		         tbb::concurrent_queue<BufferSet<InEdge>*>* buffers,
		         size_t buffer_size) : in_files(file_list),
			buffer_pool(buffers),
			read_unit(buffer_size * INEDGE_SIZE),
			total_size(0),
			data_left(0) {
			it = in_files.begin();
		}
		~ReadEdge() { }
		void* operator()(tbb::flow_control& fc) const {
			size_t buffer_pos = 0;
			BufferSet<InEdge>* input;
			while (!buffer_pool->try_pop(input))
				std::this_thread::yield();
			while (it != in_files.end()) {
				if (data_left == 0) {
					in_file.OpenFile(it->c_str(), false, false);
					total_size = in_file.file_size();
					data_left = total_size;
				}
				while (data_left > 0 && buffer_pos < read_unit) {
					size_t to_read = std::min(read_unit - buffer_pos, data_left);
					in_file.Read(total_size - data_left, to_read,
					             reinterpret_cast<char*>(input->buffer)
					             + buffer_pos);
          data_left = data_left - to_read;
					buffer_pos += to_read;
				}
				if (data_left == 0) {
					++it;
					in_file.Close(true);
				}
				if (buffer_pos == read_unit) {
					input->cur_size = buffer_pos / INEDGE_SIZE;
					return input;
				}
			}
			if (buffer_pos != 0) {
				input->cur_size = buffer_pos / INEDGE_SIZE;
				return input;
			}
			delete input;
      fc.stop();
			return nullptr;
		}
	  private:
		std::vector<std::string> in_files;
		mutable std::vector<std::string>::iterator it;
		tbb::concurrent_queue<BufferSet<InEdge>*>* buffer_pool;
		mutable Turbo_bin_io_handler in_file;
		mutable size_t total_size;
		mutable size_t data_left;
		const size_t read_unit;
	};

	class SortEdge {
	  public:
		SortEdge() { }
		~SortEdge() { }
		void* operator()(void* edge_data) const {
			auto* data = static_cast<BufferSet<InEdge>*>(edge_data);
			tbb::parallel_sort(data->buffer, data->buffer + data->cur_size);
			return data;
		}
	};

	class WriteEdge {
	  public:
		WriteEdge(std::string write_path,
		          tbb::concurrent_queue<BufferSet<InEdge>*>* buffers) : 
      output_path(write_path),
			buffer_pool(buffers),
			block_count(0) { }
		~WriteEdge() { }
		void operator()(void* edge_data) const {
			auto* data = static_cast<BufferSet<InEdge>*>(edge_data);
			std::string out_filename = output_path + "/sorted_run_"
			                           + std::to_string(block_count++);
			Splittable_Turbo_bin_io_handler tempfile(out_filename.c_str(), true, true, true);
			tempfile.Append(data->cur_size * INEDGE_SIZE,
			                reinterpret_cast<char*>(data->buffer));
			tempfile.Close();
			buffer_pool->push(data);
		}

		size_t get_count() {
			return block_count;
		}
	  private:
		mutable size_t block_count;
		std::string output_path;
		tbb::concurrent_queue<BufferSet<InEdge>*>* buffer_pool;
	};
	const static int INEDGE_SIZE = sizeof(InEdge);
};
