#pragma once

#include <thread>
#include "md5.hpp"
#include "mpi.h"
#include "TypeDef.hpp"
#include "Turbo_bin_io_handler.hpp"
#include "tbb/concurrent_queue.h"
#include "tbb/parallel_pipeline.h"
#include "tbb/task_group.h"

struct RedistributeArgs {
	int machine_num;
	int machine_count;
	size_t buffer_count;
	size_t buffer_size;
    size_t recv_counts;
    size_t in_recv_counts;
    size_t* recv_count;
	std::string send_path;
	std::string recv_path;
    std::string prefix;
    std::string histogram_path;
	bool delete_original;
	bool validation_mode;
    bool redistribute_inedges;
};

template <typename InputNodeType>
class GraphRedistributor {
	typedef EdgePairEntry<InputNodeType> Edge;
  public:
	static RedistributeArgs redistribute(RedistributeArgs args, bool do_sort = false) {
		int pipes = (args.buffer_count + 1) / 2;
		std::vector<int> send_files;
		for (int i = 0; i < args.machine_count; i++)
			send_files.push_back((i + args.machine_num) % args.machine_count);

		tbb::concurrent_queue<NetBufferSet<Edge>*> buffer_queue;
		for (int i = 0; i < args.buffer_count; i++)
			buffer_queue.push(new NetBufferSet<Edge>(args.buffer_size));

    /*NetReadEdge<InputNodeType> input_f(send_files, args.send_path,
        &buffer_queue, args.buffer_size, args.delete_original, args.prefix);
    NetReverseEdge<InputNodeType> reverse_f;
    NetSendEdge<InputNodeType> send_f(&buffer_queue);
    NetRecvEdge<InputNodeType> recv_f(&buffer_queue,
        args.buffer_size, args.machine_count * args.machine_count);
    NetSortEdge<InputNodeType> sort_f;
		NetWriteEdge<InputNodeType> output_f(args.recv_path, &buffer_queue, args.machine_count, args.prefix);
		AddMD5<InputNodeType> add_md5_f;
		ProcessMD5<InputNodeType> proc_md5_f;*/

    tbb::filter<void, void*> input_f(tbb::filter_mode::serial_in_order,
        NetReadEdge(send_files, args.send_path, &buffer_queue,
          args.buffer_size, args.delete_original, args.prefix));
    tbb::filter<void*, void*> reverse_f(tbb::filter_mode::serial_out_of_order,
        NetReverseEdge());
    tbb::filter<void*, void> send_f(tbb::filter_mode::serial_in_order,
        NetSendEdge(&buffer_queue));
    tbb::filter<void, void*> recv_f(tbb::filter_mode::serial_out_of_order,
        NetRecvEdge(&buffer_queue, args.buffer_size, args.machine_count * args.machine_count));
    tbb::filter<void*, void*> sort_f(tbb::filter_mode::serial_out_of_order,
        NetSortEdge());
    tbb::filter<void*, void> output_f(tbb::filter_mode::parallel,
        NetWriteEdge(args.recv_path, &buffer_queue, args.machine_count, args.prefix));

    tbb::filter<void, void> send_pipeline_f;
    tbb::filter<void, void> recv_pipeline_f;
		//tbb::parallel_pipeline send;
		//tbb::parallel_pipeline recv;
		tbb::task_group run;

    if (args.prefix == "in_") {
      send_pipeline_f = input_f & reverse_f & send_f;
    } else {
      send_pipeline_f = input_f & send_f;
    }
    //send.add_filter(input_f);
    //if (args.prefix == "in_") send.add_filter(reverse_f);
    //if (args.validation_mode) send.add_filter(add_md5_f);
    //send.add_filter(send_f);

    if (do_sort) {
      recv_pipeline_f = recv_f & sort_f & output_f;
    } else {
      recv_pipeline_f = recv_f & output_f;
    }
    //recv.add_filter(recv_f);
    //if (args.validation_mode) recv.add_filter(proc_md5_f);
    //if (do_sort) recv.add_filter(sort_f);
    //recv.add_filter(output_f);

		run.run([&] { tbb::parallel_pipeline(pipes, send_pipeline_f); });
    tbb::parallel_pipeline(pipes, recv_pipeline_f);
		run.wait();

		//send.clear();
		//recv.clear();
    if (!(args.prefix == "in_")) {
      glob_t gl;
      int64_t recv_count = 0;
      if(glob((std::string(args.recv_path.c_str()) + "/sorted_run*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      if(glob((std::string(args.recv_path.c_str()) + "/recv*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      
      args.recv_counts = recv_count;
    } else {
      glob_t gl;
      int64_t recv_count = 0;
      if(glob((std::string(args.recv_path.c_str()) + "/in_sorted_run*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      if(glob((std::string(args.recv_path.c_str()) + "/in_recv*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      
      args.in_recv_counts = recv_count;
    }
    //if (!args.delete_original && args.redistribute_inedges)
    if (!(args.prefix == "" && args.redistribute_inedges)) {
      for (int i = 0; i < args.machine_count; i++) {
        for (int j = 0; j < args.machine_count; j++) {
          if (args.redistribute_inedges) {
            std::string link_file_name = args.send_path
              + "/in_send_" + std::to_string(j) + "_" + std::to_string(i);
            unlink(link_file_name.c_str());
          }
          std::string file_name = args.send_path
            + "/send_" + std::to_string(i) + "_" + std::to_string(j);
          remove(file_name.c_str());
        }
      }
    }
    NetBufferSet<Edge>* temp;
    while (buffer_queue.try_pop(temp))
      delete temp;
		return args;
	}

	static RedistributeArgs redistribute_and_build_histogram(RedistributeArgs args) {
    INVARIANT(false); // XXX disabled 2021.11.29
    GraphRedistributor::init_histogram();
		int pipes = (args.buffer_count + 1) / 2;
		std::vector<int> send_files;
		for (int i = 0; i < args.machine_count; i++)
			send_files.push_back((i + args.machine_num) % args.machine_count);

		tbb::concurrent_queue<NetBufferSet<Edge>*> buffer_queue;
		for (int i = 0; i < args.buffer_count; i++)
			buffer_queue.push(new NetBufferSet<Edge>(args.buffer_size));

		/*NetReadEdge<InputNodeType> input_f(send_files, args.send_path,
		                                   &buffer_queue, args.buffer_size, args.delete_original, args.prefix);
		NetSendEdge<InputNodeType> send_f(&buffer_queue);
		NetRecvEdge<InputNodeType> recv_f(&buffer_queue,
		                                  args.buffer_size, args.machine_count * args.machine_count);
		CalculateHistogram<InputNodeType> histogram_f;
    //NetSortEdge<InputNodeType> sort_f;
		NetWriteEdge<InputNodeType> output_f(args.recv_path, &buffer_queue, args.machine_count, args.prefix);
		//NetWriteEdgeDst<InputNodeType> output_f(args.recv_path, &buffer_queue, args.machine_count);
		AddMD5<InputNodeType> add_md5_f;
		ProcessMD5<InputNodeType> proc_md5_f;*/

		/*tbb::parallel_pipeline send;
		tbb::parallel_pipeline recv;
		tbb::task_group run;
		send.add_filter(input_f);
		if (args.validation_mode)
			send.add_filter(add_md5_f);
		send.add_filter(send_f);
		recv.add_filter(recv_f);
		if (args.validation_mode)
			recv.add_filter(proc_md5_f);
        recv.add_filter(histogram_f);
        //recv.add_filter(sort_f);
		recv.add_filter(output_f);

		run.run([&] { send.run(pipes); });
		recv.run(pipes);
		run.wait();

		send.clear();
		recv.clear();
		args.recv_counts = output_f.get_count();
        histogram_f.accumulate_histogram();
        if (!(args.prefix == "" && args.redistribute_inedges)) {
			for (int i = 0; i < args.machine_count; i++) {
                for (int j = 0; j < args.machine_count; j++) {
                    if (args.redistribute_inedges) {
                        std::string link_file_name = args.send_path
                            + "/in_send_" + std::to_string(j) + "_" + std::to_string(i);
                        unlink(link_file_name.c_str());
                    }
                    std::string file_name = args.send_path
                        + "/send_" + std::to_string(i) + "_" + std::to_string(j);
                    remove(file_name.c_str());
                }
			}
        }
		NetBufferSet<Edge>* temp;
		while (buffer_queue.try_pop(temp))
			delete temp;
        GraphRedistributor::print_histogram(args.histogram_path);*/
		return args;
	}
	
  static RedistributeArgs redistribute_inedges(RedistributeArgs args) {
    //GraphRedistributor::init_histogram();
    int pipes = (args.buffer_count + 1) / 2;
    std::vector<int> send_files;
    for (int i = 0; i < args.machine_count; i++)
      send_files.push_back((i + args.machine_num) % args.machine_count);

    tbb::concurrent_queue<NetBufferSet<Edge>*> buffer_queue;
    for (int i = 0; i < args.buffer_count; i++)
      buffer_queue.push(new NetBufferSet<Edge>(args.buffer_size));

		/*NetReadEdge<InputNodeType> input_f(send_files, args.send_path,
		                                   &buffer_queue, args.buffer_size, args.delete_original, args.prefix);
    NetReverseEdge<InputNodeType> reverse_f;
		NetSendEdge<InputNodeType> send_f(&buffer_queue);
		NetRecvEdge<InputNodeType> recv_f(&buffer_queue,
		                                  args.buffer_size, args.machine_count * args.machine_count);
		//CalculateHistogram<InputNodeType> histogram_f;
    NetSortEdge<InputNodeType> sort_f;
    NetWriteEdge<InputNodeType> output_f(args.recv_path, &buffer_queue, args.machine_count, args.prefix);
    AddMD5<InputNodeType> add_md5_f;
    ProcessMD5<InputNodeType> proc_md5_f;*/
    
    tbb::filter<void, void*> input_f(tbb::filter_mode::serial_in_order,
        NetReadEdge(send_files, args.send_path, &buffer_queue,
          args.buffer_size, args.delete_original, args.prefix));
    tbb::filter<void*, void*> reverse_f(tbb::filter_mode::serial_out_of_order,
        NetReverseEdge());
    tbb::filter<void*, void> send_f(tbb::filter_mode::serial_in_order,
        NetSendEdge(&buffer_queue));
    tbb::filter<void, void*> recv_f(tbb::filter_mode::serial_out_of_order,
        NetRecvEdge(&buffer_queue, args.buffer_size, args.machine_count * args.machine_count));
    tbb::filter<void*, void*> sort_f(tbb::filter_mode::serial_out_of_order,
        NetSortEdge());
    tbb::filter<void*, void> output_f(tbb::filter_mode::parallel,
        NetWriteEdge(args.recv_path, &buffer_queue, args.machine_count, args.prefix));

    tbb::filter<void, void> send_pipeline_f;
    tbb::filter<void, void> recv_pipeline_f;

    //tbb::parallel_pipeline send;
    //tbb::parallel_pipeline recv;
    tbb::task_group run;

    //send.add_filter(input_f);
    //send.add_filter(reverse_f);
    //if (args.validation_mode) send.add_filter(add_md5_f);
    //send.add_filter(send_f);
    send_pipeline_f = input_f & reverse_f & send_f;

    //recv.add_filter(recv_f);
    //if (args.validation_mode) recv.add_filter(proc_md5_f);
    ////recv.add_filter(histogram_f);
    //recv.add_filter(sort_f);
    //recv.add_filter(output_f);
    recv_pipeline_f = recv_f & sort_f & output_f;

    //run.run([&] { send.run(pipes); });
    //recv.run(pipes);
		run.run([&] { tbb::parallel_pipeline(pipes, send_pipeline_f); });
    tbb::parallel_pipeline(pipes, recv_pipeline_f);
    run.wait();

    //send.clear();
    //recv.clear();
    if (!(args.prefix == "in_")) {
      glob_t gl;
      int64_t recv_count = 0;
      if(glob((std::string(args.recv_path.c_str()) + "/sorted_run*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      if(glob((std::string(args.recv_path.c_str()) + "/recv*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      
      args.recv_counts = recv_count;
    } else {
      glob_t gl;
      int64_t recv_count = 0;
      if(glob((std::string(args.recv_path.c_str()) + "/in_sorted_run*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      if(glob((std::string(args.recv_path.c_str()) + "/in_recv*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
        recv_count += gl.gl_pathc;
      
      args.in_recv_counts = recv_count;
    }
    //histogram_f.accumulate_histogram();
    if (!args.delete_original)
      for (int i = 0; i < args.machine_count; i++) {
        for (int j = 0; j < args.machine_count; j++) {
          std::string file_name = args.send_path
            + "/send_" + std::to_string(i) + "_" + std::to_string(j);
          remove(file_name.c_str());
        }
      }
    NetBufferSet<Edge>* temp;
    while (buffer_queue.try_pop(temp))
      delete temp;
    //GraphRedistributor::print_histogram();
		return args;
	}
	
    static void init_histogram() {
		int64_t total_bin_num = (PartitionStatistics::my_num_internal_nodes() + HISTOGRAM_BIN_WIDTH - 1) / HISTOGRAM_BIN_WIDTH;
		std::vector<std::vector<int64_t>>& histogram = GraphPreprocessor<node_t, degree_t, file_t>::getHistogram();
		histogram.resize(PartitionStatistics::num_machines());
		for (int i = 0; i < PartitionStatistics::num_machines(); i++) {
			histogram[i].resize(total_bin_num, 0);
		}
	}
    
    static void print_histogram(std::string histogram_path) {
        // Create Directory first
        mode_t old_umask;
        old_umask = umask(0);
        int is_error = mkdir((histogram_path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
        if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
            fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (histogram_path).c_str(), errno);
            umask(old_umask);
            return;
        }
        umask(old_umask);

		std::vector<std::vector<int64_t>>& histogram = GraphPreprocessor<node_t, degree_t, file_t>::getHistogram();
		for (int i = 0; i < PartitionStatistics::num_machines(); i++) {
            std::string filename = histogram_path + std::to_string(i) + ".txt";
            std::ofstream of_(filename.c_str());
            of_ << histogram[i].size() << "\t" << HISTOGRAM_BIN_WIDTH << "\n";
            for (int64_t j = 0; j < histogram[i].size(); j++) { 
                of_ << histogram[i][j] << "\n";
            }
            of_.close();
		}
	}

  private:
	template <typename T>
	struct NetBufferSet {
		NetBufferSet(size_t size) : max_size(size),
			cur_size(0),
			end_of_input(false),
			buffer(new T[size + 1 + MD5_SIZE]) { }
		~NetBufferSet() {
			delete[] buffer;
		}
		T* buffer;
        size_t dst;
		size_t target;
		size_t cur_size;
		size_t max_size;
		bool end_of_input;
		const static int MD5_SIZE = (MD5::HashBytes - 1 + sizeof(T)) / sizeof(T);
	};

	class NetReadEdge {
	  public:
		NetReadEdge(std::vector<int>& file_list,
		            std::string send_path,
		            tbb::concurrent_queue<NetBufferSet<Edge>*>* buffers,
		            size_t buffer_size, bool delete_original_, std::string prefix_) : in_files(file_list),
			input_path(send_path),
			buffer_pool(buffers),
			read_unit(buffer_size * EDGE_SIZE),
			file_size(0),
			data_left(0),
      partitioned_file_idx(0),
      delete_original(delete_original_),
      prefix(prefix_) {
			it = in_files.begin();
		}
		~NetReadEdge() { }
		void* operator()(tbb::flow_control& fc) const {
			size_t buffer_pos = 0;
			NetBufferSet<Edge>* input;
			while (it != in_files.end()) {
				while (!buffer_pool->try_pop(input))
					std::this_thread::yield();
				input->target = *it;
				if (data_left == 0) {
					std::string file_name = input_path
					                        + "/" + prefix + "send_" + std::to_string(*it) + "_" + std::to_string(partitioned_file_idx);
					in_file.OpenFile(file_name.c_str(), false, false);
					file_size = in_file.file_size();
					data_left = file_size;
				}
				while (data_left > 0 && buffer_pos < read_unit) {
					size_t to_read = std::min(read_unit - buffer_pos, data_left);
          in_file.Read(file_size - data_left, to_read,
              reinterpret_cast<char*>(input->buffer)
              + buffer_pos);
					data_left -= to_read;
					buffer_pos += to_read;
				}
				if (data_left == 0) {
					in_file.Close(delete_original);
					input->cur_size = buffer_pos / EDGE_SIZE;
          input->end_of_input = true;
          input->buffer[input->cur_size].src_vid_ = static_cast<InputNodeType>(partitioned_file_idx);
          if (++partitioned_file_idx == in_files.size()) {
            partitioned_file_idx = 0;
            it++;
          }
					return input;
				}
				if (buffer_pos == read_unit) {
					input->cur_size = buffer_pos / EDGE_SIZE;
					input->end_of_input = false;
          input->buffer[input->cur_size].src_vid_ = static_cast<InputNodeType>(partitioned_file_idx);
					return input;
				}
			}
			if (buffer_pos != 0) {
				input->cur_size = buffer_pos / EDGE_SIZE;
				input->end_of_input = true;
        input->buffer[input->cur_size].src_vid_ = static_cast<InputNodeType>(partitioned_file_idx);
				return input;
			}
      fc.stop();
			return nullptr;
		}
	  private:
		std::vector<int> in_files;
		mutable std::vector<int>::iterator it;
		std::string input_path;
    std::string prefix;
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		mutable Turbo_bin_io_handler in_file;
		mutable size_t file_size;
		mutable size_t data_left;
		const size_t read_unit;
		bool delete_original;
    mutable int partitioned_file_idx;
	};

	/*template <typename InputNodeType>
	class NetReadInEdge : public tbb::filter {
	  public:
		NetReadInEdge(std::vector<int>& file_list,
		            std::string send_path,
		            tbb::concurrent_queue<NetBufferSet<Edge>*>* buffers,
		            size_t buffer_size, bool delete_original_) : tbb::filter(serial_in_order),
			in_files(file_list),
			input_path(send_path),
			buffer_pool(buffers),
			read_unit(buffer_size * EDGE_SIZE),
			file_size(0),
			data_left(0),
            partitioned_file_idx(0),
			delete_original(delete_original_) {
			it = in_files.begin();
		}
		~NetReadInEdge() { }
		void* operator()(void*) {
			size_t buffer_pos = 0;
			NetBufferSet<Edge>* input;
			while (it != in_files.end()) {
				while (!buffer_pool->try_pop(input))
					std::this_thread::yield();
				input->target = *it;
				if (data_left == 0) {
					std::string file_name = input_path
					                        + "/send_" + std::to_string(partitioned_file_idx) + "_" + std::to_string(*it);
					in_file.OpenFile(file_name.c_str(), false, false);
					file_size = in_file.file_size();
					data_left = file_size;
				}
				while (data_left > 0 && buffer_pos < read_unit) {
					size_t to_read = std::min(read_unit - buffer_pos, data_left);
					in_file.Read(file_size - data_left, to_read,
					             reinterpret_cast<char*>(input->buffer)
					             + buffer_pos);
					data_left -= to_read;
					buffer_pos += to_read;
				}
				if (data_left == 0) {
					in_file.Close(delete_original);
					input->cur_size = buffer_pos / EDGE_SIZE;
					input->end_of_input = true;
                    input->buffer[input->cur_size].src_vid_ = static_cast<InputNodeType>(partitioned_file_idx);
                    if (++partitioned_file_idx == in_files.size()) {
                        partitioned_file_idx = 0;
                        it++;
                    }
					return input;
				}
				if (buffer_pos == read_unit) {
					input->cur_size = buffer_pos / EDGE_SIZE;
					input->end_of_input = false;
                    input->buffer[input->cur_size].src_vid_ = static_cast<InputNodeType>(partitioned_file_idx);
					return input;
				}
			}
			if (buffer_pos != 0) {
				input->cur_size = buffer_pos / EDGE_SIZE;
				input->end_of_input = true;
                input->buffer[input->cur_size].src_vid_ = static_cast<InputNodeType>(partitioned_file_idx);
				return input;
			}
			return nullptr;
		}
	  private:
		std::vector<int> in_files;
		std::vector<int>::iterator it;
		std::string input_path;
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		Turbo_bin_io_handler in_file;
		size_t file_size;
		size_t data_left;
		const size_t read_unit;
		bool delete_original;
        int partitioned_file_idx;
	};*/
    
	class NetReverseEdge {
	  public:
		NetReverseEdge() { }
		~NetReverseEdge() { }
		void* operator()(void* edge_data) const {
      auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
      for (int64_t idx = 0; idx < data->cur_size; idx++) {
        std::swap(data->buffer[idx].src_vid_, data->buffer[idx].dst_vid_);
      }
			return data;
		}
	};

	class NetSendEdge {
	  public:
		NetSendEdge(tbb::concurrent_queue<
		            NetBufferSet<Edge>*>* buffers) : buffer_pool(buffers),
			EOS(~((InputNodeType)(int64_t)0)) { }
		~NetSendEdge() { }
		void operator()(void* edge_data) const {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
			if (data->cur_size != 0)
				MPI_Send(data->buffer, (data->cur_size + 1) * EDGE_SIZE,
				         MPI_BYTE, data->target, 0, MPI_COMM_WORLD);
			if (data->end_of_input)
				MPI_Send(&EOS, sizeof(InputNodeType),
				         MPI_BYTE, data->target, 0, MPI_COMM_WORLD);
			buffer_pool->push(data);
		}
	  private:
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		const InputNodeType EOS;
	};

	class NetRecvEdge {
	  public:
		NetRecvEdge(tbb::concurrent_queue<NetBufferSet<Edge>*>* buffers,
		            size_t size, size_t machines) : buffer_pool(buffers),
			buffer_size(size),
			recv_remaining(machines),
			EOS(~((InputNodeType)(int64_t)0)) {
			buffer_size += NetBufferSet<Edge>::MD5_SIZE;
		}
		~NetRecvEdge() { }
		void* operator()(tbb::flow_control& fc) const {
			NetBufferSet<Edge>* input;
			while (!buffer_pool->try_pop(input))
				std::this_thread::yield();
			while (recv_remaining) {
				MPI_Status stat;
				int recv_size;
				MPI_Recv(input->buffer, (buffer_size + 1) * EDGE_SIZE,
				         MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
				MPI_Get_count(&stat, MPI_BYTE, &recv_size);
				if (recv_size == sizeof(InputNodeType) &&
				        EOS == reinterpret_cast<InputNodeType*>(input->buffer)[0]) {
					recv_remaining--;
					continue;
				}
				input->cur_size = (recv_size - sizeof(Edge)) / EDGE_SIZE;
        input->dst = static_cast<size_t>(input->buffer[input->cur_size].src_vid_);
        INVARIANT(input->dst >= 0 && input->dst < PartitionStatistics::num_machines());
				return input;
			}
      fc.stop();
			return nullptr;
		}
	  private:
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		mutable size_t recv_remaining;
		size_t buffer_size;
		const InputNodeType EOS;
	};
	
    /*template <typename InputNodeType>
    class CalculateHistogram : public tbb::filter {
		public:
		CalculateHistogram() : tbb::filter(serial_out_of_order), 
								local_histogram(GraphPreprocessor<node_t, degree_t, file_t>::getHistogram()) {
			total_bin_size = local_histogram[0].size();
			bin_width = HISTOGRAM_BIN_WIDTH;
			temp_num_threads = 4;

			per_thread_local_histogram.resize(temp_num_threads);
			for(int i = 0; i < temp_num_threads; i++) {
				per_thread_local_histogram[i].resize(PartitionStatistics::num_machines());
				for (int j = 0; j < PartitionStatistics::num_machines(); j++) {
					per_thread_local_histogram[i][j].resize(total_bin_size, 0);
				}
			}
		}

		~CalculateHistogram() { }

		void* operator()(void* edge_data) {	
            auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
			//XXX use openmp?
#pragma omp parallel num_threads(temp_num_threads)
			{
				int64_t machine_num;
				int64_t bin_num;
				int tid = omp_get_thread_num();
#pragma omp for
				for(int64_t i = 0; i < data->cur_size; i++) {
					InputNodeType dst_vid = data->buffer[i].dst_vid_;
					ALWAYS_ASSERT(dst_vid >= 0 && dst_vid < PartitionStatistics::my_num_internal_nodes() * PartitionStatistics::num_machines());
					machine_num = ((int64_t)dst_vid) / PartitionStatistics::my_num_internal_nodes();
					bin_num = (dst_vid - PartitionStatistics::my_num_internal_nodes() * machine_num) / bin_width;
					if(machine_num > PartitionStatistics::num_machines()) {
						fprintf(stdout, "[%lld] machine_num = %lld, dst_vid = %lld, internal_nodes = %lld, i = %lld, data->cur_size = %lld\n", PartitionStatistics::my_machine_id(), machine_num, (int64_t)dst_vid, PartitionStatistics::my_num_internal_nodes(), i, data->cur_size);
					}
					ALWAYS_ASSERT(machine_num < PartitionStatistics::num_machines() && machine_num >= 0);
					ALWAYS_ASSERT(bin_num < total_bin_size && bin_num >= 0);
					per_thread_local_histogram[tid][machine_num][bin_num]++;
				}
			}
			return edge_data;
		}

        void accumulate_histogram() {
			for(int i = 0; i < temp_num_threads; i++)
				for(int j = 0; j < PartitionStatistics::num_machines(); j++)
					for (int k = 0; k < total_bin_size; k++)
						local_histogram[j][k] += per_thread_local_histogram[i][j][k];
        }

		private:
		std::vector<std::vector<int64_t>>& local_histogram;
		std::vector<std::vector<std::vector<int64_t>>> per_thread_local_histogram;
		int64_t total_bin_size;
		int64_t bin_width;
		int temp_num_threads;
	};*/
	
	class NetSortEdge {
	  public:
		NetSortEdge() { }
		~NetSortEdge() { }
		void* operator()(void* edge_data) const {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
            std::sort(data->buffer, data->buffer + data->cur_size,
                    [] (const Edge &ledge, const Edge &redge){
                        return (ledge.src_vid_ < redge.src_vid_ ||
                                ((ledge.src_vid_ == redge.src_vid_) && (ledge.dst_vid_ < redge.dst_vid_)));
                });
            //tbb::parallel_sort(data->buffer, data->buffer + data->cur_size);
			return data;
		}
	};

	class NetWriteEdge {
	  public:
		NetWriteEdge(std::string write_path,
		             tbb::concurrent_queue<
		             NetBufferSet<Edge>*>* buffers,
                 size_t machines, std::string prefix_) : buffer_pool(buffers),
			output_path(write_path),
      block_count(0),
      prefix(prefix_) { }
		~NetWriteEdge() { }
		void* operator()(void* edge_data) const {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
      //auto dst = data->dst;

      // For InEdge
      //std::string out_filename = output_path
      //                           + "/" + prefix + "sorted_run_"
      //                           + std::to_string(std::atomic_fetch_add((std::atomic<size_t>*)&block_count, 1UL));
			//Splittable_Turbo_bin_io_handler tempfile(out_filename.c_str(), true, true, true);
      // For Original BBP
			std::string out_filename = output_path
			                           + "/" + prefix + "recv_"
			                           + std::to_string(std::atomic_fetch_add((std::atomic<size_t>*)&block_count, 1UL));
			Turbo_bin_io_handler tempfile(out_filename.c_str(), true, true, true);
			tempfile.Append(data->cur_size * EDGE_SIZE,
			                reinterpret_cast<char*>(data->buffer));
			tempfile.Close();
			buffer_pool->push(data);
			return nullptr;
		}

    size_t get_count() {
			return block_count;
		}
	  private:
    mutable size_t block_count;
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		std::string output_path;
		std::string prefix;
	};
	
  /*  template <typename InputNodeType>
	class NetWriteEdgeSorted : public tbb::filter {
	  public:
		NetWriteEdgeSorted(std::string write_path,
		             tbb::concurrent_queue<
		             NetBufferSet<Edge>*>* buffers,
                     size_t machines, std::string prefix_) : tbb::filter(parallel),
			buffer_pool(buffers),
			output_path(write_path),
            block_count(0),
            prefix(prefix_) { }
		~NetWriteEdgeSorted() { }
		void* operator()(void* edge_data) {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
            //auto dst = data->dst;
			std::string out_filename = output_path
			                           + "/" + prefix + "sorted_run_"
			                           + std::to_string(std::atomic_fetch_add((std::atomic<size_t>*)&block_count, 1UL));
			Splittable_Turbo_bin_io_handler tempfile(out_filename.c_str(), true, true, true);
			//std::string out_filename = output_path
			//                           + "/" + prefix + "recv_"
			//                           + std::to_string(std::atomic_fetch_add((std::atomic<size_t>*)&block_count, 1UL));
			//Turbo_bin_io_handler tempfile(out_filename.c_str(), true, true, true);
			tempfile.Append(data->cur_size * EDGE_SIZE,
			                reinterpret_cast<char*>(data->buffer));
			tempfile.Close();
			buffer_pool->push(data);
			return nullptr;
		}

        size_t get_count() {
			return block_count;
		}
	  private:
        size_t block_count;
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		std::string output_path;
		std::string prefix;
	};*/
	
    /*template <typename InputNodeType>
	class NetWriteEdgeDst : public tbb::filter {
	  public:
		NetWriteEdgeDst(std::string write_path,
		             tbb::concurrent_queue<
		             NetBufferSet<Edge>*>* buffers,
                     size_t machines) : tbb::filter(parallel),
			buffer_pool(buffers),
			output_path(write_path),
            block_count(new size_t[machines]){ 
                for (int i = 0; i < machines; i++) block_count[i] = 0;
            }
		~NetWriteEdgeDst() { }
		void* operator()(void* edge_data) {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
            auto dst = data->dst;
			std::string out_filename = output_path
			                           + "/sorted_run_" + std::to_string(dst) + "-"
			                           + std::to_string(std::atomic_fetch_add((std::atomic<size_t>*)&block_count[dst], 1UL));
			Splittable_Turbo_bin_io_handler tempfile(out_filename.c_str(), true, true, true);
			tempfile.Append(data->cur_size * EDGE_SIZE,
			                reinterpret_cast<char*>(data->buffer));
			tempfile.Close();
			buffer_pool->push(data);
			return nullptr;
		}

        size_t* get_count() {
			return block_count;
		}
	  private:
        size_t* block_count;
		tbb::concurrent_queue<NetBufferSet<Edge>*>* buffer_pool;
		std::string output_path;
	};*/

	/*template <typename InputNodeType>
	class AddMD5 : public tbb::filter {
	  public:
		AddMD5() : tbb::filter(parallel) { }
		~AddMD5() { }
		void* operator()(void* edge_data) {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
			MD5 md5;
			md5.add(data->buffer, data->cur_size * EDGE_SIZE);
			md5.getHash(reinterpret_cast<unsigned char*>(data->buffer + data->cur_size));
			data->cur_size += NetBufferSet<Edge>::MD5_SIZE;
			return data;
		}
	};*/

	/*template <typename InputNodeType>
	class ProcessMD5 : public tbb::filter {
	  public:
		ProcessMD5() : tbb::filter(parallel) { }
		~ProcessMD5() { }
		void* operator()(void* edge_data) {
			auto* data = static_cast<NetBufferSet<Edge>*>(edge_data);
			unsigned char expected[MD5::HashBytes];
			data->cur_size -= NetBufferSet<Edge>::MD5_SIZE;
			MD5 md5;
			md5.add(data->buffer, data->cur_size * EDGE_SIZE);
			md5.getHash(expected);
			for (auto i = 0; i < MD5::HashBytes; i++)
				if (expected[i]
				        != ((unsigned char*)(data->buffer + data->cur_size))[i]) {
					std::cout << "ERROR: MD5 does not match" << std::endl;
					break;
				}
			return data;
		}
	};*/

	const static int EDGE_SIZE = sizeof(Edge);
};
