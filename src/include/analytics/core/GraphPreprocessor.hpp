#pragma once

/*
 * Design of the GraphPreprocessor
 *
 * This class implements Balanced Buffer-aware Partitioning (BBP). To answer
 * the question, "What are the most important partitioning objectives for 
 * improving overall processing performance?", our objectives of partitioning
 * are threefold. 
 *     1) We balance the workloads across machines to maximize the overall 
 *        efficiency of graph processing. In order to avoid imbalanced 
 *        workloads, we need to balance the number of edges as well as the 
 *        number of high-degree and low-degree vertices among machines.
 *     2) We also balance the number of vertices across machines, so that 
 *        each machine requires the same memory budget for ensuring in-memory 
 *        access to the vertices.
 *     3) We use range partitioning rather than hash partitioning, where the 
 *        vertex IDs assigned to each machine are consecutively allocated. 
 *        This way, we can access the state of any vertex by using cheap 
 *        offset calculation.
 *
 * In order to achieve out objectives, BBP first sorts the vertices by their 
 * degrees and distributes them across machines in a round-robin manner to
 * balance the number of edges as well as the number of high-degree and 
 * low-degree vertices among machines. In total, we have p-by-1 partitions, 
 * where p is the number of machines. Then, BBP renumbers vertex IDs to 
 * (1) assign consecutive vertex IDs to the vertices in each machine and 
 * (2) exploit the degree order information as partial order constraints 
 * that can significantly accelerate the set intersection computation. For 
 * vertices in each machine, new vertex IDs are assigned in descending order 
 * of their degrees. In this way, we allocate lower vertex ID for the vertices 
 * with higher degrees within each machine.
 *
 * After renumbering, BBP further partitions the edges in each machine by the 
 * equally sized range of source and target vertex ID. The source vertices are 
 * range-partitioned into q ranges, and the destination vertices are 
 * range-partitioned into p × q ranges, where q is determined by the available 
 * memory budget in a machine for processing neighborhood-centric analytics.
 * Each machine has q-by-pq edge chunks.
 *
 * Additionally, in order to reduce synchronization costs across NUMA nodes due 
 * to concurrent updates by Compare-And-Swap (CAS) for the in-memory local gather 
 * operations, we further range partition each edge chunk by the destination 
 * vertices into r edge sub-chunks where r is the number of NUMA nodes. We balance
 * the number of edges among the sub-chunks by exploiting the degree information 
 * of vertices.
 *
 * When the graph preprocessing process implemented in this class is finished, 
 * p-by-1 partitions and degree histograms are generated as final results. 
 * After that, q-by-pqr edge chunks are generated through the DB building
 * process. The graph preprocessing process consists of the following steps:
 *     1) Count the degree of each vertex
 *     2) According to the degree order, assign a new vertex ID to each vertex
 *     3) Redistribute edges according to range partition
 *     4) Sort the edges on each machine
 */

#include <iostream>
#include <algorithm>
#include <functional>
#include <atomic>
#include <queue>
#include <thread>
#include <string>
#include <chrono>
#include <sys/statvfs.h>

#include "analytics/core/TypeDef.hpp"
#include "analytics/core/turbo_dist_internal.hpp"
#include "analytics/core/RequestRespond.hpp"
#include "analytics/core/GraphRenumberer.hpp"
#include "analytics/datastructure/FixedSizeBufferPool.hpp"
#include "analytics/io/EdgePairListReader.hpp"
#include "analytics/io/PartitionedEdges.hpp"
#include "analytics/io/Blocking_Turbo_bin_io_handler.hpp"
#include "analytics/io/DummyDataReader.hpp"
#include "analytics/io/Splittable_Turbo_bin_io_handler.hpp"
#include "analytics/util/Aio_Helper.hpp"
#include "analytics/util/GraphPreprocessorConfiguration.hpp"
#include "analytics/util/ExternalSort.hpp"
#include "analytics/util/GraphRedistributor.hpp"
#include "storage/cache/disk_aio/eXDB_dist_internal.hpp"

#define ALIGN_EXPAND(x, n) ((x) = (x) / (n) * (n - 1) + (n))
#define TIMEDIFF_MILLISEC(begin, end) ((double)std::chrono::duration_cast<std::chrono::milliseconds>((end) - (begin)).count())

#define VID_TO_PARTITIONID(vid, num_chunks, total_vertices) ((vid * num_chunks)/ total_vertices)

bool is_number(const std::string& s) {
	std::string::const_iterator it = s.begin();
	while (it != s.end() && std::isdigit(*it)) ++it;
	return !s.empty() && it == s.end();
}

template <typename InputNodeType, typename Degree_t, typename FileNodeType>
class GraphPreprocessor {
	typedef void (GraphPreprocessor::*vFunctionCallback)(void);
	typedef EdgePairEntry<InputNodeType> InputEdge;
	typedef EdgePairEntry<FileNodeType> FileEdge;
	typedef Degree_t degree_t;
	public:
	GraphPreprocessor() : max_degree_(0), cut_degree_(0), timer_id(0) {}
	~GraphPreprocessor() {
		finalize();
	}

	void run(int argc, char** argv) {
		try {
			run_(argc, argv);
		}
		catch (std::exception& e) {
			fprintf(stdout, "[Error in Machine %lld] %s\n", args_.machine_num, e.what());
			StackTracer::GetStackTracer().print_call_stack();
			_exit(1);
		}
	}
    static std::vector<std::vector<int64_t>> local_histogram;
    static std::vector<std::vector<int64_t>>& getHistogram() {
        return local_histogram;
    }
	static std::vector<std::vector<int64_t>> SubchunkVidRangePerEdgeChunk;
	static std::vector<std::vector<int64_t>>& getSubchunkVidRange() {
		return SubchunkVidRangePerEdgeChunk;
	}


	private:
	GraphPreprocessArgs args_;
	RedistributeArgs redist_args;
	ExternalSortArgs sort_args;

	MPI_Datatype mpi_type_;
	InputNodeType max_degree_;
	InputNodeType cut_degree_;
	std::size_t edge_count_;
	int64_t num_chunks_;
	int64_t fake_vertex;
	int64_t edge_cnt;
	std::vector<int64_t> range;
	int64_t buffer_num_;

	Splittable_Turbo_bin_io_handler* degree_table_;
	Turbo_bin_io_handler* oldtonew_vid_table_;

	PartitionedEdges<FileNodeType, degree_t>* edge_partitioned_;
	std::string temp_output_path_;
	std::string temp_original_path_;
	std::vector<std::string> edgefile_list;
	FixedSizeConcurrentBufferPool* concurrent_buffer_pool;


    private:
	void run_(int argc, char** argv) {
		init(argc, argv);
		double total_time = 0.0;

        system_fprintf(0, stdout, "Preprocessing START\n");
        system_fprintf(0, stdout, "Waiting for all machines...\n");

		total_time += timewatch_wrapper("Preparation",
				&GraphPreprocessor::init_edge_list_and_reader);

		switch(args_.phase_from) {
			case 0 :
				total_time += timewatch_wrapper("Phase 0",
						&GraphPreprocessor::create_undirected);
				if(args_.phase_to == 0) break;
			case 1 :
				total_time += timewatch_wrapper("Phase 1",
						&GraphPreprocessor::create_edge_colony);
				if(args_.phase_to == 1) break;
			case 2 :
				total_time += timewatch_wrapper("Phase 2",
						&GraphPreprocessor::calc_degree_table);
				if(args_.phase_to == 2) break;
			case 3 :
				total_time += timewatch_wrapper("Phase 3",
						&GraphPreprocessor::calc_new_vid_table);
				if(args_.phase_to == 3) break;
			case 4 :
				total_time += timewatch_wrapper("Phase 4",
						&GraphPreprocessor::assign_new_vid_hashjoin);
				if(args_.phase_to == 4) break;
			case 5 :
				total_time += timewatch_wrapper("Phase 5",
						&GraphPreprocessor::rearrange_edge_distribution);
				if(args_.phase_to == 5) break;
			case 6 :
				total_time += timewatch_wrapper("Phase 6",
						&GraphPreprocessor::redistribute_edges);
				if(args_.phase_to == 6) break;
			case 7 :
				total_time += timewatch_wrapper("Phase 7-1",
						&GraphPreprocessor::create_sorted_run);
				if(args_.phase_to == 7) break;
			case 8 :
				total_time += timewatch_wrapper("Phase 7-2",
						&GraphPreprocessor::merge_sorted_runs);
				if (args_.sort_validation)
					total_time += timewatch_wrapper("Phase 7-3",
							&GraphPreprocessor::validate_external_sort);
				if(args_.phase_to == 8) break;
		}

        system_fprintf(0, stdout, "Preprocessing END\n");
        system_fprintf(0, stdout, "Total Elapsed time: %.3lf\n", total_time / 1000);
	}

	void init(int argc, char** argv) {
		//Initialize MPI
		MPI::Init_thread(argc, argv, MPI_THREAD_MULTIPLE);
		MPI_Comm_rank(MPI_COMM_WORLD, &(args_.machine_num));
		MPI_Comm_size(MPI_COMM_WORLD, &(args_.machine_count));
		PartitionStatistics::init();

		//Read Configuration file
		GraphPreprocessConfigurationReader conf_reader;
		conf_reader.read_configuration(argc, argv, args_);
		
        if(args_.partitioning_scheme > 4 || args_.partitioning_scheme < 0) {
			fprintf(stdout, "[%lld] Partitioning Scheme Mode ID must be a value in [0,4]\n", args_.machine_num);
			throw std::runtime_error("[GraphPreprocessor::run] Invalid partitioning scheme\n");
		} else {
			if(args_.machine_num == 0) {
                switch(args_.partitioning_scheme) {
                    case 0: fprintf(stdout, "Partitioning scheme = BBP\n"); break;
                    case 1: fprintf(stdout, "Partitioning scheme = Partitioning using a given range file\n"); break;
                    case 2: fprintf(stdout, "Partitioning scheme = Random partitioning\n"); break;
                    case 3: fprintf(stdout, "Partitioning scheme = Hash partitioning\n"); break;
                    case 4: fprintf(stdout, "Partitioning scheme = GraphX Hash partitioning\n"); break;
                }
			}
		}

		UserArguments::BUILD_DB = args_.build_db;
		args_.buffer_size = sizeof(FileEdge) * 1024 * 1024L; //XXX

		mpi_type_ = PrimitiveType2MPIType<Degree_t>(0 /*Dummy*/);

		omp_set_num_threads(args_.num_threads);
		core_id::set_core_ids(args_.num_threads);

		//TODO - original = (8, 11), (8, 11)
		Aio_Helper::Initialize(DEFAULT_NIO_THREADS, 8, 15);

		//init num_chunks_ 
        //TODO remove heuristic
		int64_t total_file_size = args_.edge_num * sizeof(InputEdge) / args_.machine_count; //# of edges * sizeof(edge) / # of machine (GB)
		int64_t total_vertices_size = args_.max_vid * sizeof(node_t); //# of vertices * sizeof(vertex) (GB)
		int64_t total_mem_size = args_.mem_size;
		int64_t mem_size = args_.mem_size - (3456 * 1024 * 1024L); //TODO hard coding
		ALWAYS_ASSERT(mem_size >= 0);
		num_chunks_ = ceil((double)(args_.max_vid * (4 * sizeof(node_t)))/(mem_size)); 		
        num_chunks_ = args_.machine_count * ceil((double)num_chunks_ / args_.machine_count);
		num_chunks_ = args_.machine_count;

        //TODO remove heuristic
		int64_t buffer_num = total_mem_size / (args_.buffer_size * 5);
		while(buffer_num < (num_chunks_ * num_chunks_) * args_.num_threads) {
			args_.buffer_size = args_.buffer_size / 2;
			buffer_num = total_mem_size / (args_.buffer_size * 5);
		}
		while(buffer_num < args_.machine_count * args_.num_threads) {
			args_.buffer_size = args_.buffer_size / 2;
			buffer_num = buffer_num * 2;
		}

		ALWAYS_ASSERT(args_.buffer_size >= sizeof(FileEdge));
		buffer_num_ = buffer_num;
		concurrent_buffer_pool = new FixedSizeConcurrentBufferPool();
		concurrent_buffer_pool->Initialize(args_.buffer_size * buffer_num, args_.buffer_size);
		LocalStatistics::register_mem_alloc_info("ConcurrentBufferPool", args_.buffer_size * buffer_num / (1024 * 1024));

		system_fprintf(0, stdout, "Num chunks : %lld, Buffer pool num : %lld, Buffer size : %lld\n",num_chunks_, buffer_num, args_.buffer_size);
		LocalStatistics::print_mem_alloc_info();
		//Align total vertices to multiple of # chunks
		args_.total_vertices = args_.max_vid = ceil((double)args_.total_vertices / num_chunks_) * num_chunks_;
        
    	system_fprintf(0, stdout, "sizeof(InputEdge) = %lld, sizeof(FileEdge) = %lld\n", sizeof(InputEdge), sizeof(FileEdge));
	}

	void finalize() {
		PartitionStatistics::wait_for_all();
		PartitionStatistics::close();
		MPI::Finalize();
	}

	double timewatch_wrapper(std::string routine, vFunctionCallback function) {
        PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "%s START\n", routine.c_str());
		auto begin_time = std::chrono::steady_clock::now();
		(this->*function)();
        PartitionStatistics::wait_for_all();
		auto end_time = std::chrono::steady_clock::now();
		system_fprintf(0, stdout, "%s END, Elapsed : %.3f\n", routine.c_str(), TIMEDIFF_MILLISEC(begin_time, end_time) / 1000);
		return TIMEDIFF_MILLISEC(begin_time, end_time);
	}

	void init_edge_list_and_reader() {
		glob_t gl;
        int file_num = 0;
        std::vector<int> edgefile_index;
		
        system_fprintf(0, stdout, "Source path = %s\n",args_.source_path.c_str());
		if(glob((std::string(args_.source_path.c_str()) + "/*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
            file_num = gl.gl_pathc;

        edgefile_index.resize(file_num);
        edgefile_list.resize(file_num);

        for (int i = 0; i < file_num; i++) {
            const char* file_name = std::strrchr(gl.gl_pathv[i], '/');
            if (!is_number(std::string(file_name+1))) {
				fprintf(stderr, "[%lld] Edge file name should consist of only integers; %s\n", PartitionStatistics::my_machine_id(), gl.gl_pathv[i]);
                throw std::runtime_error("[GraphPreprocessor::run] Invalid file name\n");
            }
			edgefile_index[i] = std::stoi(std::string(file_name+1));
        }
		
        std::sort(edgefile_index.begin(), edgefile_index.end());
		for (int i = 0; i < edgefile_list.size(); i++)
			edgefile_list[i] = args_.source_path + std::to_string(edgefile_index[i]);

        //TODO add creating condition
		std::string degree_table_filename_ = args_.temp_path + "degree_table";
		degree_table_ = new Splittable_Turbo_bin_io_handler(degree_table_filename_.c_str(), true, true, true);

		PartitionStatistics::wait_for_all();
	}

	void TwoDimRangePartitioning() {
		if(args_.machine_num == 0) {
			int64_t start_vid = 0;
			int64_t end_vid = -1;
            for(auto i = 0; i < num_chunks_; i++) {
                start_vid = end_vid + 1;
                end_vid = ceil((double)(args_.total_vertices * (i + 1)) / num_chunks_) - 1;
                fprintf(stdout, "[%lld] Range partition range begin[%lld] = %lld, end[%lld] = %lld\n", args_.machine_num, i, start_vid, i, end_vid); 
            }
		}
		
        PartitionStatistics::wait_for_all();
        begin_time_log("Two Dimension Range Partitioning");
		
        //Initialize Variables
        size_t buffer_size_ = args_.buffer_size / sizeof(FileEdge);
		int64_t self_edges = 0;
        edge_cnt = 0;
        
        std::vector<Turbo_bin_mmapper> edgefiles(edgefile_list.size());
        std::vector<InputEdge*> edgefile_ptr_(edgefiles.size());
        std::vector<std::vector<std::vector<int>>> edge_buffer_index(args_.num_threads);
        std::vector<std::vector<std::vector<FileEdge*>>> edge_buffer(args_.num_threads);
		
        std::vector<std::vector<std::vector<SimpleContainer>>> containers(args_.num_threads);
        std::vector<std::vector<Blocking_Turbo_bin_io_handler>> partition_files(num_chunks_);
		std::queue<std::future<void>>* reqs_to_wait = new std::queue<std::future<void>>[args_.num_threads];
		
        //Initialize vectors
        for(auto i = 0; i < edgefiles.size(); i++) {
			edgefiles[i].OpenFileAndMemoryMap(edgefile_list[i].c_str(), false);
			edgefile_ptr_[i] = (InputEdge*) edgefiles[i].data();
		}
	
		for(auto i = 0; i < args_.num_threads; i++) {
            containers[i].resize(num_chunks_);
            edge_buffer[i].resize(num_chunks_);
            edge_buffer_index[i].resize(num_chunks_);
			for(auto j = 0; j < num_chunks_; j++) {
                containers[i][j].resize(num_chunks_);
                edge_buffer[i][j].resize(num_chunks_);
                edge_buffer_index[i][j].resize(num_chunks_, 0);
				for(auto k = 0; k < num_chunks_; k++) {
					containers[i][j][k] = concurrent_buffer_pool->Alloc();
					edge_buffer[i][j][k] = (FileEdge*)containers[i][j][k].data;
				}
			}
		}
		
		for(auto i = 0; i < num_chunks_; i++) {
            partition_files[i].resize(num_chunks_);
			for(auto j = 0; j < num_chunks_; j++) {
				std::string file_name = args_.temp_path + "/partition_"+std::to_string(i)+"_"+std::to_string(j);
				truncate(file_name.c_str(), 0);
				remove(file_name.c_str());
				partition_files[i][j].OpenFile(file_name.c_str(), true, true, true);
			}
		}
		
        time_log("INIT");

		if(args_.bidirect_mode) { //Undirected Graph Partitioning
			for(auto i = 0; i < edgefiles.size(); i++) {
#pragma omp parallel num_threads(args_.num_threads)
				{
					InputEdge edge_pair;
					int tid = omp_get_thread_num();
					int64_t edge_cnt_per_thread = 0;
                    int64_t self_edges_per_thread = 0;
#pragma omp for
					for(int64_t j = 0; j < edgefiles[i].file_size() / sizeof(InputEdge); j++) {
						edge_pair = edgefile_ptr_[i][j];
						FileNodeType src = (FileNodeType)edge_pair.src_vid_;
						FileNodeType dst = (FileNodeType)edge_pair.dst_vid_;
						if(edge_pair.src_vid_ != edge_pair.dst_vid_) {
							edge_cnt_per_thread += 2;
#ifdef PRINT_LOG
							if(src > args_.total_vertices || dst > args_.total_vertices) {
  							    fprintf(stdout, "[%lld] Wrong src vid = %lld, dst vid = %lld at %lldth index / %lld\n", args_.machine_num, edge_pair.src_vid_, edge_pair.dst_vid_, j, edgefiles[i].file_size());
                                //throw unsupported_exception();
                            }
#endif
							INVARIANT(src <= args_.total_vertices && dst <= args_.total_vertices);
                            putEdgeIntoBuffer(src, dst, edge_buffer_index, edge_buffer, partition_files, containers, tid, buffer_size_, reqs_to_wait);
                            putEdgeIntoBuffer(dst, src, edge_buffer_index, edge_buffer, partition_files, containers, tid, buffer_size_, reqs_to_wait);
						} else {
                            self_edges_per_thread++;
                        }
					}
					std::atomic_fetch_add((std::atomic<int64_t>*)&edge_cnt, (int64_t)edge_cnt_per_thread);
					std::atomic_fetch_add((std::atomic<int64_t>*)&self_edges, (int64_t)self_edges_per_thread);
				}
				edgefiles[i].Close(args_.remove_mode);
			}
		} else { //Directed Graph Partitioning
			for(auto i = 0; i < edgefiles.size(); i++) {
#pragma omp parallel num_threads(args_.num_threads)
				{
					InputEdge edge_pair;
					int tid = omp_get_thread_num();
					int64_t edge_cnt_per_thread = 0;
					int partitionID_x, partitionID_y, index;
                    int64_t self_edges_per_thread = 0;
#pragma omp for
					for(int64_t j = 0; j < edgefiles[i].file_size() / sizeof(InputEdge); j++) {
						edge_pair = edgefile_ptr_[i][j];
						
						FileNodeType src = (FileNodeType)edge_pair.src_vid_;
						FileNodeType dst = (FileNodeType)edge_pair.dst_vid_;
						if(edge_pair.src_vid_ != edge_pair.dst_vid_) {
							edge_cnt_per_thread++;
#ifdef PRINT_LOG
							if(src > args_.total_vertices || dst > args_.total_vertices) {
  							    fprintf(stdout, "[%lld] Wrong src vid = %lld, dst vid = %lld at %lldth index / %lld\n", args_.machine_num, edge_pair.src_vid_, edge_pair.dst_vid_, j, edgefiles[i].file_size());
                                //throw unsupported_exception();
                            }
#endif
							INVARIANT(src <= args_.total_vertices && dst <= args_.total_vertices);
                            putEdgeIntoBuffer(src, dst, edge_buffer_index, edge_buffer, partition_files, containers, tid, buffer_size_, reqs_to_wait);
						} else {
                            self_edges_per_thread++;
                        }
					}
					std::atomic_fetch_add((std::atomic<int64_t>*)&edge_cnt, (int64_t)edge_cnt_per_thread);
					std::atomic_fetch_add((std::atomic<int64_t>*)&self_edges, (int64_t)self_edges_per_thread);
				}
				edgefiles[i].Close(args_.remove_mode);
			}
		}

#pragma omp parallel num_threads(args_.num_threads)
        {
            int tid = omp_get_thread_num();
            for(int64_t i = 0; i < num_chunks_; i++) {
                for(int64_t j = 0; j < num_chunks_; j++) {
                    if(edge_buffer_index[tid][i][j] != 0) {
                        for(int64_t k = edge_buffer_index[tid][i][j]; k < buffer_size_; k++) {
                            edge_buffer[tid][i][j][k].src_vid_ = (FileNodeType)-1;
                            edge_buffer[tid][i][j][k].dst_vid_ = (FileNodeType)-1;
                        }
                        reqs_to_wait[tid].push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_writeBuffertoFile, this, &partition_files[i][j], (char *)&edge_buffer[tid][i][j][0], args_.buffer_size, containers[tid][i][j]));
                    }
                }
            }
            while(!reqs_to_wait[tid].empty()) {
                reqs_to_wait[tid].front().get();
                reqs_to_wait[tid].pop();
            }
        }

        int64_t total_edge_cnt;
        int64_t total_self_edges;
        MPI_Allreduce(&edge_cnt, &total_edge_cnt, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
        MPI_Allreduce(&self_edges, &total_self_edges, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);

        system_fprintf(0, stdout, "Total edge cnt = %lld\n", total_edge_cnt); 
        system_fprintf(0, stdout, "Total self edges = %lld\n", total_self_edges); 

		time_log("READ EDGES");

		for(int64_t i = 0; i < num_chunks_; i++)
			for(int64_t j = 0; j < num_chunks_; j++)
				partition_files[i][j].Close();

		if(args_.remove_mode) {
			for (int i = 0; i < edgefile_list.size(); i++) {
				truncate(edgefile_list[i].c_str(), 0);
				remove(edgefile_list[i].c_str());
			}
		}
        delete reqs_to_wait;

        end_time_log("Two Dimension Range Partitioning");
	}

	void reduceDegreeTable(Turbo_bin_io_handler* degree_table) {
		begin_time_log("Reduce Degree Table");
		InputNodeType start_vid, end_vid;
		size_t temp_size = 512 * 1024L;
		size_t send_buffer_size = temp_size * sizeof(degree_t);
		turbo_timer timer;

		degree_t* temp_degrees = new degree_t[temp_size];
		degree_t* global_degrees = new degree_t[temp_size];

		PartitionStatistics::wait_for_all();
		time_log("wait_for_all");
		system_fprintf(0, stdout, "\tReduce Start\n");

		max_degree_ = 0;
		start_vid = 0;
		end_vid = start_vid + temp_size < args_.total_vertices ? start_vid + temp_size : args_.total_vertices;

		while(start_vid != args_.total_vertices) {
			InputNodeType section_size = end_vid - start_vid;

			ALWAYS_ASSERT (section_size > 0);
			ALWAYS_ASSERT (section_size <= temp_size);

			send_buffer_size = section_size * sizeof(degree_t);
			memset((void*)temp_degrees, 0, send_buffer_size);
			memset((void*)global_degrees, 0, send_buffer_size);

			degree_table->Read(start_vid * sizeof(degree_t), send_buffer_size, (char*)temp_degrees);

			int64_t sum = 0;

			timer.start_timer(0);
			int result = MPI_Allreduce(temp_degrees, global_degrees, section_size, mpi_type_, MPI_SUM, MPI_COMM_WORLD);
			timer.stop_timer(0);
			
			if(result != 0)
				fprintf(stderr, "MPI_Allreduce Error: %d\n",result);

			for(int64_t i = 0; i < section_size; i++)
				if(max_degree_ < global_degrees[i])
					max_degree_ = global_degrees[i];

			degree_table->Write(start_vid * sizeof(degree_t), send_buffer_size, (char*)&global_degrees[0]);

			start_vid = end_vid;
			end_vid = start_vid + temp_size < args_.total_vertices ? start_vid + temp_size : args_.total_vertices;
		}
		time_log("ALLREDUCE");
		//ALWAYS_ASSERT(max_degree_ <= args_.total_vertices);
		//if(args_.machine_num == 0)
			//fprintf(stdout, "\tReduce Elapsed time : %f, max_degree = %lld\n", timer.get_timer(0), (int64_t) max_degree_);

		delete[] temp_degrees;
		delete[] global_degrees;
		PartitionStatistics::wait_for_all();
		end_time_log("Reduce Degree Table");
	}

	void reduceDegreeTable(Splittable_Turbo_bin_io_handler* degree_table) {
		begin_time_log("Reduce Degree Table");
		InputNodeType start_vid, end_vid;
		std::size_t temp_size = 512 * 1024L;
		std::size_t send_buffer_size = temp_size * sizeof(degree_t);
		turbo_timer timer;

		degree_t* temp_degrees = new degree_t[temp_size];
		degree_t* global_degrees = new degree_t[temp_size];

		PartitionStatistics::wait_for_all();
		time_log("wait_for_all");
		system_fprintf(0, stdout, "\tReduce Start\n");

		max_degree_ = 0;
		start_vid = 0;
		end_vid = start_vid + temp_size < args_.total_vertices ? start_vid + temp_size : args_.total_vertices;
		system_fprintf(0, stdout, "total vertices = %lld\n",args_.total_vertices);

		while(start_vid != args_.total_vertices) {
			InputNodeType section_size = end_vid - start_vid;

			ALWAYS_ASSERT (section_size > 0);
			ALWAYS_ASSERT (section_size <= temp_size);

			send_buffer_size = section_size * sizeof(degree_t);
			memset((void*)temp_degrees, 0, send_buffer_size);
			memset((void*)global_degrees, 0, send_buffer_size);

			degree_table->Read(start_vid * sizeof(degree_t), send_buffer_size, (char*)temp_degrees);

			timer.start_timer(0);
			int result = MPI_Allreduce(temp_degrees, global_degrees, section_size, mpi_type_, MPI_SUM, MPI_COMM_WORLD);
			timer.stop_timer(0);

			if(result != 0)
				fprintf(stderr, "MPI_Allreduce Error: %d\n",result);

			if(start_vid == 0 && args_.machine_num == 0) {
				for(auto i = 0; i < 5; i++)
					fprintf(stdout, "[%lld] Degree value = %lld\n", args_.machine_num, global_degrees[i]);
			}

			for(int64_t i = 0; i < section_size; i++)
				if(max_degree_ < global_degrees[i])
					max_degree_ = global_degrees[i];

			degree_table->Write(start_vid * sizeof(degree_t), send_buffer_size, (char*)&global_degrees[0]);

			start_vid = end_vid;
			end_vid = start_vid + temp_size < args_.total_vertices ? start_vid + temp_size : args_.total_vertices;
		}
		time_log("ALLREDUCE");
		//ALWAYS_ASSERT(max_degree_ <= args_.total_vertices);
		//if(args_.machine_num == 0)
			//fprintf(stdout, "\tReduce Elapsed time : %f, max_degree = %lld\n", timer.get_timer(0), (int64_t) max_degree_);

		delete[] temp_degrees;
		delete[] global_degrees;
		PartitionStatistics::wait_for_all();
		end_time_log("Reduce Degree Table");
	}
	
	struct PaddedCounts {
			int64_t cnts;
			int64_t dummy1;
			int64_t dummy2;
			int64_t dummy3;
	};

	void makeNewVidTable() {
		begin_time_log("Make New Vid Table");
		std::vector<std::vector<int64_t>> cnt;
		std::vector<PaddedCounts> padded_histogram;
		PaddedCounts zero;
		zero.cnts = 0;
		zero.dummy1 = 0;
		zero.dummy2 = 0;
		zero.dummy3 = 0;
		std::vector<InputNodeType> newvid_offset;
		std::size_t temp_size = 128 * 1024 * 1024L; //128M
		std::size_t read_buffer_size = temp_size * sizeof(degree_t); //1GB
		std::size_t write_buffer_size;
		std::vector<degree_t> temp_degrees;
		InputNodeType start_vid, end_vid;

		if(args_.machine_count == 1) {
			max_degree_ = 0;

			start_vid = 0;
			end_vid = start_vid + temp_size < args_.total_vertices ? start_vid+temp_size : args_.total_vertices;
			while(start_vid != args_.total_vertices){
				InputNodeType section_size = end_vid - start_vid;
				read_buffer_size = section_size * sizeof(degree_t);
				ALWAYS_ASSERT (section_size > 0);
				temp_degrees.resize(section_size);
				degree_table_->Read(start_vid * sizeof(degree_t), read_buffer_size, (char*)&temp_degrees[0]);

				for(int64_t i = 0; i < section_size; i++) {
					if(max_degree_ < temp_degrees[i]) max_degree_ = temp_degrees[i];
				}

				start_vid = end_vid;
				end_vid =  start_vid+temp_size < args_.total_vertices ? start_vid+temp_size : args_.total_vertices;
			}

		}
		time_log("READ");

		//padded_histogram.resize(max_degree_ + 1, zero);
		cnt.resize(args_.num_threads);
		for(auto i = 0; i < args_.num_threads; i++) {
			cnt[i].resize(max_degree_ + 1, 0);
		}
		newvid_offset.resize(max_degree_ + 1, 0);

		start_vid = 0;
		end_vid =  start_vid+temp_size < args_.total_vertices ? start_vid+temp_size : args_.total_vertices;

		//TODO: overlap disk, cpu & reduce atomic
		while(start_vid != args_.total_vertices){
			InputNodeType section_size = end_vid - start_vid;
			read_buffer_size = section_size * sizeof(degree_t);

			ALWAYS_ASSERT (section_size > 0);
			temp_degrees.resize(section_size);
			// XXX
			degree_table_->Read(start_vid * sizeof(degree_t), read_buffer_size, (char*)&temp_degrees[0]);
			// XXX
			// Maybe Parallel for?
#pragma omp parallel for
			for(int64_t i = 0; i < section_size; i++) {
				ALWAYS_ASSERT(temp_degrees[i] <= max_degree_);
				//std::atomic_fetch_add((std::atomic<int64_t>*)&padded_histogram[temp_degrees[i]].cnts, (int64_t)1);
				//std::atomic_fetch_add((std::atomic<int64_t>*)&cnt[temp_degrees[i]], (int64_t)1);
				cnt[omp_get_thread_num()][temp_degrees[i]]++;
				//cnt[temp_degrees[i]]++;
			}
			start_vid = end_vid;
			end_vid =  start_vid+temp_size < args_.total_vertices ? start_vid+temp_size : args_.total_vertices;
		}
		time_log("CALCULATE DEGREE");

		PartitionStatistics::wait_for_all();
		time_log("wait_for_all");

		for(auto i = 1; i < args_.num_threads; i++) {
			for(auto j = 0; j < max_degree_ + 1; j++) {
				cnt[0][j] += cnt[i][j];
			}
		}
		int64_t temp = 0;
		for(auto i = max_degree_; i >= 0; i--) {
			newvid_offset[i] = temp;
			temp += cnt[0][i]; 
			//temp += padded_histogram[i].cnts;
		}
		time_log("MAX DEGREE CNT");

//		int64_t zerovertices = padded_histogram[0].cnts;
		int64_t zerovertices = cnt[0][0];
		int64_t temp_zero = 0;
		size_t start_offset;
		int64_t real_vertices = args_.total_vertices - zerovertices;
		int64_t col_size = ((real_vertices + args_.machine_count - 1) / args_.machine_count);
		int64_t last_row = real_vertices % args_.machine_count;
		fake_vertex = last_row == 0 ? 0 : args_.machine_count - last_row;
		int64_t temp_fake = fake_vertex;
		args_.total_vertices = args_.total_vertices - zerovertices + fake_vertex;
		bool test = zerovertices < fake_vertex ? false : true;

		system_fprintf(0, stdout, "\tZero vertices : %lld, Total vertices : %lld, Fake vertices : %lld\n", zerovertices, args_.total_vertices, fake_vertex);

		std::vector<FileNodeType> temp_buffer;

		start_vid = 0;
		end_vid =  start_vid+temp_size < args_.max_vid ? start_vid+temp_size : args_.max_vid;

		while(start_vid != args_.max_vid) {
			InputNodeType section_size = end_vid - start_vid;
			read_buffer_size = section_size * sizeof(degree_t);
			write_buffer_size = section_size * sizeof(FileNodeType);
			start_offset = start_vid * sizeof(degree_t);
			ALWAYS_ASSERT (section_size > 0);
			temp_degrees.resize(section_size);
			temp_buffer.resize(section_size);

			degree_table_->Read(start_offset, read_buffer_size, (char*)&temp_degrees[0], true);

			// XXX
//#pragma omp parallel for
			for(int64_t i = 0; i < section_size; i++) {
				int64_t idx = temp_degrees[i];
				ALWAYS_ASSERT(idx >= 0);
//				padded_histogram[idx].cnts--;
				//std::atomic_fetch_add((std::atomic<int64_t>*)&cnt[idx], (int64_t) -1);
				//if(cnt[idx] < 0) std::cout<<"Error : "<<i<<", "<<idx<<std::endl;
	//			ALWAYS_ASSERT(padded_histogram[idx].cnts >= 0);

				// XXX
				//int64_t order = std::atomic_fetch_add((std::atomic<int64_t>*)&cnt2[idx], (int64_t)1);
				int64_t order = newvid_offset[idx]++;
				if(idx == 0) {
					if(fake_vertex > 0) {
						//int64_t temp_ = std::atomic_fetch_add((std::atomic<int64_t>*)&fake_vertex, (int64_t)-1);
						temp_buffer[i] = col_size * (++last_row) - 1L;
						system_fprintf(0, stdout, "\tFake vertex = %lld\n", (int64_t)temp_buffer[i]);
						fake_vertex--;
					}
					else {
						temp_buffer[i] = -1L;
						//std::atomic_fetch_add((std::atomic<int64_t>*)&zerovertices, (int64_t)-1);
						zerovertices--;
					}
				}
				else {
					temp_buffer[i] = (order / args_.machine_count) + col_size * (order % args_.machine_count);
				}
				/*if(temp_degrees[i] < -1 || temp_degrees[i] > args_.max_vid) {
					fprintf(stdout, "[%lld] Error Mapping : %lld / %lld\n", args_.machine_num, temp_degrees[i], args_.max_vid);
					}
					ALWAYS_ASSERT(temp_degrees[i] >= -1 && temp_degrees[i] <= args_.max_vid);*/
			}

			oldtonew_vid_table_->Append(write_buffer_size, (char*)&temp_buffer[0]);

			start_vid = end_vid;
			end_vid =  start_vid+temp_size < args_.max_vid ? start_vid+temp_size : args_.max_vid;
		}
		time_log("MAX DEGREE CNT");

		degree_table_->Close(true);
		//for(int64_t i = 0; i < max_degree_ + 1; i++)
		//	ALWAYS_ASSERT(cnt[i] == 0);

		if(test)
			ALWAYS_ASSERT(zerovertices == temp_fake);
		//std::cout<<args_.machine_num<< "Make new vid phase finish"<<std::endl<<"Disk Elapsed time : "<<timer.get_timer(2)<<std::endl<<"CPU Elapsed time : "<<timer.get_timer(5)<<std::endl;
		end_time_log("Make New Vid Table");
	}

	template <typename T>
	void ReadfileAndProcess(std::vector<std::string> file_list) {
		Turbo_bin_io_handler** file_reader = new Turbo_bin_io_handler*[file_list.size()];
		
#pragma omp parallel num_threads(4)
		{
			std::size_t* start_offset = new std::size_t[3];
			std::size_t* buffer_rw_size = new std::size_t[3];
			std::size_t processed_size = 0;
			int cur = 0;
			int prev_cur = 0;

			std::queue<std::future<void>> read_reqs_to_wait;
			std::queue<std::future<void>> write_reqs_to_wait;
			T** buffer = new T*[3];
			SimpleContainer* containers = new SimpleContainer[3];

#pragma omp for
			for(auto i = 0; i < file_list.size(); i++) {
				processed_size = 0;
				cur = 0;
				file_reader[i] = new Turbo_bin_io_handler;
				file_reader[i]->OpenFile(file_list[i].c_str(), false, true);
				int64_t file_size = file_reader[i]->file_size();
				system_fprintf(0, stdout, "File open %s, size = %lld\n", file_list[i].c_str(), file_size);
				for(auto j = 0; j < 3; j++) {
					containers[j] = concurrent_buffer_pool->Alloc();
					buffer[j] = (T*)containers[j].data;
					start_offset[j] = 0;
					buffer_rw_size[j] = 32 * 1024 * 1024L;
				}

				buffer_rw_size[0] = std::min(buffer_rw_size[0], file_size - start_offset[0]);
				if(buffer_rw_size[0] > 0)
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[0], start_offset[0], buffer_rw_size[0]));

				start_offset[1] = start_offset[0] + buffer_rw_size[0];
				buffer_rw_size[1] = std::min(buffer_rw_size[1], file_size - start_offset[1]);
				if(buffer_rw_size[1] > 0)
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[1], start_offset[1], buffer_rw_size[1]));

				start_offset[2] = start_offset[1] + buffer_rw_size[1];
				buffer_rw_size[2] = std::min(buffer_rw_size[2], file_size - start_offset[2]);
				if(buffer_rw_size[2] > 0)
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[2], start_offset[2], buffer_rw_size[2]));

				while(processed_size < file_size) {
					prev_cur = (cur == 0) ? 2 : cur - 1;
					read_reqs_to_wait.front().get();
					read_reqs_to_wait.pop();
					
					ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(T) == 0);
					for(auto k = 0; k < (buffer_rw_size[cur] / sizeof(T)); k++) {
						buffer[cur][k].src_vid_ = 1;
					}

					write_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_writeBuffertoDisk, this, file_reader[i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur], containers[cur]));
					containers[cur] = concurrent_buffer_pool->Alloc();
					buffer[cur] = (T*)containers[cur].data;

					processed_size = start_offset[cur] + buffer_rw_size[cur];

					start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
					buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
					ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

					cur = (cur + 1) % 3;
//					cur = !cur;
					ALWAYS_ASSERT(cur == 0 || cur == 1 || cur == 2);
				}
				concurrent_buffer_pool->Dealloc(containers[0]);
				concurrent_buffer_pool->Dealloc(containers[1]);
				concurrent_buffer_pool->Dealloc(containers[2]);
				while(!read_reqs_to_wait.empty()) {
					read_reqs_to_wait.front().get();
					read_reqs_to_wait.pop();
				}
			}
			while(!write_reqs_to_wait.empty()) {
				write_reqs_to_wait.front().get();
				write_reqs_to_wait.pop();
			}

#pragma omp for
			for(auto i = 0; i < file_list.size(); i++) {
				processed_size = 0;
				cur = 0;
				int64_t file_size = file_reader[i]->file_size();
				system_fprintf(0, stdout, "File open %s, size = %lld\n", file_list[i].c_str(), file_size);
				for(auto j = 0; j < 3; j++) {
					containers[j] = concurrent_buffer_pool->Alloc();
					buffer[j] = (T*)containers[j].data;
					start_offset[j] = 0;
					buffer_rw_size[j] = 32 * 1024 * 1024L;
				}

				buffer_rw_size[0] = std::min(buffer_rw_size[0], file_size - start_offset[0]);
				if(buffer_rw_size[0] > 0)
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[0], start_offset[0], buffer_rw_size[0]));

				start_offset[1] = start_offset[0] + buffer_rw_size[0];
				buffer_rw_size[1] = std::min(buffer_rw_size[1], file_size - start_offset[1]);
				if(buffer_rw_size[1] > 0)
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[1], start_offset[1], buffer_rw_size[1]));

				start_offset[2] = start_offset[1] + buffer_rw_size[1];
				buffer_rw_size[2] = std::min(buffer_rw_size[2], file_size - start_offset[2]);
				if(buffer_rw_size[2] > 0)
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[2], start_offset[2], buffer_rw_size[2]));

				while(processed_size < file_size) {
					prev_cur = (cur == 0) ? 2 : cur - 1;
					read_reqs_to_wait.front().get();
					read_reqs_to_wait.pop();
					
					ALWAYS_ASSERT(buffer_rw_size[cur] % sizeof(T) == 0);
					for(auto k = 0; k < (buffer_rw_size[cur] / sizeof(T)); k++) {
						if(buffer[cur][k].src_vid_ != 1) {
							fprintf(stdout, "Wrong value\n");
							//throw unsupported_exception();
						}
					}

					containers[cur] = concurrent_buffer_pool->Alloc();
					buffer[cur] = (T*)containers[cur].data;

					processed_size = start_offset[cur] + buffer_rw_size[cur];

					start_offset[cur] = start_offset[prev_cur] + buffer_rw_size[prev_cur];
					buffer_rw_size[cur] = std::min(buffer_rw_size[cur], file_size - start_offset[cur]);
					ALWAYS_ASSERT(start_offset[cur] + buffer_rw_size[cur] <= file_size);
					read_reqs_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_readBufferfromDisk, this, file_reader[i], (char *)buffer[cur], start_offset[cur], buffer_rw_size[cur]));

					cur = (cur + 1) % 3;
//					cur = !cur;
					ALWAYS_ASSERT(cur == 0 || cur == 1 || cur == 2);
				}
				concurrent_buffer_pool->Dealloc(containers[0]);
				concurrent_buffer_pool->Dealloc(containers[1]);
				concurrent_buffer_pool->Dealloc(containers[2]);
				while(!read_reqs_to_wait.empty()) {
					read_reqs_to_wait.front().get();
					read_reqs_to_wait.pop();
				}
			}

			delete[] containers;
			delete[] buffer;
			delete buffer_rw_size;
			delete start_offset;
		}

		for(auto i = 0; i < file_list.size(); i++) {
			file_reader[i]->Close();
		}

		delete file_reader;
	}

	void testNewVidTable() {
		size_t temp_size = 128 * 1024 * 1024L; //128M
		size_t read_buffer_size = temp_size * sizeof(degree_t); //1GB
		std::vector<InputNodeType> temp_degrees;
		InputNodeType start_vid, end_vid;

		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "Mapping correctness test start\n");
		start_vid = 0;
		end_vid =  start_vid+temp_size < args_.max_vid ? start_vid+temp_size : args_.max_vid;

		while(start_vid != args_.max_vid){
			InputNodeType section_size = end_vid - start_vid;
			read_buffer_size = section_size * sizeof(InputNodeType);
			size_t start_offset = start_vid * sizeof(InputNodeType);
			temp_degrees.resize(section_size, 0L);

			degree_table_->Read(start_offset, read_buffer_size, (char*)&temp_degrees[0]);

			for(int64_t i = 0; i < section_size; i++) {
				int64_t idx = temp_degrees[i];
				if(idx != i + start_vid) {
					if(idx != -1) {
						fprintf(stdout, "[%lld] Error index : %lld, Error value : %lld\n", args_.machine_num, i + start_vid, idx);
						//throw unsupported_exception();
					}
				}
			}

			start_vid = end_vid;
			end_vid =  start_vid+temp_size < args_.max_vid ? start_vid+temp_size : args_.max_vid;
		}
	}
	//XXX
	void testOldtoNewVidTable() {
		size_t temp_size = 128 * 1024 * 1024L; //128M
		size_t read_buffer_size = temp_size * sizeof(FileNodeType); //1GB
		std::vector<FileNodeType> temp_degrees;
		std::vector<uint64_t> bit_vector;
		bit_vector.resize(ceil((args_.total_vertices + 63) / 64), 0);
		InputNodeType start_vid, end_vid;
		int64_t relabeled_vertices = 0;

		start_vid = 0;
		end_vid = std::min(start_vid + temp_size, args_.max_vid);

		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "BEGIN\tTest whether Old vid to New vid mapping is bijective\n");
		while(start_vid != args_.max_vid){
			InputNodeType section_size = end_vid - start_vid;
			read_buffer_size = section_size * sizeof(FileNodeType);
			std::size_t start_offset = start_vid * sizeof(FileNodeType);
			temp_degrees.resize(section_size);

			oldtonew_vid_table_->Read(start_offset, read_buffer_size, (char*)&temp_degrees[0]);

			for(int64_t i = 0; i < section_size; i++) {
				int64_t idx = temp_degrees[i];
				if(idx != -1) {
					int64_t idx2 = idx / 64;
					uint64_t bit_block = bit_vector[idx2];
					uint64_t shift = 63 - (idx - (64 * idx2));
					uint64_t mask = 1L << shift;
					if((bit_block & mask) == 0) {
						bit_vector[idx2] = bit_block | mask;
						relabeled_vertices++;
					} else {
						fprintf(stdout, "\t[%lld] Duplicate index : %lld at %lld\n", args_.machine_num, idx, i + start_vid);
						//throw unsupported_exception();
					}
				}
			}

			start_vid = end_vid;
			end_vid = std::min(start_vid + temp_size, args_.max_vid);
		}

		if(args_.total_vertices <= args_.max_vid && args_.total_vertices != relabeled_vertices)
			fprintf(stdout, "END\t[%lld] Uniqueness test Fail, Total vertices : %lld\n",args_.machine_num, relabeled_vertices);
	}

	void testBidirect() {
		size_t temp_size = 128 * 1024 * 1024L; //128M
		size_t send_buffer_size = temp_size * sizeof(degree_t); //1GB
		std::vector<degree_t> temp_degrees;
		InputNodeType start_vid, end_vid;

		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "BEGIN\t Test whether degree is multiples of 2 in undirected Graph\n");
		start_vid = 0;
		end_vid =  start_vid+temp_size < args_.max_vid ? start_vid+temp_size : args_.max_vid;

		while(start_vid != args_.max_vid){
			InputNodeType section_size = end_vid - start_vid;
			send_buffer_size = section_size * sizeof(degree_t);
			size_t start_offset = start_vid * sizeof(degree_t);
			temp_degrees.resize(section_size);

			degree_table_->Read(start_offset, send_buffer_size, (char*)&temp_degrees[0]);

			for(int64_t i = 0; i < section_size; i++) {
				int64_t idx = temp_degrees[i];
				if(idx % 2 == 1) {
					fprintf(stdout, "[%lld] Error index : %lld, Error value : %lld\n", args_.machine_num, i + start_vid, idx);
					//throw unsupported_exception();
				}
			}

			start_vid = end_vid;
			end_vid =  start_vid+temp_size < args_.max_vid ? start_vid+temp_size : args_.max_vid;
		}
	}

	void testSendPartition() {
		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "BEGIN\tTest whether (OutDegrees == InDegrees) holds on Undirected Graph\n");

		ALWAYS_ASSERT(args_.total_vertices % args_.machine_count == 0);

		Turbo_bin_io_handler* indegree_table_ = new Turbo_bin_io_handler;
		indegree_table_->OpenFile((args_.temp_path + "/indegree_table").c_str(), true, true, true);

		Turbo_bin_io_handler** send_files = new Turbo_bin_io_handler*[args_.machine_count];
		for(int64_t i = 0; i < args_.machine_count; i++) {
			std::string file_name = args_.temp_path + "/send_dst_" + std::to_string(i);
			send_files[i] = new Turbo_bin_io_handler;
			send_files[i]->OpenFile(file_name.c_str(), false, false, false);
		}

		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "\tCreating Indegree table\n");
		std::vector<FileEdge> temp_file;
		std::vector<degree_t> indegree_table_buffer;
		InputNodeType start_vid = 0;
		InputNodeType end_vid = -1;
		for(int64_t i = 0; i < args_.machine_count; i++) {
			start_vid = end_vid + 1;
			end_vid = ((i + 1) * args_.total_vertices) / args_.machine_count - 1;
			InputNodeType section_size = end_vid - start_vid + 1;
			indegree_table_buffer.resize(section_size);
			memset((void*)&indegree_table_buffer[0], 0, section_size * sizeof(degree_t));

			//XXX
			std::size_t buffer_size = 768 * 1024 * 1024L;
			temp_file.resize(buffer_size / sizeof(FileEdge));
			std::size_t temp_file_size = send_files[i]->file_size();
			int64_t partition_size = ceil((double)temp_file_size / buffer_size);

			int64_t start, end;
			start = 0;
			end = 0;
			end = end + buffer_size < temp_file_size ? end + buffer_size : temp_file_size;
			for(int64_t k = 0; k < partition_size; k++) {
				std::size_t section_size_ = end - start;
				send_files[i]->Read(start, section_size_, (char*)&temp_file[0]);

				for(int64_t j = 0; j < (section_size_ / sizeof(FileEdge)); j++) {
					ALWAYS_ASSERT(temp_file[j].dst_vid_ >= start_vid && temp_file[j].dst_vid_ <= end_vid);
					indegree_table_buffer[temp_file[j].dst_vid_ - start_vid]++;
				}
				start = end;
				end = end + buffer_size < temp_file_size ? end + buffer_size : temp_file_size;
			}
			ALWAYS_ASSERT(end == temp_file_size);
			indegree_table_->Append(section_size * sizeof(degree_t), (char*)&indegree_table_buffer[0]);
		}

		indegree_table_buffer.clear();

		reduceDegreeTable(indegree_table_);

		Turbo_bin_io_handler* outdegree_table_ = new Turbo_bin_io_handler;
		outdegree_table_->OpenFile((args_.temp_path + "/outdegree_table").c_str(), true, true, true);

		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "\tCreating Outdegree table\n");
		Turbo_bin_io_handler** dst_send_files = new Turbo_bin_io_handler*[args_.machine_count];
		for(int64_t i = 0; i < args_.machine_count; i++) {
			std::string file_name = args_.temp_path + "/send_" + std::to_string(i);
			dst_send_files[i] = new Turbo_bin_io_handler;
			dst_send_files[i]->OpenFile(file_name.c_str(), false, false, false);
		}

		std::vector<degree_t> outdegree_table_buffer;
		start_vid = 0;
		end_vid = -1;
		for(int64_t i = 0; i < args_.machine_count; i++) {
			start_vid = end_vid + 1;
			end_vid = ((i + 1) * args_.total_vertices) / args_.machine_count - 1;
			InputNodeType section_size = end_vid - start_vid + 1;
			outdegree_table_buffer.resize(section_size);
			memset((void*)&outdegree_table_buffer[0], 0, section_size * sizeof(degree_t));

			//XXX
			std::size_t buffer_size = 768 * 1024 * 1024L;
			temp_file.resize(buffer_size / sizeof(FileEdge));
			std::size_t temp_file_size = dst_send_files[i]->file_size();
			int64_t partition_size = ceil((double)temp_file_size / buffer_size);

			int64_t start, end;
			start = 0;
			end = 0;
			end = end + buffer_size < temp_file_size ? end + buffer_size : temp_file_size;

			for(int64_t k = 0; k < partition_size; k++) {
				std::size_t section_size = end - start;
				dst_send_files[i]->Read(start, section_size, (char*)&temp_file[0]);

				for(int64_t j = 0; j < (section_size / sizeof(FileEdge)); j++) {
					ALWAYS_ASSERT(temp_file[j].src_vid_ >= start_vid && temp_file[j].src_vid_ <= end_vid);
					outdegree_table_buffer[temp_file[j].src_vid_ - start_vid]++;
				}
				start = end;
				end = end + buffer_size < temp_file_size ? end + buffer_size : temp_file_size;
			}
			ALWAYS_ASSERT(end == temp_file_size);
			outdegree_table_->Append(section_size * sizeof(degree_t), (char*)&outdegree_table_buffer[0]);
		}

		outdegree_table_buffer.clear();

		reduceDegreeTable(outdegree_table_);

		int64_t wrong_vertices = 0;
		int64_t zero_vertices = 0;
		bool success = true;

		start_vid = 0;
		end_vid = -1;
		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "\tChecking InDegree ?= OutDegree\n");
		for(int64_t i = 0; i < args_.machine_count; i++) {
			start_vid = end_vid + 1;
			end_vid = ((i + 1) * args_.total_vertices) / args_.machine_count - 1;
			InputNodeType section_size = end_vid - start_vid + 1;

			indegree_table_buffer.resize(section_size);
			outdegree_table_buffer.resize(section_size);
			memset((void*)&indegree_table_buffer[0], 0, section_size * sizeof(degree_t));
			memset((void*)&outdegree_table_buffer[0], 0, section_size * sizeof(degree_t));

			indegree_table_->Read(start_vid * sizeof(degree_t), section_size * sizeof(degree_t), (char*)&indegree_table_buffer[0]);
			outdegree_table_->Read(start_vid * sizeof(degree_t), section_size * sizeof(degree_t), (char*)&outdegree_table_buffer[0]);

			for(int64_t j = 0; j < section_size; j++) {
				if(indegree_table_buffer[j] != outdegree_table_buffer[j]) {
					success = false;
					//if(args_.machine_num == 0)
						//fprintf(stdout, "[%lld] Indegree[%lld] : %lld, Outdegree[%lld] : %lld\n", args_.machine_num, j + start_vid, indegree_table_buffer[j], j + start_vid, outdegree_table_buffer[j]);
					wrong_vertices++;
				}

				if((indegree_table_buffer[j] == 0) || (outdegree_table_buffer[j] == 0)) {
					zero_vertices++;
				}
			}
		}
		indegree_table_->Close(true);
		outdegree_table_->Close(true);

		for(int64_t i = 0; i < args_.machine_count; i++) {
			dst_send_files[i]->Close();
		}
		for(int64_t i = 0; i < args_.machine_count; i++) {
			send_files[i]->Close(true);
		}
		//delete temp_edge_reader;
		delete outdegree_table_;
		delete indegree_table_;
		delete[] send_files;
		delete[] dst_send_files;

		if(!success) {
			fprintf(stdout, "\t[%lld] Send partition test Fail, Wrong vertices : %lld, Zero vertices : %lld\n", args_.machine_num, wrong_vertices, zero_vertices);
		} else {
			system_fprintf(0, stdout, "\tWrong vertices : %lld, Zero vertices : %lld\nEND\tChecking OutDegrees == InDegrees for Undirected Graph\n", wrong_vertices, zero_vertices);
		}
	}

	void testInOutDegreeAndMakeDegree(Splittable_Turbo_bin_io_handler* indegree_table, Splittable_Turbo_bin_io_handler* outdegree_table) {
		max_degree_ = 0;
		bool success = true;
		int64_t wrong_vertices = 0;
		int64_t zero_vertices = 0;
		int64_t temp_chunks;
		InputNodeType start_vid, end_vid;
		std::vector<degree_t> indegree_table_buffer;
		std::vector<degree_t> outdegree_table_buffer;
		start_vid = 0;
		end_vid = -1;
		temp_chunks = num_chunks_ * 2;
		PartitionStatistics::wait_for_all();
		system_fprintf(0, stdout, "\tChecking InDegree ?= OutDegree\n");
		for(int64_t i = 0; i < temp_chunks; i++) {
			start_vid = end_vid + 1;
			end_vid = ceil((double)(args_.total_vertices * (i + 1)) / temp_chunks) - 1;
			if(end_vid == args_.total_vertices) end_vid--;
			InputNodeType section_size = end_vid - start_vid + 1;

			indegree_table_buffer.resize(section_size);
			outdegree_table_buffer.resize(section_size);
			memset((void*)&indegree_table_buffer[0], 0, section_size * sizeof(degree_t));
			memset((void*)&outdegree_table_buffer[0], 0, section_size * sizeof(degree_t));

			indegree_table->Read(start_vid * sizeof(degree_t), section_size * sizeof(degree_t), (char*)&indegree_table_buffer[0]);
			outdegree_table->Read(start_vid * sizeof(degree_t), section_size * sizeof(degree_t), (char*)&outdegree_table_buffer[0]);

			for(int64_t j = 0; j < section_size; j++) {
				if(indegree_table_buffer[j] != outdegree_table_buffer[j]) {
					success = false;
					//if(args_.machine_num == 0)
					//	fprintf(stdout, "[%lld] Indegree[%lld] : %lld, Outdegree[%lld] : %lld\n", args_.machine_num, j + start_vid, indegree_table_buffer[j], j + start_vid, outdegree_table_buffer[j]);
					wrong_vertices++;
				}

				if((indegree_table_buffer[j] == 0) || (outdegree_table_buffer[j] == 0)) {
					zero_vertices++;
				}
				indegree_table_buffer[j] = indegree_table_buffer[j] + outdegree_table_buffer[j];
				if(max_degree_ < indegree_table_buffer[j])
					max_degree_ = indegree_table_buffer[j];
			}
			indegree_table->Write(start_vid * sizeof(degree_t), section_size * sizeof(degree_t), (char*)&indegree_table_buffer[0]);
		}


		if(!success) {
			fprintf(stdout, "\t[%lld] Send partition test Fail, Wrong vertices : %lld, Zero vertices : %lld\n", args_.machine_num, wrong_vertices, zero_vertices);
		}
		else {
			system_fprintf(0, stdout, "END\tChecking OutDegrees == InDegrees for Local Merged Graph\n");
		}
	}

	static void call_writeBuffertoFile(GraphPreprocessor* pointer, Blocking_Turbo_bin_io_handler* handler, char* buffer, size_t size_to_append, SimpleContainer cont) {
		pointer->writeBuffertoFile(handler, buffer, size_to_append, cont);
	}

	void writeBuffertoFile(Blocking_Turbo_bin_io_handler* handler, char* buffer, size_t size_to_append, SimpleContainer cont) {
		handler->Append(size_to_append, buffer);
		concurrent_buffer_pool->Dealloc(cont);
	}


	static void call_writeBuffertoDisk(GraphPreprocessor* pointer, Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_write, SimpleContainer cont) {
		pointer->writeBuffertoDisk(handler, buffer, start_offset, size_to_write, cont);
	}

	void writeBuffertoDisk(Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_write, SimpleContainer cont) {
		handler->Write(start_offset, size_to_write, buffer);
		concurrent_buffer_pool->Dealloc(cont);
	}

	static void call_readBufferfromDisk(GraphPreprocessor* pointer, Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_read) {
		pointer->readBufferfromDisk(handler, buffer, start_offset, size_to_read);
	}

	void readBufferfromDisk(Turbo_bin_io_handler* handler, char* buffer, std::size_t start_offset, std::size_t size_to_read) {
		handler->Read(start_offset, size_to_read, buffer);
	}

	void store_partitionedRanges() {
		TextFormatSequentialWriter* partitionedRangeFile = new TextFormatSequentialWriter((args_.partitioned_range_path + "/" + std::to_string(args_.machine_num)).c_str());

		int64_t col_size = ceil((double)args_.total_vertices / args_.machine_count);
		ALWAYS_ASSERT(args_.total_vertices % args_.machine_count == 0);

		std::string src_vid = std::to_string(col_size * args_.machine_num);
		std::string dst_vid = std::to_string(col_size * (args_.machine_num + 1) - 1);
		std::string partitionedRange = (src_vid + "\t" + dst_vid + "\n");
		partitionedRangeFile->pushNext(partitionedRange);

		delete partitionedRangeFile;
	}

	void store_partitionedRanges(std::vector<int64_t> partition_range) {
		TextFormatSequentialWriter* partitionedRangeFile = new TextFormatSequentialWriter((args_.partitioned_range_path + "/" + std::to_string(args_.machine_num)).c_str());

		ALWAYS_ASSERT(args_.total_vertices % args_.machine_count == 0);

		std::string src_vid;
		if(args_.machine_num == 0)
			src_vid = std::to_string(0);
		else
			src_vid = std::to_string(partition_range[args_.machine_num - 1] + 1);
		std::string dst_vid = std::to_string(partition_range[args_.machine_num]);
		std::string partitionedRange = (src_vid + "\t" + dst_vid + "\n");
		partitionedRangeFile->pushNext(partitionedRange);

		delete partitionedRangeFile;
	}

	void makeSendPartition() {
		begin_time_log("Make Send Partition");
		ReturnStatus rs;
		PartitionStatistics::wait_for_all();
		time_log("wait_for_all");
        if (args_.partitioning_scheme != 1) {
			rs = edge_partitioned_->makeSendPartition(args_, std::vector<int64_t> ());
        } else {
			rs = edge_partitioned_->makeSendPartition(args_, range);
        }
		time_log("wait_for_all");

		if (rs == FAIL) {
			fprintf(stdout, "[%lld] Something wrong in makeSendPartition", args_.machine_num);
			throw std::runtime_error("");
		}
		end_time_log("Make Send Partition");
	}

	void makeInOutDegree() {
		Splittable_Turbo_bin_io_handler* outdegree_table_ = new Splittable_Turbo_bin_io_handler((args_.temp_path + "/outdegree_table").c_str(), true, true, true);

		sleep(1);
		system_fprintf(0, stdout, "BEGIN\tTest whether (OutDegrees == InDegrees) holds on Local Merged Graph\n");

		edge_partitioned_->calculateLocalInOutDegreeTable(degree_table_, outdegree_table_, args_.num_threads, edge_cnt, args_.total_vertices, num_chunks_, args_.machine_num);

		if(args_.machine_count > 1)
			reduceDegreeTable(degree_table_);
		if(args_.machine_count > 1)
			reduceDegreeTable(outdegree_table_);
		testInOutDegreeAndMakeDegree(degree_table_, outdegree_table_);

		outdegree_table_->Close(true);
		delete outdegree_table_;
	}

	void makeVidRange() {
		begin_time_log("Make Vid Range");
		int64_t total_edge = 0;
		int64_t total_edge2 = 0;
		int idx = 0;
		InputNodeType start_vid, end_vid;
		std::vector<degree_t> temp_degrees;
		size_t temp_size = 512 * 1024L;
		size_t send_buffer_size = temp_size * sizeof(degree_t);

		system_fprintf(0, stdout, "BEGIN\tMake vid range; partitioned by degree size\n");

		range.resize(args_.machine_count, 0);
		temp_degrees.resize(temp_size);

		time_log("INIT");
		MPI_Allreduce(MPI_IN_PLACE, &edge_cnt, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
		time_log("AllReduce");

		int64_t edge_partition_size = (edge_cnt + args_.machine_count - 1) / args_.machine_count;
		edge_partition_size *= 2;
		system_fprintf(0, stdout, "\tTotal edge in all machine = %lld, Number of degree in a partition = %lld\n", edge_cnt, edge_partition_size);

		start_vid = 0;
		end_vid = start_vid + temp_size < args_.total_vertices ? start_vid + temp_size : args_.total_vertices;

		while(start_vid != args_.total_vertices) {
			InputNodeType section_size = end_vid - start_vid;
			send_buffer_size = section_size * sizeof(degree_t);
			degree_table_->Read(start_vid * sizeof(degree_t), send_buffer_size, (char*)&temp_degrees[0]);

			for(int64_t i = 0; i < section_size; i++) {
				total_edge += temp_degrees[i];
				total_edge2 += temp_degrees[i];
				if(total_edge >= edge_partition_size && idx < args_.machine_count) {
					range[idx] = i + start_vid;
					total_edge = 0;
					idx++;
				}
			}

			degree_table_->Write(start_vid * sizeof(degree_t), send_buffer_size, (char*)&temp_degrees[0]);
			start_vid = end_vid;
			end_vid = start_vid + temp_size < args_.total_vertices ? start_vid + temp_size : args_.total_vertices;
		}
		time_log("Vid Calculate");

		while(idx < args_.machine_count) {
			range[idx] = args_.total_vertices;
			idx++;
		}
		ALWAYS_ASSERT(idx == args_.machine_count);
		ALWAYS_ASSERT(total_edge2 == edge_cnt * 2);
		ALWAYS_ASSERT(range[args_.machine_count - 1] == args_.total_vertices);

		if(args_.machine_num == 0) {
			fprintf(stdout, "\tCounted total edge : %lld\n",total_edge2);
			for(int i = 0; i < args_.machine_count; i++) {
				fprintf(stdout, "\tRange[%d] : %lld\n", i, range[i]);
			}
			fprintf(stdout, "END\tMake vid range\n");
		}
		end_time_log("Make Vid Range");
	}

	void GraphRenumbering() {
		GraphRenumberer<FileNodeType, Degree_t> renumberer(edge_partitioned_, num_chunks_, UserArguments::NUM_THREADS, args_.total_vertices);
        //spawn thread for RequestRespond
		std::thread* rr_receiver_ = new std::thread(RequestRespond::ReceiveRequest);
        //initialize UserArguments, PartitionStatistics, RequestRespond
        init_graphRenumbering();
		
#ifdef PRINT_LOG
        if(PartitionStatistics::my_machine_id() == 0)
    		fprintf(stdout, "[%lld] Total vertices before renumbering = %lld\n", args_.machine_num, args_.total_vertices);
		PartitionStatistics::wait_for_all();
#endif

        renumberer.run_(0, NULL, &args_.total_vertices, args_.partitioning_scheme);
        INVARIANT(args_.total_vertices % args_.machine_count == 0);

#ifdef PRINT_LOG
        if(PartitionStatistics::my_machine_id() == 0)
		    fprintf(stdout, "[%lld] Total vertices after renumbering = %lld\n", args_.machine_num, args_.total_vertices);
#endif

        //Update PartitionStatistics, finalize RequestRespond
        finalize_graphRenumbering();
        //Wait for terminating spawned thread
		rr_receiver_->join();
        //Store partitioned ranges
        store_partitionedRanges();
        PartitionStatistics::wait_for_all();
	}

    inline void putEdgeIntoBuffer(FileNodeType src, FileNodeType dst, std::vector<std::vector<std::vector<int>>>& edge_buffer_index, std::vector<std::vector<std::vector<FileEdge*>>>& edge_buffer, std::vector<std::vector<Blocking_Turbo_bin_io_handler>>& partition_files, std::vector<std::vector<std::vector<SimpleContainer>>>& containers, int tid, size_t buffer_size_, std::queue<std::future<void>>* reqs_to_wait) {
        int partitionID_x, partitionID_y, index;

        partitionID_x = VID_TO_PARTITIONID(src, num_chunks_, args_.total_vertices);
        partitionID_y = VID_TO_PARTITIONID(dst, num_chunks_, args_.total_vertices);

        ALWAYS_ASSERT(partitionID_x < num_chunks_ && partitionID_y < num_chunks_);

        index = edge_buffer_index[tid][partitionID_x][partitionID_y];
        ALWAYS_ASSERT(index <= buffer_size_); //expand buffer size
        edge_buffer[tid][partitionID_x][partitionID_y][index].src_vid_ = src;
        edge_buffer[tid][partitionID_x][partitionID_y][index].dst_vid_ = dst;

        edge_buffer_index[tid][partitionID_x][partitionID_y]++;
        if(edge_buffer_index[tid][partitionID_x][partitionID_y] == buffer_size_) {
            ALWAYS_ASSERT(edge_buffer_index[tid][partitionID_x][partitionID_y] == args_.buffer_size / sizeof(FileEdge));
            reqs_to_wait[tid].push(Aio_Helper::async_pool.enqueue(GraphPreprocessor::call_writeBuffertoFile, this, &partition_files[partitionID_x][partitionID_y], (char *)&edge_buffer[tid][partitionID_x][partitionID_y][0], args_.buffer_size, containers[tid][partitionID_x][partitionID_y]));
            containers[tid][partitionID_x][partitionID_y] = concurrent_buffer_pool->Alloc();
            edge_buffer[tid][partitionID_x][partitionID_y] = (FileEdge*)containers[tid][partitionID_x][partitionID_y].data;
            edge_buffer_index[tid][partitionID_x][partitionID_y] = 0;
        }
    }

    void init_graphRenumbering() {
		//init userarguments
		UserArguments::VECTOR_PARTITIONS = num_chunks_ / args_.machine_count;
		UserArguments::WORKSPACE_PATH = args_.temp_path;
		UserArguments::SCHEDULE_TYPE = -1;
		UserArguments::MAX_LEVEL = 1;

		//init partitionstatistics
		PartitionStatistics::init();
		PartitionStatistics::my_first_node_id() = ceil((double)(args_.total_vertices * args_.machine_num * UserArguments::VECTOR_PARTITIONS) / num_chunks_);
		PartitionStatistics::my_last_node_id() = ceil((double)(args_.total_vertices * (args_.machine_num + 1) * UserArguments::VECTOR_PARTITIONS) / num_chunks_) - 1;
		PartitionStatistics::my_first_internal_vid() = PartitionStatistics::my_first_node_id();
		PartitionStatistics::my_last_internal_vid() = PartitionStatistics::my_last_node_id();
        PartitionStatistics::my_num_internal_nodes() = (args_.total_vertices + args_.machine_count - 1) / args_.machine_count;
		PartitionStatistics::num_subchunks_per_edge_chunk() = 1;

		TurboDB::EdgeSubchunksVidRange.resize(num_chunks_);
		for(int i = 0; i < num_chunks_; i++) {
			TurboDB::EdgeSubchunksVidRange[i].SetBegin(ceil((double)(args_.total_vertices * i) / num_chunks_));
			TurboDB::EdgeSubchunksVidRange[i].SetEnd(ceil((double)(args_.total_vertices * (i + 1)) / num_chunks_) - 1);
		}

		PartitionStatistics::wait_for_all();
		PartitionStatistics::replicate();
#ifdef PRINT_LOG
		system_fprintf(0, stdout, "WORKSPACE_PATH = %s, VECTOR_PARTITIONS = %d, NUM_THREADS = %d\nMY_FIRST_NODE_ID = %lld, MY_LAST_NODE_ID = %lld, MY_NUM_INTERNAL_NODES = %lld\n", UserArguments::WORKSPACE_PATH.c_str(), UserArguments::VECTOR_PARTITIONS, UserArguments::NUM_THREADS, PartitionStatistics::my_first_node_id(), PartitionStatistics::my_last_node_id(), PartitionStatistics::my_num_internal_nodes());
#endif

		//initialize RequestRespond
		RequestRespond::Initialize(4 * 1024 * 1024L, 128, 2 * 1024 * 1024 * 1024L, DEFAULT_NIO_BUFFER_SIZE, DEFAULT_NIO_THREADS, UserArguments::NUM_THREADS);

        //initialize threads
#pragma omp parallel num_threads(UserArguments::NUM_THREADS) 
		{
			int i = omp_get_thread_num();
			TG_ThreadContexts::thread_id = i;
			TG_ThreadContexts::core_id = NumaHelper::assign_core_id_scatter(i, 0, UserArguments::NUM_THREADS - 1);
			TG_ThreadContexts::socket_id = NumaHelper::get_socket_id_by_omp_thread_id(i);
		}
    }

    void finalize_graphRenumbering() {
		PartitionStatistics::wait_for_all();
		//reinitialize PartitionStatistics
		PartitionStatistics::my_num_internal_nodes() = args_.total_vertices / args_.machine_count;
		PartitionStatistics::my_first_node_id() = PartitionStatistics::my_num_internal_nodes() * PartitionStatistics::my_machine_id();
		PartitionStatistics::my_last_node_id() = PartitionStatistics::my_num_internal_nodes() * (PartitionStatistics::my_machine_id() + 1) - 1;
		PartitionStatistics::my_first_internal_vid() = PartitionStatistics::my_first_node_id();
		PartitionStatistics::my_last_internal_vid() = PartitionStatistics::my_last_node_id();
		PartitionStatistics::num_subchunks_per_edge_chunk() = 1; //XXX why 2?
		PartitionStatistics::replicate();

		RequestRespond::Close();
		RequestRespond::EndRunning();
    }

    void GraphRenumberingByGivenRange() {
        std::vector<int64_t> offset;
        std::ifstream range_file;
        int temp_idx = 0;
        int64_t max_range = 0;
        char line[100];

        range.resize(args_.machine_count);
        offset.resize(args_.machine_count);
        range_file.open(args_.given_partition_range_path.c_str());

        // Calculate offset for transforming given range to equi-range partitioning
        while (range_file.getline(line, 100)) {
            range[temp_idx] = std::stoll(line) - 1;
            ALWAYS_ASSERT(temp_idx < args_.machine_count);
            if (temp_idx == 0) {
                max_range = range[temp_idx] + 1;
            } else {
                if(max_range < range[temp_idx] - range[temp_idx - 1])
                    max_range = range[temp_idx] - range[temp_idx - 1];
            }
            system_fprintf(0, stdout, "\trange[%ld] = %ld\n", temp_idx, range[temp_idx]);
            temp_idx++;
        }
        system_fprintf(0, stdout, "\tmax range = %ld\n", max_range);

        offset[0] = 0;
        system_fprintf(0, stdout, "\toffset[%ld] = %ld\n", 0, offset[0]);
        for(int i = 1; i < args_.machine_count; i++) {
            offset[i] = ((max_range * i) - 1) - range[i - 1];
            system_fprintf(0, stdout, "\toffset[%ld] = %ld\n", i, offset[i]);
        }
        args_.total_vertices = max_range * (args_.machine_count);

        edge_partitioned_->relabelingEdges_byOffset(range, offset, args_.num_threads, args_.max_vid, num_chunks_);
        for(int i = 0; i < args_.machine_count; i++) {
            range[i] = (max_range * (i+1)) - 1;
            system_fprintf(0, stdout, "\trange[%ld] = %ld\n", i, range[i]);
        }
        store_partitionedRanges(range);
        range_file.close();
    }

	//Phase 0
	void create_undirected() {
		ReturnStatus rs;
		turbo_timer timer;

		if(args_.bidirect_mode) {
			//Phase 0-1 : Make Hash partition
			if(args_.machine_num == 0) {
				fprintf(stdout,"BEGIN\tPhase 0-1 : TwoDimRangePartitioning\n");
				timer.start_timer(0);
			}
			TwoDimRangePartitioning();
			PartitionStatistics::wait_for_all();
			system_fprintf(0, stdout,"END\tPhase 0-1 : TwoDimRangePartitioning, Elapsed time : %.2f\n", timer.stop_timer(0));

			//Phase 0-2 : Calculate Local Degree and divide range
			if(args_.machine_num == 0) {
				fprintf(stdout,"BEGIN\tPhase 0-2 : Calculate Local Degree and divide range\n");
				timer.start_timer(0);
			}

			edge_partitioned_ = new PartitionedEdges<FileNodeType, degree_t>(num_chunks_, args_.num_threads, args_.temp_path, args_.temp_path, concurrent_buffer_pool, args_.buffer_size);
			if(!args_.inout_test_mode) {
				rs = edge_partitioned_->calculateLocalDegreeTable(degree_table_, args_.num_threads, edge_cnt, args_.total_vertices, num_chunks_, args_.machine_num);
				PartitionStatistics::wait_for_all();
				if(args_.machine_count > 1) {
					reduceDegreeTable(degree_table_);
				}		
			}
			else
				makeInOutDegree();

			PartitionStatistics::wait_for_all();
			if(args_.machine_count > 1) {
				timer.start_timer(0);
				makeVidRange();
				timer.stop_timer(0);
				system_fprintf(0, stdout, "\tReduce Elapsed time : %f\n", timer.get_timer(0));
			}
			else if(args_.machine_count == 1) {
				range.resize(1);
				range[0] = args_.total_vertices;
			}
			PartitionStatistics::wait_for_all();

			system_fprintf(0, stdout,"END\tPhase 0-2 : Calculate Local Degree and divide range, Elapsed time : %.2f\n", timer.stop_timer(0));

			degree_table_->Close(true);
			delete degree_table_;

			//Phase 0-3 : Make Send partition
			if(args_.machine_num == 0) {
				fprintf(stdout,"BEGIN\tPhase 0-3 : Make Send partition\n");
				timer.start_timer(0);
			}
						
			if(args_.machine_num == 0)
				fprintf(stdout, "\tMake Send Partition; partitioned by range\n");
			//rs = edge_partitioned_->makeSendPartitionbyRange(args_.num_threads, args_.machine_count, args_.buffer_size, args_.temp_path, "send_", args_.total_vertices, edge_cnt, true, range);
			if(rs == FAIL) {
				fprintf(stdout, "[%lld] Something wrong in makeSendPartition", args_.machine_num);
				//throw unsupported_exception();
			}

			concurrent_buffer_pool->Close();
			delete edge_partitioned_;
			delete concurrent_buffer_pool;
			PartitionStatistics::wait_for_all();
			system_fprintf(0, stdout,"END\tPhase 0-3 : Make Send partition, Elapsed time : %.2f\n", timer.stop_timer(0));

			//Phase 0-4 : Redistribute
			if(args_.machine_num == 0) {
				fprintf(stdout,"BEGIN\tPhase 0-4 : Redistribute\n");
				timer.start_timer(0);
			}

			redistribute_edges();
			PartitionStatistics::wait_for_all();

			system_fprintf(0, stdout,"END\tPhase 0-4 : Redistribute, Elapsed time : %.2f\n", timer.stop_timer(0));

			//Phase 0-5 : Sort, Merge
			if(args_.machine_num == 0) {
				fprintf(stdout,"BEGIN\tPhase 0-5 : Sort and Merge\n");
				timer.start_timer(0);
			}
			
			system_fprintf(0, stdout,"\tCreate Sorted Run\n");
			create_sorted_run();

			system_fprintf(0, stdout,"\tMerge Sorted Run\n");
			merge_sorted_runs();
			PartitionStatistics::wait_for_all();

			if (args_.sort_validation) {
				system_fprintf(0, stdout, "\tTest validity of External Sort\n");
				validate_external_sort();

			}
			PartitionStatistics::wait_for_all();
			system_fprintf(0, stdout,"END\tPhase 0-5 : Sort and Merge, Elapsed time : %.2f\n", timer.stop_timer(0));

		}
		rename((args_.destination_path + "/tbgpp_" + std::to_string(args_.machine_num)).c_str(), (args_.destination_path + "/" + std::to_string(args_.machine_num)).c_str());
		args_.source_path = temp_output_path_;

	}

	//Phase 1 Range Partitioning for memory limit
	void create_edge_colony() {
		if(args_.bidirect_mode) {
			edgefile_list.clear();
			args_.input_type = InputFileType::BinaryVertexPair;
			args_.random_mode = 0;
			fprintf(stdout, "[%lld] Make new concurrent buffer pool\n", PartitionStatistics::my_machine_id());
			concurrent_buffer_pool = new FixedSizeConcurrentBufferPool();
			concurrent_buffer_pool->Initialize(args_.buffer_size * buffer_num_, args_.buffer_size);
			init_edge_list_and_reader();
            PartitionStatistics::wait_for_all();
		}

		bool temp_mode = args_.bidirect_mode;
		args_.bidirect_mode = false;
		TwoDimRangePartitioning();
		args_.bidirect_mode = temp_mode;
	}
	
    //Phase 2 - Graph Renumbering
	void calc_degree_table() {
        //*** BBP, Random, Simple-Hash, GraphX-Hash ***//
        if(args_.partitioning_scheme != 1) {
            ReturnStatus rs;
            turbo_timer timer;
            int64_t temp_buffer_size = args_.buffer_size;

            args_.buffer_size = 576 * 1024 * 1024L; //TODO
            //args_.buffer_size = 144 * 1024 * 1024L;
            concurrent_buffer_pool->Close();
            delete concurrent_buffer_pool;
            concurrent_buffer_pool = new FixedSizeConcurrentBufferPool();
            concurrent_buffer_pool->Initialize(3456 * 1024 * 1024L, args_.buffer_size); //TODO
            LocalStatistics::register_mem_alloc_info("ConcurrentBufferPool", 0);
            LocalStatistics::register_mem_alloc_info("ConcurrentBufferPool", 3456);
            UserArguments::NUM_THREADS = args_.num_threads;

            edge_partitioned_ = new PartitionedEdges<FileNodeType, degree_t>(num_chunks_, args_.num_threads, args_.temp_path, args_.temp_path, concurrent_buffer_pool, args_.buffer_size);

            GraphRenumbering();
            concurrent_buffer_pool->Close();
            delete concurrent_buffer_pool;

            args_.buffer_size = temp_buffer_size;
            concurrent_buffer_pool = new FixedSizeConcurrentBufferPool();
            concurrent_buffer_pool->Initialize(args_.buffer_size * buffer_num_, args_.buffer_size);
            LocalStatistics::register_mem_alloc_info("ConcurrentBufferPool", 0);
            LocalStatistics::register_mem_alloc_info("ConcurrentBufferPool", args_.buffer_size * buffer_num_ / (1024*1024));
        } else { //*** Equi-range partitioning by given partitioning range file (can be used for gemini partitioning or simple range partitioning) ***//
			edge_partitioned_ = new PartitionedEdges<FileNodeType, degree_t>(num_chunks_, args_.num_threads, args_.temp_path, args_.temp_path, concurrent_buffer_pool, args_.buffer_size);
            GraphRenumberingByGivenRange();
        }
	}

	//Phase 3
	void calc_new_vid_table() {
	}

	//Phase 4
	void assign_new_vid_hashjoin() {
	}

	//Phase 5
	void rearrange_edge_distribution() {
        ReturnStatus rs;
        makeSendPartition();

        concurrent_buffer_pool->Close();
        delete concurrent_buffer_pool;
        Aio_Helper::async_pool.Close();
        PartitionStatistics::wait_for_all();
	}

	//Phase 6
	void redistribute_edges() {
		//From new_graph resolve defragmentation
		redist_args.machine_num = args_.machine_num;
		redist_args.machine_count = args_.machine_count;
		//XXX
		//redist_args.buffer_size = 16 * 1024 * 1024 / sizeof(InputEdge); //get from parameter
		redist_args.buffer_size = 1024 * 1024; //get from parameter
		redist_args.buffer_count = args_.num_threads;
		redist_args.send_path = args_.temp_path;
		redist_args.recv_path = args_.temp_path;
		redist_args.delete_original = args_.remove_mode;
		redist_args.validation_mode = args_.redist_validation;
		if (UserArguments::BUILD_DB) { //TODO
			system_fprintf(0, stdout, "[%lld] Redistribute and Build Histogram\n", PartitionStatistics::my_machine_id());
			redist_args = GraphRedistributor<FileNodeType>::redistribute_and_build_histogram(redist_args);
		} else {
			system_fprintf(0, stdout, "[%lld] Redistribute\n", PartitionStatistics::my_machine_id());
			redist_args = GraphRedistributor<FileNodeType>::redistribute(redist_args);
		}
		PartitionStatistics::wait_for_all();
	}

	// Phase 7-1
	void create_sorted_run() {
		/*std::vector<std::string> recv_list;
		for (int i = 0; i < redist_args.recv_count; i++)
			recv_list.push_back(args_.temp_path + "/recv_" + std::to_string(i));*/
		sort_args.buffer_count = 2;
		//XXX
		sort_args.buffer_size = args_.mem_size / 2 / sizeof(InputEdge);
		sort_args.out_file_path = args_.destination_path;
		sort_args.subchunk_vid_range_path = args_.subchunk_vid_range_path;
		sort_args.temp_path = args_.temp_path;
		sort_args.delete_original = args_.remove_mode;
		sort_args.machine_num = args_.machine_num;
		sort_args.validation_mode = args_.sort_validation;
        sort_args.block_count = redist_args.recv_count;
		//sort_args = ExternalSort<FileNodeType, InputNodeType>::sorted_run(recv_list, sort_args);
		PartitionStatistics::wait_for_all();
	}

	// Phase 7-2
	void merge_sorted_runs() {
		if (UserArguments::BUILD_DB) { //TODO
			system_fprintf(0, stdout, "[%lld] Build DB\n", PartitionStatistics::my_machine_id());
			sort_args = ExternalSort<FileNodeType, InputNodeType>::merge_and_builddb(sort_args);
		}
		else {
			system_fprintf(0, stdout, "[%lld] Merge\n", PartitionStatistics::my_machine_id());
			sort_args = ExternalSort<FileNodeType, InputNodeType>::merge(sort_args);
		}
		PartitionStatistics::wait_for_all();
	}

	// Phase 7-3
	void validate_external_sort() {
		//ExternalSort<FileNodeType, InputNodeType>::validate(sort_args);
		//PartitionStatistics::wait_for_all();
	}

	turbo_timer tmr;
	int timer_id;
	void begin_time_log(std::string memo = "") {
		system_fprintf(0, stdout,"BEGIN:\t%s\n", memo.c_str());
		tmr.reset_timer(++timer_id);
		tmr.start_timer(timer_id);
	}
	void time_log(std::string memo = "") {
		if(args_.machine_num == 0)
			fprintf(stdout,"PROCESS:\t%s\tElapsed : %.2f\n", memo.c_str(), tmr.stop_timer(timer_id));
		else
			tmr.stop_timer(timer_id);
		tmr.start_timer(timer_id);
	}
	void end_time_log(std::string memo = "") {
		tmr.stop_timer(timer_id);
		system_fprintf(0, stdout,"END:\t%s\tElapsed : %.2f\n", memo.c_str(), tmr.get_timer(timer_id));
		timer_id--;
	}
};

std::vector<std::vector<int64_t>> GraphPreprocessor<node_t, degree_t, file_t>::local_histogram;
std::vector<std::vector<int64_t>> GraphPreprocessor<node_t, degree_t, file_t>::SubchunkVidRangePerEdgeChunk;


