#pragma once
#ifndef TG_ADJ_WINDOW_H
#define TG_ADJ_WINDOW_H

/*
 * Design of the TG_AdjWindow
 *
 * This class implements an adjacency list window used for the processing model 
 * of the TurboGraph++, called the nested windowed streaming model (NWSM). The 
 * adjacency list window is defined based on memory size in order to allocate a 
 * fixed sized memory space for a window. The adjacency list stream data are 
 * inserted into the size-based window until memory size reaches its maximum 
 * limit. That is, we divide the adjacency list stream into disjoint windows 
 * so that each window can be safely loaded in memory.
 *
 * NWSM chooses the scatter-gather-apply (GAS) model among the existing
 * programming model due to its popularity and extends it to support the k-walk 
 * neighborhood query. TG_AdjWindow class supports scatter computation following 
 * the execution semantics of NWSM. The following is an explanation of scatter:
 *     - With the given vertex window, we stream the adjacency list stream over 
 *       the n disjoint windows. We load subgraph that include the vertices in 
 *       vertex window and their adjacency lists in adjacency list window. Then, 
 *       we execute the user-defined adj_scatter function. This process is done 
 *       by several functions including the name RunScatterGather.
 *     - When processing the adjacency lists in the stream, the two types of 
 *       adjacency list iterating modes can be used: a partial and full adjacency 
 *       list mode. In the partial adjacency list mode, the adjacency list 
 *       includes a subset of adjacent neighbors of each vertex (Please refer 
 *       RunLocalScatterGather and LaunchTask functions). It can be used when 
 *       the computation unit is an edge such as PageRank and SSSP. In the full 
 *       adjacency list mode, the adjacency list includes all adjacent neighbors 
 *       of each vertex (Please refer RunScatterGather, IssueIoForMatAdjLists, 
 *       and ReceiveMatAdjLists functions). It must be supported to efficiently 
 *       process subgraph matching queries such as triangle counting, where fast 
 *       intersection of adjacency lists is necessary.
 *
 * In the two adjacency list iterating modes, loading subgraphs and processing 
 * user-defined scatter are handled in slightly different ways. The following is 
 * an explanation of this:
 *     - Partial adjacency list mode: 
 *       The loading process is done by IdentifyAndCountInputVerticesAndPages() 
 *       function and RunPrefetchingDiskIoForLocalScatterGather() function. The
 *       IdentifyAndCountInputVerticesAndPages() is processed in the following
 *       order:
 *           1) GroupNodeListsIntoPageLists: It receives the ID bitmap of the
 *           vertices to be processed and returns the ID bitmap of the pages
 *           (input_page_bitmap_) that have the adjacency list of the vertices.
 *           2) PrepinEdgePages: It prepins pages that are already in the buffer 
 *           pool among the pages to be processed. The prepinned pages are managed
 *           separately using hit_page_bitmap_.
 *           3) CountNumPagesToProcess: It counts the number of pages to be
 *           processed for each edge chunk. It is used to check whether all pages 
 *           marked in input_page_bitmap_ have been processed.
 *           4) InitializeTaskDataStructures: It initializes the various context
 *           information for each edge subchunk to be processed.
 *       In the RunPrefetchingDiskIoForLocalScatterGather() function, it issues
 *       prefetching pages to be read in the future. 
 *     - Full adjacency list mode:
 *       In this mode, the loading process is done by IssueIoForMatAdjLists()
 *       function and scatter process is done by callback of the function. The
 *       detailed process is as follows:
 *           1) Perform cache maintenance process for already materialized full 
 *           adjacency list. After that, perform user-defined scatter function
 *           for cached full adjacency list.
 *           2) Send I/O request to the machine that owns requested adjacency
 *           list.
 *           3) Spawn a thread for receiving materialized adjacency list through
 *           network (The thread will perform ReceiveMatAdjLists function).
 *           4) The user-defined scatter function is performed as a callback for 
 *           receiving the materialized adjacency list through the network.
 */

#include "util.hpp"

#include "TG_NWSM_Utility.hpp"
#include "TG_NWSMTaskContext.hpp"
#include "MaterializedAdjacencyLists.hpp"

#include "MemoryAllocator.hpp"
#include "VariableSizedMemoryAllocatorWithCircularBuffer.hpp"
#include "BlockMemoryStackAllocator.hpp"

#include "disk_aio_factory.hpp"
#include "Turbo_bin_aio_handler.hpp"
#include "TwoLevelBitMap.hpp"
#include "TurboDB.hpp"

#define MIN_WORKING_PAGE_SET_SIZE 64

void TG_NWSM_CallbackTask(diskaio::DiskAioRequestUserInfo &turbo_callback);

class TurbographImplementation;

template <typename UserProgram_t>
class TG_AdjWindow : public TG_NWSMCallback {
  public:
	typedef TG_AdjacencyMatrixPageIterator AdjList_Iterator_t;
	typedef TG_AdjacencyListIterator  NbrList_Iterator_t;
	typedef TG_NWSMTaskContext task_ctxt_t;
	//typedef tbb::concurrent_queue<PageID> task_page_queue_t;
	typedef moodycamel::ConcurrentQueue<PageID> task_page_queue_t;
	typedef tbb::concurrent_queue<VerIDPageIDPair> task_version_page_queue_t;
    typedef void (UserProgram_t::*SubQueryFunc)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]);

	std::atomic<int64_t> adjlist_io_bytes_edges;
	std::atomic<int64_t> adjlist_io_bytes_etc;
	std::atomic<int64_t> adjlist_io_bytes_edges_subqueries[10];
	std::atomic<int64_t> adjlist_io_bytes_edges_cached;
	std::atomic<int64_t> adjlist_io_bytes_etc_cached;
  private:

    int lv_;
    Range<int> version_range_;
    UserProgram_t* user_program_;
	MemoryAllocator* adj_window_memory_allocator;
    std::vector<SubQueryFunc> SubQueries;
    std::vector<std::vector<SubQueryFunc>> PruningSubQueries;
    std::vector<std::vector<bool>> is_exist_pruning_subqueries;
    std::vector<TwoLevelBitMap<node_t>*> dq_nodes;
    std::vector<bool> is_dq_nodes_empty_at_prev_ss;
    std::vector<bool> use_Vdv_or_Va;
    std::vector<std::vector<bool>> start_from_VdE;

	void (UserProgram_t::*AdjScatter)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]);
	void (UserProgram_t::*AdjScatter_PULL)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]);
	void (TurbographImplementation::*IncAdjScatter)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]);
		
    std::queue<std::future<void>> tasks_to_wait;

	Range<node_t>  input_node_vid_range_;
	TwoLevelBitMap<node_t> delta_nwsm_input_node_bitmap_[2];		  // |V|/pq/8
	TwoLevelBitMap<node_t> input_node_bitmap_;		  // |V|/pq/8
	TwoLevelBitMap<node_t> full_input_node_bitmap_;	 // |V|/8
	TwoLevelBitMap<node_t>* per_machine_input_node_bitmap_;	 // |V|/8

	// PARTIAL LIST ITERATOR MODE
	TwoLevelBitMap<PageID> input_page_bitmap_;
	TwoLevelBitMap<PageID> hit_page_bitmap_;

	PageID cur_begin_page_id_;
	PageID cur_end_page_id_;

	// FULL LIST ITERATOR MODE
	Range<node_t> vid_range_to_process;
	Range<node_t> vid_range_being_processed;  // (input_node_bitmap) & (vid_range_being_processed)
	Range<node_t> vid_range_processed;
	TwoLevelBitMap<node_t> current_vid_window;		  // |V|/pq/8
	TwoLevelBitMap<node_t> current_vid_window_being_processed;		  // |V|/pq/8
	BitMap<node_t> fulllist_latest_flags;		  // |V|/pq/8

	// LocalScatterGather
	TG_Vector<TG_Vector<TG_Vector<TG_Vector<PageID>>>> num_pages_to_process_per_subchunk; //...[db_type][version][pq][pqr]
	TG_Vector<TG_Vector<TG_Vector<TG_Vector<Range<PageID>>>>> block_ranges_per_subchunk;
	task_ctxt_t*** taskid_to_ctxt;
	task_page_queue_t*** taskid_to_work_queue;
	task_page_queue_t*** taskid_to_processed_page_queue;

	TG_Vector<int64_t> taskid_to_num_processed_version;
	char threadid_to_taskid_map[MAX_NUM_CPU_CORES];
	int64_t* taskid_to_overflow_cnt;
	task_page_queue_t* processed_page_queue;

	int iters;

	turbo_timer local_scatter_gather_timer;
	turbo_timer per_thread_lsg_timer[32];

  public:
	turbo_timer per_thread_subquery_timer[32][10];

	TG_AdjWindow() {
		input_node_vid_range_.Set (-1, -1);
		vid_range_to_process.Set (-1, -1);
		vid_range_processed.Set (-1, -1);

		user_program_ = NULL;
		AdjScatter = NULL;
		AdjScatter_PULL = NULL;
		IncAdjScatter = NULL;
		adj_window_memory_allocator = NULL;
        per_machine_input_node_bitmap_ = NULL;

		adjlist_io_bytes_edges.store(0);
		adjlist_io_bytes_etc.store(0);
        for (int i = 0; i < 10; i++)
            adjlist_io_bytes_edges_subqueries[i].store(0);
		adjlist_io_bytes_edges_cached.store(0);
		adjlist_io_bytes_etc_cached.store(0);
	
        taskid_to_ctxt = NULL;
        taskid_to_work_queue = NULL;
        taskid_to_processed_page_queue = NULL;
        taskid_to_overflow_cnt = NULL;
        
        per_machine_input_node_bitmap_ = NULL;
    }

    void SetWindowFinalized(bool f) {
		if (lv_ + 1 == UserArguments::MAX_LEVEL) return;
        ALWAYS_ASSERT(lv_ == 0);
        MaterializedAdjacencyLists::VidToAdjTable.SetWindowFinalized(f, lv_);
    }

    void ReleaseEdgeWindow (EdgeType e_type);
	void Close(EdgeType e_type=OUTEDGE);

    ~TG_AdjWindow();

    double GetLocalScatterExecutionTime(bool reset_timers) {
        double exec_time = local_scatter_gather_timer.get_timer(1);
        if (reset_timers) {
            local_scatter_gather_timer.clear_all_timers();
        }
        return exec_time;
    }

	void PrintBreakDownTimer(bool reset_timers) {
		//return; // XXX - syko

#ifdef PRINT_PROFILING_TIMERS
        fprintf(stdout, "[%ld] [lv:%ld/%ld, %ld] LocalScatterGather Time Breakdown\t%.4f\t%.4f\t%.4f\t%.4f\n", (int64_t) PartitionStatistics::my_machine_id(), (int64_t) lv_, (int64_t) UserArguments::MAX_LEVEL, (int64_t) UserArguments::INC_STEP, local_scatter_gather_timer.get_timer(0), local_scatter_gather_timer.get_timer(1), local_scatter_gather_timer.get_timer(2), local_scatter_gather_timer.get_timer(3));
        std::string to_print = "[" + std::to_string(PartitionStatistics::my_machine_id()) + "] LSG Time Breakdown: \n";
        for (int i = 0; i < UserArguments::NUM_THREADS; i++)
            to_print += (std::string("\t tid") + std::to_string(i) + std::string(": ") + std::to_string(per_thread_lsg_timer[i].get_timer(0)) + ", " + std::to_string(per_thread_lsg_timer[i].get_timer(1)) + ", " + std::to_string(per_thread_lsg_timer[i].get_timer(0) - per_thread_lsg_timer[i].get_timer(1)) + "\n");
        system_fprintf(0, stdout, "%s", to_print.c_str());
        to_print = "[" + std::to_string(PartitionStatistics::my_machine_id()) + "] Subquery Time Breakdown: \n";
        for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
            to_print += (std::string("\t tid") + std::to_string(i)); 
            for (int j = 0; j < SubQueries.size(); j++) {
                to_print += (std::string(", Subquery ") + std::to_string(j) + std::string(": ") + std::to_string(per_thread_subquery_timer[i][j].get_timer(0)) + ", ");
            }
        }
        system_fprintf(0, stdout, "%s\n", to_print.c_str());
#endif
    
        if (reset_timers) {
            local_scatter_gather_timer.clear_all_timers();
            for (int i = 0; i < UserArguments::NUM_THREADS; i++)
                per_thread_lsg_timer[i].clear_all_timers();
        }
    }

	MemoryAllocator* GetMemoryAllocator() {
		return adj_window_memory_allocator;
	}

	void InitializeAdjWindowMemoryAllocator(char* ptr, int64_t capacity) {
		if (lv_ + 1 == UserArguments::MAX_LEVEL) {
            // Last Level, we use circular stream buffer
			adj_window_memory_allocator = new VariableSizedMemoryAllocatorWithCircularBuffer();
		} else {
			// Otherwise, we use stack buffer
			adj_window_memory_allocator = new BlockMemoryStackAllocator();
		}
		adj_window_memory_allocator->Initialize(ptr, capacity);
	}

    void InitializeTaskDataStructures(Range<int64_t> src_vector_chunks, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);

	void IdentifyAndCountInputVerticesAndPages(Range<int64_t> src_vector_chunks, Range<int64_t> dst_vector_chunks, Range<node_t> vid_range, PageID MaxNumPagesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
		INVARIANT(src_vector_chunks.length() == 1);

		turbo_timer tim;

		Range<int64_t> src_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(src_vector_chunks);
		Range<int64_t> dst_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(dst_vector_chunks);
		if (UserArguments::USE_FULLIST_DB) {
			dst_edge_subchunks.Set(0, 0);
		}
		tim.start_timer(0);

		tim.start_timer(1);
		GroupNodeListsIntoPageLists(dst_edge_subchunks, vid_range, MaxNumPagesToPin, vid_range_being_processed, version_range, e_type, d_type);
		tim.stop_timer(1);

		tim.start_timer(2);
		PrePinEdgePages(src_vector_chunks, dst_edge_subchunks, version_range, e_type, d_type);
		tim.stop_timer(2);

		tim.start_timer(3);
		CountNumPagesToProcess(src_vector_chunks, dst_edge_subchunks, version_range, e_type, d_type);
		tim.stop_timer(3);

		tim.start_timer(4);
		InitializeTaskDataStructures(src_vector_chunks, version_range, e_type, d_type);
		tim.stop_timer(4);

		tim.stop_timer(0);
		fprintf(stdout, "[%ld] (%ld, %ld, %d) IdentifyAndCount %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3), tim.get_timer(4));
#ifdef PRINT_PROFILING_TIMERS
		fprintf(stdout, "[%ld] (%ld, %ld, %d) IdentifyAndCount %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3), tim.get_timer(4));
#endif
		fflush(stdout);
	}

	void EmptyProcessedPageQueue(EdgeType e_type, DynamicDBType d_type);

	ReturnStatus PrepareForLocalScatterGather(Range<int64_t> src_vector_chunks, Range<int64_t> dst_vector_chunks, Range<node_t> vid_range, PageID MaxNumPagesToPin, bool NoEvict) {
		if (vid_range_to_process.GetBegin() == -1) {
			ALWAYS_ASSERT (vid_range_being_processed.GetBegin() == -1);
			ALWAYS_ASSERT (vid_range_being_processed.GetEnd() == -1);
			
			vid_range_to_process = vid_range;
			vid_range_processed.Set(vid_range_to_process.GetBegin(), vid_range_to_process.GetBegin() - 1);
		}

		if (vid_range_processed == vid_range_to_process) {
			vid_range_to_process.Set (-1, -1);
			vid_range_processed.Set (-1, -1);
			vid_range_being_processed.Set(-1, -1);
			return DONE;
		}

		Range<PageID> cur_block_range(-1, -1);
		Range<int64_t> src_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(src_vector_chunks);
		Range<int64_t> dst_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(dst_vector_chunks);

		if (UserArguments::MAX_LEVEL == 1) {
			vid_range_being_processed = vid_range_to_process;
			vid_range_processed.SetEnd(vid_range_being_processed.GetEnd());
			return ON_GOING;
		} else {
			abort(); // syko, tslee: 2018/10/17
			return DONE;
        }
	}
	
    // For 'ProcessNestedWindowedStreamFullList'
    // with MaxLevel > 1 && lv == MaxLevel - 1
	virtual ReturnStatus RunScatterGatherAtLastLevel(Range<int64_t> src_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<node_t> vid_range, int64_t MaxNumBytesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type) {
		ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == FULL_LIST);
		ALWAYS_ASSERT (src_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS == src_vector_chunks.GetEnd() / UserArguments::VECTOR_PARTITIONS);
		ALWAYS_ASSERT (lv_ == UserArguments::MAX_LEVEL - 1);
		ALWAYS_ASSERT (lv_ > 0);

		turbo_timer fulllist_scatter_gather_timer;
		fulllist_scatter_gather_timer.start_timer(0);

		input_node_vid_range_ = vid_range;
		SetWindowFinalized(false);
		
		int src_machine = src_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;

		//fprintf(stdout, "\tBEGIN [%ld] %ld [lv:%ld/%ld] [src:%d] [RunScatterGatherAtLastLevel %p]. vid_range = [%ld, %ld], _to_process [%ld, %ld], _processed [%ld, %ld], _being_processed [%ld, %ld]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv_, UserArguments::MAX_LEVEL, src_machine, this, vid_range.GetBegin(), vid_range.GetEnd(), vid_range_to_process.GetBegin(), vid_range_to_process.GetEnd(), vid_range_processed.GetBegin(), vid_range_processed.GetEnd(), vid_range_being_processed.GetBegin(), vid_range_being_processed.GetEnd());

		fulllist_scatter_gather_timer.start_timer(1);
		if (vid_range_to_process.GetBegin() == -1) {
			vid_range_to_process = vid_range;
			vid_range_processed.Set(vid_range_to_process.GetBegin(), vid_range_to_process.GetBegin() - 1);
			if (UserArguments::ITERATOR_MODE == FULL_LIST) {
				input_node_bitmap_.CopyFrom(per_machine_input_node_bitmap_[src_machine]);
			}
			fulllist_scatter_gather_timer.stop_timer(1);

			// Mark the candidates & Identify the vertices that will be included in the next sliding window
			vid_range_being_processed.Set(vid_range_processed.GetEnd() + 1, vid_range_to_process.GetEnd());
		}

		if (vid_range_processed == vid_range_to_process) {
			vid_range_to_process.Set (-1, -1);
			vid_range_processed.Set (-1, -1);
			vid_range_being_processed.Set (-1, -1);
			SetWindowFinalized(true);
			return DONE;
		}

		// Issue IO with Callback
		fulllist_scatter_gather_timer.start_timer(2);
		ALWAYS_ASSERT (vid_range_to_process.contains(vid_range_being_processed));
		Range<int64_t> dst_vector_chunks(0, PartitionStatistics::num_target_vector_chunks() - 1);
		tasks_to_wait.push(RequestRespond::rr_.async_req_pool.enqueue(TG_AdjWindow<UserProgram_t>::IssueIoForMatAdjLists, this, src_machine, dst_vector_chunks, vid_range_to_process, &per_machine_input_node_bitmap_[src_machine], vid_range_being_processed, NoEvict, version_range, e_type, ALL, false));
		while (tasks_to_wait.size() > 1) {
			tasks_to_wait.front().wait();
			tasks_to_wait.pop();
		}
		WaitAllTasks();
		fulllist_scatter_gather_timer.stop_timer(2);

		node_t end_vid_processed = per_machine_input_node_bitmap_[src_machine].FindFirstMarkedEntry(vid_range_being_processed.GetBegin() - vid_range_to_process.GetBegin(), vid_range_being_processed.GetEnd() - vid_range_to_process.GetBegin());
		if (end_vid_processed == -1) end_vid_processed = vid_range_being_processed.GetEnd();
		else end_vid_processed += vid_range_to_process.GetBegin() - 1;
		vid_range_processed.SetEnd(end_vid_processed);
		SetWindowFinalized(true);

		fulllist_scatter_gather_timer.stop_timer(0);
		
		//fprintf(stdout, "\tEND [%ld] %ld [lv:%ld/%ld] [src:%d] [RunScatterGatherAtLastLevel %p]. vid_range = [%ld, %ld], _to_process [%ld, %ld], _processed [%ld, %ld], _being_processed [%ld, %ld]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv_, UserArguments::MAX_LEVEL, src_machine, this, vid_range.GetBegin(), vid_range.GetEnd(), vid_range_to_process.GetBegin(), vid_range_to_process.GetEnd(), vid_range_processed.GetBegin(), vid_range_processed.GetEnd(), vid_range_being_processed.GetBegin(), vid_range_being_processed.GetEnd());
#ifdef REPORT_PROFILING_TIMERS
		fprintf(stdout, "[LV-%ld\tRUN_SCATTER_GATHER: %ld]\t%.2f\t%.2f\t%.2f\t%.2f\n"
		        , lv_ + 1
		        , PartitionStatistics::my_machine_id()
		        , fulllist_scatter_gather_timer.get_timer(0)
		        , fulllist_scatter_gather_timer.get_timer(1)
		        , fulllist_scatter_gather_timer.get_timer(2)
		        , fulllist_scatter_gather_timer.get_timer(3));
#endif
        
        return ON_GOING;
    }
	
    virtual ReturnStatus RunScatterGather(Range<int64_t> src_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<node_t> vid_range, int64_t MaxNumBytesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type) {
        if (lv_ < UserArguments::MAX_LEVEL - 1) {
            RunScatterGatherAtInnerLevel(src_vector_chunks, next_src_vector_chunks, vid_range, MaxNumBytesToPin, NoEvict, version_range, e_type);
        } else {
            RunScatterGatherAtLastLevel(src_vector_chunks, next_src_vector_chunks, vid_range, MaxNumBytesToPin, NoEvict, version_range, e_type);
        }
    }

	// For 'ProcessNestedWindowedStreamFullList'
	virtual ReturnStatus RunScatterGatherAtInnerLevel(Range<int64_t> src_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<node_t> vid_range, int64_t MaxNumBytesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type) {
		ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == FULL_LIST);
		ALWAYS_ASSERT (src_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS == src_vector_chunks.GetEnd() / UserArguments::VECTOR_PARTITIONS);
        ALWAYS_ASSERT (lv_ < UserArguments::MAX_LEVEL - 1);

        turbo_timer fulllist_scatter_gather_timer;
        fulllist_scatter_gather_timer.start_timer(0);
		
        input_node_vid_range_ = vid_range;
        SetWindowFinalized(false);
	
        int src_machine = src_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;

        fulllist_scatter_gather_timer.start_timer(1);
		// If this is the first window, initialize
        bool perform_cache_maintenance = false; 
        if (vid_range_to_process.GetBegin() == -1) {
            perform_cache_maintenance = true;
			ALWAYS_ASSERT (vid_range_being_processed.GetBegin() == -1);
			ALWAYS_ASSERT (vid_range_being_processed.GetEnd() == -1);
			vid_range_to_process = vid_range;
			vid_range_processed.Set(vid_range_to_process.GetBegin(), vid_range_to_process.GetBegin() - 1);
			if (UserArguments::ITERATOR_MODE == FULL_LIST) {
                //input_node_bitmap_.CopyFrom(per_machine_input_node_bitmap_[src_machine]);
                //per_machine_input_node_bitmap_[src_machine].ClearAll();
            }
			current_vid_window.CopyFrom(input_node_bitmap_);
		    fulllist_latest_flags.ClearAll();
        }
        fulllist_scatter_gather_timer.stop_timer(1);
		
        //fprintf(stdout, "BEGIN [%ld] %ld [lv:%ld/%ld] [current_vw: %ld] [RunScatterGatherAtInnerLevel %p]. vid_range = [%ld, %ld], _to_process [%ld, %ld], _processed [%ld, %ld], _being_processed [%ld, %ld]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv_, UserArguments::MAX_LEVEL, 0, this, vid_range.GetBegin(), vid_range.GetEnd(), vid_range_to_process.GetBegin(), vid_range_to_process.GetEnd(), vid_range_processed.GetBegin(), vid_range_processed.GetEnd(), vid_range_being_processed.GetBegin(), vid_range_being_processed.GetEnd());

		if (vid_range_processed == vid_range_to_process) {
			ALWAYS_ASSERT (current_vid_window.GetTotalInRangeSingleThread(0, vid_range_to_process.length() - 1) == 0);
			this->vid_range_to_process.Set(-1, -1);
			this->vid_range_processed.Set(-1, -1);
			this->vid_range_being_processed.Set(-1, -1);
            
            SetWindowFinalized(true);
			return DONE;
		}

		ALWAYS_ASSERT (src_vector_chunks.GetBegin() == src_vector_chunks.GetEnd());
		PartitionID cur_src_vector_chunk_id = src_vector_chunks.GetBegin();
		PartitionID next_src_vector_chunk_id = next_src_vector_chunks.GetBegin();

		// Identify the vertices that will be included in the next sliding window
		vid_range_being_processed.Set(vid_range_processed.GetEnd() + 1, vid_range_to_process.GetEnd());

		// Process Vertices with vid in 'vid_range_being_processed'
        fulllist_scatter_gather_timer.start_timer(2);
		ALWAYS_ASSERT (vid_range_to_process.contains(vid_range_being_processed));
		Range<int64_t> dst_vector_chunks(0, PartitionStatistics::num_target_vector_chunks() - 1);
		IssueIoForMatAdjLists(this, src_machine, dst_vector_chunks, vid_range_to_process, &current_vid_window, vid_range_being_processed, NoEvict, version_range, e_type, ALL, perform_cache_maintenance);
        fulllist_scatter_gather_timer.stop_timer(2);

		// Update vid_range_processed
        fulllist_scatter_gather_timer.start_timer(3);
		node_t end_vid_processed = current_vid_window.FindFirstMarkedEntry(vid_range_being_processed.GetBegin() - vid_range_to_process.GetBegin(), vid_range_being_processed.GetEnd() - vid_range_to_process.GetBegin());
        fulllist_scatter_gather_timer.stop_timer(3);

		if (end_vid_processed == -1) end_vid_processed = vid_range_being_processed.GetEnd();
		else end_vid_processed += vid_range_to_process.GetBegin() - 1;
		

		INVARIANT (end_vid_processed == vid_range_to_process.GetBegin() - 1 || vid_range_to_process.contains(end_vid_processed));
		INVARIANT (end_vid_processed == vid_range_to_process.GetBegin() - 1 || vid_range_to_process.GetBegin() <= end_vid_processed);

		vid_range_processed.SetEnd(end_vid_processed);
        SetWindowFinalized(true);
        fulllist_scatter_gather_timer.stop_timer(0);
        
        //fprintf(stdout, "END [%ld] %ld [lv:%ld/%ld] [current_vw: %ld] [RunScatterGatherAtInnerLevel]. vid_range_processed [%ld, %ld], vid_range_being_processed [%ld, %ld]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv_, UserArguments::MAX_LEVEL, 0, vid_range_processed.GetBegin(), vid_range_processed.GetEnd(), vid_range_being_processed.GetBegin(), vid_range_being_processed.GetEnd());
		
#ifdef PRINT_PROFILING_TIMERS
		fprintf(stdout, "[LV-%ld\tRUN_SCATTER_GATHER: %ld]\t%.2f\t%.2f\t%.2f\t%.2f\n"
		        , lv_ + 1
		        , PartitionStatistics::my_machine_id()
		        , fulllist_scatter_gather_timer.get_timer(0)
		        , fulllist_scatter_gather_timer.get_timer(1)
		        , fulllist_scatter_gather_timer.get_timer(2)
		        , fulllist_scatter_gather_timer.get_timer(3));
#endif
        
        
        return ON_GOING;
	}

	bool HasVertexInWindow(node_t vid) {
		ALWAYS_ASSERT(lv_ >= 0 && lv_ < UserArguments::MAX_LEVEL - 1);
		return (vid_range_to_process.contains(vid) && current_vid_window_being_processed.Get(vid - vid_range_to_process.GetBegin()));
	}
	
    bool HasVertexInWindow(int subquery_idx, node_t vid) {
		ALWAYS_ASSERT(lv_ >= 0 && lv_ < UserArguments::MAX_LEVEL - 1);
		return (vid_range_to_process.contains(vid) && dq_nodes[subquery_idx-1]->Get(vid - vid_range_to_process.GetBegin()));
	}

    static void PreprocessAdjScatterCallbackOfCachedMatAdj(TG_AdjWindow<UserProgram_t>* obj, void* ptr_mat_adj, int lv, bool current, bool delta) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) ptr_mat_adj;
        if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
            mat_adj->ConvertVidToDegreeOrderAndSortOfCachedMatAdj(&obj->fulllist_latest_flags, obj->vid_range_to_process.GetBegin(), current, delta);
		    ALWAYS_ASSERT (mat_adj->SanityCheckUsingDegree(true));
		    ALWAYS_ASSERT (mat_adj->SanityCheck());
        }
    }

    static void AdjScatterCallbackOfCachedMatAdj (TG_AdjWindow<UserProgram_t>* obj, void* ptr_mat_adj, int lv, bool NoEvict) {
	    ALWAYS_ASSERT (UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
		ALWAYS_ASSERT (obj != NULL);
		ALWAYS_ASSERT (obj->user_program_ != NULL);
        
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) ptr_mat_adj;
        obj->adjlist_io_bytes_edges_cached.fetch_add(mat_adj->GetNumEdges() * sizeof(node_t));
		obj->adjlist_io_bytes_etc_cached.fetch_add(mat_adj->GetNumAdjacencyLists() * sizeof(Slot32));
    
        Scatter_MatAdj(obj, mat_adj, true, true);
    }

    static void AdjScatterCallbackParallel (TG_AdjWindow<UserProgram_t>* obj, UserCallbackRequest req, SimpleContainer cont, int lv, bool NoEvict, int partition_id=-1) {
        ALWAYS_ASSERT (obj != NULL);
        ALWAYS_ASSERT (obj->user_program_ != NULL);
		
		MaterializedAdjacencyLists* mat_adj = new MaterializedAdjacencyLists();
		mat_adj->Initialize(cont.size_used, cont.capacity, cont.data);
        
        obj->adjlist_io_bytes_edges.fetch_add(mat_adj->GetNumEdges() * sizeof(node_t));
		obj->adjlist_io_bytes_etc.fetch_add(mat_adj->GetNumAdjacencyLists() * sizeof(Slot32));

        turbo_timer tim;
        tim.start_timer(0);
		if (req.parm1 == 0 && UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			mat_adj->ConvertVidToDegreeOrderAndSort();
		}
        tim.stop_timer(0);

#ifndef PERFORMANCE
		for (int64_t adj_idx = 0; adj_idx < mat_adj->GetNumAdjacencyLists(); adj_idx++) {
			node_t src_vid = mat_adj->GetSlotVid(adj_idx);
            ALWAYS_ASSERT (req.vid_range.contains(src_vid));
		}
#endif
		
        if (obj->lv_  == UserArguments::MAX_LEVEL - 1) {
            Scatter_MatAdj(obj, mat_adj, false, false, partition_id);
        } else {
            VidToAdjacencyListMappingTable::VidToAdjTable.AddCurrentBlockSizeInByte(cont.size_used, lv);
            MaterializedAdjacencyLists::VidToAdjTable.Put(mat_adj, lv); // at AdjScatterCallbackParallel
            Scatter_MatAdj(obj, mat_adj, true, true);
        }
        
        if (!NoEvict) {
			ALWAYS_ASSERT (lv == UserArguments::MAX_LEVEL - 1);
			(obj->adj_window_memory_allocator)->Free(cont.data, cont.capacity);
			delete mat_adj;
		}
    }


    // [                    ] - bitmap_vid_range
    //      [               ] - target_vid_range
	static void ReceiveMatAdjLists (TG_AdjWindow<UserProgram_t>* obj, int partition_id, int lv, Range<node_t> bitmap_vid_range, BitMap<node_t>* vids, Range<node_t> target_vid_range, int64_t est_mat_adjlist_size, bool NoEvict) {
        ALWAYS_ASSERT(obj->lv_ == lv);
        
        //fprintf(stdout, "[%ld] [RECEIVE_MAT_AD BEGIN] lv = %ld from %ld bitmap_vid_range[%ld,%ld] target_vid_range[%ld,%ld]\n", PartitionStatistics::my_machine_id(), lv, partition_id, bitmap_vid_range.GetBegin(), bitmap_vid_range.GetEnd(), target_vid_range.GetBegin(), target_vid_range.GetEnd());

		RequestRespond::server_sockets.recv_from_client_lock(partition_id);
	
        int64_t ack = 9999;
        int64_t total_recv_bytes = 0;
		bool run = true;
        int backoff;

		UserCallbackRequest req;
		std::list<std::future<void>> list_reqs_to_wait;
		turbo_timer receive_mat_adj_timer;

		receive_mat_adj_timer.start_timer(0);

        int64_t max_fail_count = 0;
		while (run) {
            int64_t recv_bytes = 0;
            bool retry = false;
			SimpleContainer cont;
			receive_mat_adj_timer.start_timer(1);
			recv_bytes = RequestRespond::server_sockets.recv_from_client((char*) &req, 0, partition_id, sizeof(UserCallbackRequest));
            total_recv_bytes += recv_bytes;
		    if (recv_bytes == -1) {
                //fprintf(stdout, "[%ld] Fail to Receiving1 (%ld -> %ld)\n", PartitionStatistics::my_machine_id(), partition_id, PartitionStatistics::my_machine_id());
                max_fail_count++;
                retry = true;
                goto Retry;
            }
		    //fprintf(stdout, "[%ld] %p Recv REQ from %ld, sz = %ld, req.data_size = %ld\n", PartitionStatistics::my_machine_id(), pthread_self(), partition_id, recv_bytes, req.data_size);
			//INVARIANT (recv_bytes == sizeof(UserCallbackRequest));
			receive_mat_adj_timer.stop_timer(1);

            if (req.data_size == 0) break;
           
            receive_mat_adj_timer.start_timer(2);
			backoff = 1;
            while ((cont.data = (obj->adj_window_memory_allocator)->Allocate(req.data_size)) == NULL) {
				usleep(backoff * 1024);
				backoff= 2 * backoff;
				if (backoff >= 256) {
					backoff = 256;
				}
			}
			cont.capacity = req.data_size;
			ALWAYS_ASSERT (cont.data != NULL);
			receive_mat_adj_timer.stop_timer(2);

			receive_mat_adj_timer.start_timer(3);
			recv_bytes = RequestRespond::server_sockets.recv_from_client(cont.data, 0, partition_id, cont.capacity, false);
            total_recv_bytes += recv_bytes;
			if (recv_bytes != cont.capacity || retry) {
                //fprintf(stdout, "[%ld] Fail to Receiving2 %s %s (%ld -> %ld)\n", PartitionStatistics::my_machine_id(), (recv_bytes != cont.capacity) ? "true" : "false", retry ? "true" : "false", partition_id, PartitionStatistics::my_machine_id());
                (obj->adj_window_memory_allocator)->Free(cont.data, cont.capacity);
		        retry = true;
                max_fail_count++;
                goto Retry;
                //fprintf(stdout, "[%ld] recv_bytes = %ld, cont.capacity = %ld\n", PartitionStatistics::my_machine_id(), recv_bytes, cont.capacity);
            }
			//INVARIANT(recv_bytes == cont.capacity);
			//INVARIANT(recv_bytes == req.data_size);
            if (!retry) {
                cont.size_used = req.data_size;
                //total_recv_bytes += recv_bytes;
            }
		
            /*ack = 9999;
            recv_bytes = RequestRespond::server_sockets.send_to_client((char*) &ack, 0, sizeof(int64_t), partition_id);
		    if (recv_bytes != sizeof(int64_t)) { 
                (obj->adj_window_memory_allocator)->Free(cont.data, cont.capacity);
                goto Retry;
            }
            INVARIANT (recv_bytes == sizeof(int64_t));*/
			receive_mat_adj_timer.stop_timer(3);

Retry:
            if (retry) {
                //fprintf(stdout, "[%ld] Retry at ReceiveMatAdjLists fail_count = %ld\n", PartitionStatistics::my_machine_id(), max_fail_count);
                if (max_fail_count > 10) break;
                else continue;
            } else {
                //fprintf(stdout, "[%ld] lv:%d Success to Receive (%ld -> %ld) [%ld, %ld]\n", PartitionStatistics::my_machine_id(), UserArguments::CURRENT_LEVEL, partition_id, PartitionStatistics::my_machine_id(), req.vid_range.GetBegin(), req.vid_range.GetEnd());
                max_fail_count = 0;
            }
            INVARIANT(!retry);
            list_reqs_to_wait.push_back(RequestRespond::rr_.async_udf_pool.enqueue(TG_AdjWindow<UserProgram_t>::AdjScatterCallbackParallel, obj, req, cont, lv, NoEvict, partition_id));

            receive_mat_adj_timer.start_timer(4);
            while (list_reqs_to_wait.size() > MAT_ADJ_MAX_PENDING_REQS * 2) {
                list_reqs_to_wait.remove_if([&](std::future<void>& wait) { 
                        return wait.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
                        });
            }    
            receive_mat_adj_timer.stop_timer(4);
        }
        receive_mat_adj_timer.start_timer(4);
        while (list_reqs_to_wait.size() > 0) {
            list_reqs_to_wait.remove_if([&](std::future<void>& wait) { 
                    return wait.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
                    });
        }
        receive_mat_adj_timer.stop_timer(4);
        RequestRespond::rr_.async_udf_pool.WaitAll();
		receive_mat_adj_timer.stop_timer(0);
        
//#ifdef REPORT_PROFILING_TIMERS
		fprintf(stdout, "[%ld] %ld [RECEIVE_MAT_ADJ: %ld->%ld, lv:%ld]\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f, %ld [MB], %ld [B], %.2f [MB/sec]\n"
		        , PartitionStatistics::my_machine_id()
                , UserArguments::UPDATE_VERSION
                , req.from
		        , req.to
                , lv
		        , receive_mat_adj_timer.get_timer(0)
		        , receive_mat_adj_timer.get_timer(1)
		        , receive_mat_adj_timer.get_timer(2)
		        , receive_mat_adj_timer.get_timer(3)
		        , receive_mat_adj_timer.get_timer(4)
                , total_recv_bytes / (1024*1024L)
                , total_recv_bytes
                , ((double) total_recv_bytes / (1024*1024L)) / receive_mat_adj_timer.get_timer(0));
//#endif
		RequestRespond::server_sockets.recv_from_client_unlock(partition_id);
        
        INVARIANT (list_reqs_to_wait.size() == 0);
	}

	static void IssueIoForMatAdjLists (TG_AdjWindow<UserProgram_t>* obj, int src_machine, Range<int64_t> dst_vector_chunks, Range<node_t> bitmap_vid_range, TwoLevelBitMap<node_t>* vids, Range<node_t> target_vid_range, bool NoEvict, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, bool perform_cache_maintenance = false) {
        ALWAYS_ASSERT (PartitionStatistics::per_machine_vid_range(src_machine).contains(bitmap_vid_range));
        turbo_timer issue_io_for_mat_adj_timer;
		issue_io_for_mat_adj_timer.start_timer(0);
		issue_io_for_mat_adj_timer.start_timer(1);
		obj->current_vid_window_being_processed.ClearAll();

		// TODO - Assuming that the bitmap size is O(|V|/(# of Machines))
		Range<node_t> src_machine_vid_range = PartitionStatistics::per_machine_vid_range(src_machine);
		ALWAYS_ASSERT (src_machine_vid_range == bitmap_vid_range);

		AdjListRequestBatch req;
		req.rt = AdjListBatchIo;
		req.from = PartitionStatistics::my_machine_id();
		req.to = src_machine;
		req.src_edge_partition_range.Set(src_machine, src_machine);
		req.dst_edge_partition_range = PartitionStatistics::vector_chunks_to_edge_subchunks(dst_vector_chunks);
        req.version_range.Set(version_range.GetBegin(), version_range.GetEnd());
        req.e_type = e_type;
        req.d_type = d_type;
		req.lv = obj->lv_;
        req.MaxBytesToPin = (NoEvict) ? (obj->adj_window_memory_allocator->GetSize() - VidToAdjacencyListMappingTable::ComputeVidToAdjTableSize(bitmap_vid_range)) : std::numeric_limits<int64_t>::max();
        INVARIANT(req.MaxBytesToPin > 0);
        //fprintf(stdout, "[%ld] MaxBytesToPin = NoEvict ? %s, %ld - %ld bitmap_vid_range [%ld, %ld] : %ld, vids total = %ld\n", PartitionStatistics::my_machine_id(), NoEvict ? "true" : "false", obj->adj_window_memory_allocator->GetSize(), VidToAdjacencyListMappingTable::ComputeVidToAdjTableSize(bitmap_vid_range), bitmap_vid_range.GetBegin(), bitmap_vid_range.GetEnd(), std::numeric_limits<int64_t>::max(), vids->GetTotal());
		issue_io_for_mat_adj_timer.stop_timer(1);
   
        if (UserArguments::USE_DELTA_NWSM && UserArguments::UPDATE_VERSION > 0 && obj->lv_ == 0 && target_vid_range.GetBegin() != -1 && target_vid_range.GetBegin() == src_machine_vid_range.GetBegin() && perform_cache_maintenance) {
            moodycamel::ConcurrentQueue<void*> cached_mat_adj;
            std::future<void> wait0;
            TG_Vector<bool> to_keep(bitmap_vid_range.length(), false);
		    
            issue_io_for_mat_adj_timer.start_timer(2);
            int64_t est_mat_adjlist_size = RequestRespond::ComputeMaterializedAdjListsSize(req.dst_edge_partition_range, bitmap_vid_range, vids, req.MaxBytesToPin);
		    issue_io_for_mat_adj_timer.stop_timer(2);
		    
            issue_io_for_mat_adj_timer.start_timer(3);
            MaterializedAdjacencyLists::EvictAsNeededAndPrepareForMergeDelta(&obj->fulllist_latest_flags, bitmap_vid_range, vids, target_vid_range, 0, est_mat_adjlist_size, &to_keep, [&](void* mat_adj) {
                cached_mat_adj.enqueue(mat_adj);
            });
		    issue_io_for_mat_adj_timer.stop_timer(3);
		    
            issue_io_for_mat_adj_timer.start_timer(4);
            MaterializedAdjacencyLists::MergeDeltaWithCachedCurrent(&obj->fulllist_latest_flags, bitmap_vid_range, vids, target_vid_range, 0, req.dst_edge_partition_range, &to_keep, [&](void* mat_adj) {
                PreprocessAdjScatterCallbackOfCachedMatAdj(obj, mat_adj, 0, true, true);
            });
            /*
            wait0 = Aio_Helper::EnqueueAsynchronousTask(MaterializedAdjacencyLists::MergeDeltaWithCachedCurrent, bitmap_vid_range, vids, target_vid_range, 0, req.dst_edge_partition_range, &to_keep, [&](void* mat_adj) {
                PreprocessAdjScatterCallbackOfCachedMatAdj(obj, mat_adj, 0, true, true);
            });
            wait0.wait();
            */
            RequestRespond::rr_.async_udf_pool.WaitAll();
		    issue_io_for_mat_adj_timer.stop_timer(4);
            
		    issue_io_for_mat_adj_timer.start_timer(5);
            void* ptr_mat_adj = nullptr;
            while (cached_mat_adj.try_dequeue(ptr_mat_adj)) {
                RequestRespond::rr_.async_udf_pool.enqueue(TG_AdjWindow<UserProgram_t>::AdjScatterCallbackOfCachedMatAdj, obj, ptr_mat_adj, 0, NoEvict);
            }
            RequestRespond::rr_.async_udf_pool.WaitAll();
            
            req.MaxBytesToPin = (NoEvict) ? (obj->adj_window_memory_allocator->GetRemainingBytes()) : std::numeric_limits<int64_t>::max();
            //fprintf(stdout, "[%ld] MaxBytesToPin = NoEvict ? %s, %ld - %ld bitmap_vid_range [%ld, %ld] : %ld, vids total = %ld\n", PartitionStatistics::my_machine_id(), NoEvict ? "true" : "false", obj->adj_window_memory_allocator->GetRemainingBytes(), VidToAdjacencyListMappingTable::ComputeVidToAdjTableSize(bitmap_vid_range), bitmap_vid_range.GetBegin(), bitmap_vid_range.GetEnd(), std::numeric_limits<int64_t>::max(), vids->GetTotal());
            VidToAdjacencyListMappingTable::VidToAdjTable.Validate(obj->lv_);
		    issue_io_for_mat_adj_timer.stop_timer(5);
        }
        /*
        node_t has_cnts = 0;
        for (node_t vid = target_vid_range.GetBegin(); vid <= target_vid_range.GetEnd(); vid++) {
            if (VidToAdjacencyListMappingTable::VidToAdjTable.Has(vid, 0))
                has_cnts++;
        }

        if (obj->lv_== 0) {
            fprintf(stdout, "[%ld] %ld [IssueIoForMatAdjLists] %lld->%lld, |requested_vertices| = %ld, |HasCnts| = %lld in [%lld, %lld]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, req.from, req.to, vids->GetTotalSingleThread(), has_cnts, target_vid_range.GetBegin(), target_vid_range.GetEnd());
        }
        */
        
        // Send IO Request
		issue_io_for_mat_adj_timer.start_timer(6);
		char* vids_data[2];
        int64_t vids_data_size[2];
        vids_data[0] = vids->data();
        vids_data_size[0] = vids->container_size();
        vids_data[1] = vids->two_level_data();
        vids_data_size[1] = vids->two_level_container_size();
        RequestRespond::SendRequest(&req, 1, vids_data, vids_data_size, 2);
		issue_io_for_mat_adj_timer.stop_timer(6);
        
        
		issue_io_for_mat_adj_timer.start_timer(7);
        int64_t est_mat_adjlist_size = std::numeric_limits<int64_t>::max();
		if (obj->lv_ < UserArguments::MAX_LEVEL - 1) {
            RequestRespond::server_sockets.recv_from_client_lock(req.to);
			int64_t recv_bytes = RequestRespond::server_sockets.recv_from_client((char*) &est_mat_adjlist_size, 0, req.to);
			INVARIANT (recv_bytes == sizeof(int64_t));
            RequestRespond::server_sockets.recv_from_client_unlock(req.to);
		}

        // At the inner level
		if (obj->lv_ < UserArguments::MAX_LEVEL - 1) {
			VidToAdjacencyListMappingTable::VidToAdjTable.Invalidate(UserArguments::CURRENT_LEVEL);
			if (obj->lv_ == 1 - 1) {
		        Range<node_t> src_machine_vid_range = PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id());
                if (VidToAdjacencyListMappingTable::VidToAdjTable.GetMaterializedVidRange(0).GetBegin() == -1 || target_vid_range.GetBegin() != src_machine_vid_range.GetBegin() || obj->adj_window_memory_allocator->GetRemainingBytes() < est_mat_adjlist_size) {
                    MaterializedAdjacencyLists::SlideBlockMemoryWindow(bitmap_vid_range, vids, target_vid_range, obj->lv_, est_mat_adjlist_size);
                }
				INVARIANT (obj->adj_window_memory_allocator->GetRemainingBytes() >= est_mat_adjlist_size);
			} else {
				while (VidToAdjacencyListMappingTable::VidToAdjTable.BeingReferenced(obj->lv_)) {
					usleep(64 * 1024);
				}
				INVARIANT (!VidToAdjacencyListMappingTable::VidToAdjTable.IsValid(obj->lv_));
				INVARIANT (!VidToAdjacencyListMappingTable::VidToAdjTable.BeingReferenced(obj->lv_));

				VidToAdjacencyListMappingTable::VidToAdjTable.DeleteAll(obj->lv_);
				VidToAdjacencyListMappingTable::VidToAdjTable.InitializeVidToAdjTable(bitmap_vid_range, obj->lv_);
            }
            VidToAdjacencyListMappingTable::VidToAdjTable.Validate(obj->lv_);
            INVARIANT (VidToAdjacencyListMappingTable::VidToAdjTable.IsValid(obj->lv_));
			INVARIANT (obj->adj_window_memory_allocator->GetRemainingBytes() >= est_mat_adjlist_size);
		
            if (req.from == req.to) {
                int64_t tmp = 1;
                int64_t bytes_sent = 0;
                turbo_tcp::Send(&RequestRespond::general_client_sockets[0], (char*) &tmp, 0, sizeof(int64_t), req.to, &bytes_sent);
            }
        }
		issue_io_for_mat_adj_timer.stop_timer(7);


		// Spawn a thread for network receive
		// Wait
		issue_io_for_mat_adj_timer.start_timer(8);
		std::future<void> wait = Aio_Helper::EnqueueHotAsynchronousTask(TG_AdjWindow<UserProgram_t>::ReceiveMatAdjLists, obj, req.to, req.lv, bitmap_vid_range, vids, target_vid_range, est_mat_adjlist_size, NoEvict);
        wait.wait();
		issue_io_for_mat_adj_timer.stop_timer(8);

		// Receive 'vids'
		issue_io_for_mat_adj_timer.start_timer(9);
        //if (obj->lv_ < UserArguments::MAX_LEVEL - 1) {
        if (true) {
            TwoLevelBitMap<node_t>* tmp_vids = RequestRespond::GetTempNodeBitMap(true, -1);
            RequestRespond::server_sockets.recv_from_client_lock(req.to);
            RequestRespond::server_sockets.recv_from_client((char*) tmp_vids->data(), 0, src_machine);
            RequestRespond::server_sockets.recv_from_client((char*) tmp_vids->two_level_data(), 0, src_machine);
            RequestRespond::server_sockets.recv_from_client_unlock(req.to);
            TwoLevelBitMap<node_t>::Intersection(*vids, *tmp_vids, *vids);
            RequestRespond::ReturnTempNodeBitMap(true, tmp_vids);
        } else {
            //ALWAYS_ASSERT(vids->GetTotalSingleThread() == 0); // why not?
            vids->ClearAll();
        }
        issue_io_for_mat_adj_timer.stop_timer(9);

		issue_io_for_mat_adj_timer.stop_timer(0);
		
        fprintf(stdout, "[%lld] %lld [LV-%lld\tISSUE_IO_FOR_MAT_ADJ: %d->%d][vids_tot: %ld]\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n"
		        , PartitionStatistics::my_machine_id()
                , UserArguments::UPDATE_VERSION
		        , obj->lv_ + 1
		        , req.from
		        , req.to
                //, vids->GetTotal()
                , 0
		        , issue_io_for_mat_adj_timer.get_timer(0)
		        , issue_io_for_mat_adj_timer.get_timer(1)
		        , issue_io_for_mat_adj_timer.get_timer(2)
		        , issue_io_for_mat_adj_timer.get_timer(3)
		        , issue_io_for_mat_adj_timer.get_timer(4)
		        , issue_io_for_mat_adj_timer.get_timer(5)
		        , issue_io_for_mat_adj_timer.get_timer(6)
		        , issue_io_for_mat_adj_timer.get_timer(7)
		        , issue_io_for_mat_adj_timer.get_timer(8)
		        , issue_io_for_mat_adj_timer.get_timer(9));
    }

	void PrePinEdgePages(Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, Range<int> version_range = Range<int>(0, UserArguments::MAX_VERSION), EdgeType e_type=OUTEDGE, DynamicDBType d_type=INSERT);
	
    virtual ReturnStatus RunLocalScatterGatherInMemory(Range<int64_t> start_dst_vector_chunks, Range<int64_t> src_vector_chunks, Range<int64_t> dst_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<int64_t> next_dst_vector_chunks, Range<node_t> vid_range, PageID MaxNumPagesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);

	virtual ReturnStatus RunLocalScatterGather(Range<int64_t> start_dst_vector_chunks, Range<int64_t> src_vector_chunks, Range<int64_t> dst_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<int64_t> next_dst_vector_chunks, Range<node_t> vid_range, PageID MaxNumPagesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);

	bool ScheduleNextTask (task_ctxt_t*& ctxt, PartitionID cur_src_vector_chunk_id, PartitionID cur_dst_vector_chunk_id, int64_t& num_subchunks_scheduled, Range<int> version_range, EdgeType e_type, DynamicDBType d_type=INSERT);

	PageID LaunchTask (task_ctxt_t* ctxt, bool NoEvict, EdgeType e_type);

	bool FlushUpdateBufferPerThread(int start_partition_id, int cur_dst_vector_chunk_id, int next_dst_vector_chunk_id, int64_t subchunk_idx) {
		bool overflow = false;
		for(auto& vec : user_program_->Vectors() ) {
			overflow |= vec->FlushUpdateBufferPerThread(start_partition_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, subchunk_idx);
		}
		return overflow;
	}
	void FlushUpdateBuffer(int start_partition_id, int cur_dst_vector_chunk_id, int next_dst_vector_chunk_id, int64_t subchunk_idx, int64_t overflow_cnt) {
		for(auto& vec : user_program_->Vectors() ) {
			vec->FlushUpdateBuffer(start_partition_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, subchunk_idx, overflow_cnt);
		}
	}

	

	
	void ScatterPage_Edge(PageID pid, Page* page, TwoLevelBitMap<node_t>& active_vertices_bitmap) {
		ALWAYS_ASSERT(page != NULL);
		ALWAYS_ASSERT(UserArguments::ITERATOR_MODE == PARTIAL_LIST);
		ALWAYS_ASSERT(UserArguments::USE_DEGREE_ORDER_REPRESENTATION == false); // currently not supported

		ReturnStatus st;
		AdjList_Iterator_t adjlist_iter;
		NbrList_Iterator_t nbrlist_iter;
		adjlist_iter.SetCurrentPage(pid, page);
		TG_ThreadContexts::pid_being_processed = pid;

		int64_t edges_cnt = 0;
		int64_t tid = omp_get_thread_num();
		per_thread_lsg_timer[tid].start_timer(0);
		while (adjlist_iter.GetNextAdjList(active_vertices_bitmap, nbrlist_iter) == OK) {
			ALWAYS_ASSERT (nbrlist_iter.GetSrcVid() >= PartitionStatistics::my_first_node_id()
			               && nbrlist_iter.GetSrcVid() <= PartitionStatistics::my_last_node_id());

			// Before processing
			node_t src_vid = nbrlist_iter.GetSrcVid();
			node_t idx = src_vid - PartitionStatistics::my_first_internal_vid();
			node_t* nbr_vids[3] = {nullptr};
			int64_t nbr_sz[3] = {0};
      int64_t num_process_nbr_sz;
			if (!TG_ThreadContexts::run_delta_nwsm) {
				nbr_vids[0] = nbrlist_iter.GetData();
				nbr_sz[0] = nbrlist_iter.GetNumEntries();
        num_process_nbr_sz = nbr_sz[0];
			} else {
				if (TG_ThreadContexts::ctxt->db_type == INSERT) {
					nbr_vids[0] = nbrlist_iter.GetData();
					nbr_sz[1] = nbrlist_iter.GetNumEntries();
          num_process_nbr_sz = nbr_sz[1];
				} else {
					nbr_vids[0] = nbrlist_iter.GetData();
					nbr_sz[2] = nbrlist_iter.GetNumEntries();
          num_process_nbr_sz = nbr_sz[2];
				}
			}

			// Processing
			per_thread_lsg_timer[tid].start_timer(1);
			if (UserArguments::USE_INCREMENTAL_SCATTER) {
				if (UserArguments::RUN_SUBQUERIES) {
#ifdef OPTIMIZE_WINDOW_SHARING
					for (auto subquery_idx = 0; subquery_idx < dq_nodes.size(); subquery_idx++) {
						per_thread_subquery_timer[tid][subquery_idx].start_timer(0); 
						if (dq_nodes[subquery_idx]->Get(idx)) {
							((user_program_)->*SubQueries[subquery_idx])(src_vid, nbr_vids, nbr_sz);
              edges_cnt += num_process_nbr_sz;
						}
						per_thread_subquery_timer[tid][subquery_idx].stop_timer(0); 
					}
#else
					per_thread_subquery_timer[tid][UserArguments::CURRENT_SUBQUERY_IDX].start_timer(0); 
					if (dq_nodes[UserArguments::CURRENT_SUBQUERY_IDX]->Get(idx)) {
						((user_program_)->*SubQueries[UserArguments::CURRENT_SUBQUERY_IDX])(src_vid, nbr_vids, nbr_sz);
            edges_cnt += num_process_nbr_sz;
					}
					per_thread_subquery_timer[tid][UserArguments::CURRENT_SUBQUERY_IDX].stop_timer(0); 
#endif
				} else if (UserArguments::RUN_PRUNING_SUBQUERIES) {
					if (is_exist_pruning_subqueries.size() > UserArguments::PRUNING_BFS_LV) {
						for (auto subquery_idx = 0; subquery_idx < is_exist_pruning_subqueries[UserArguments::PRUNING_BFS_LV].size(); subquery_idx++) {
							if (is_exist_pruning_subqueries[UserArguments::PRUNING_BFS_LV][subquery_idx]) {
								((user_program_)->*PruningSubQueries[UserArguments::PRUNING_BFS_LV][subquery_idx])(src_vid, nbr_vids, nbr_sz);
                edges_cnt += num_process_nbr_sz;
							}
						}
					}
				} else {
					((user_program_)->*IncAdjScatter)(src_vid, nbr_vids, nbr_sz);
          edges_cnt += num_process_nbr_sz;
				}
			} else {
				if (UserArguments::USE_PULL) {
					((user_program_)->*AdjScatter_PULL)(src_vid, nbr_vids, nbr_sz);
				} else {
					((user_program_)->*AdjScatter)(src_vid, nbr_vids, nbr_sz);
				}
        edges_cnt += num_process_nbr_sz;
			}
			per_thread_lsg_timer[tid].stop_timer(1);
		}
		per_thread_lsg_timer[tid].stop_timer(0);
		user_program_->per_thread_num_processed_edges_cnts[omp_get_thread_num()].idx += edges_cnt;
		user_program_->per_thread_num_processed_pages_cnts[omp_get_thread_num()].idx++;
		if (edges_cnt == 0)
			user_program_->per_thread_num_processed_empty_pages_cnts[omp_get_thread_num()].idx++;
	}

	void GroupNodeListsIntoPageLists(Range<int64_t>& dst_edge_partitions, Range<node_t> vid_range, PageID MaxNumPagesToPin, Range<node_t>& ProcessedVertices, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
		input_page_bitmap_.ClearAll();
        if (e_type == INOUTEDGE) {
            LOG_ASSERT(false); // syko - 2019.07.28
            //GroupNodeListsIntoPageListsForStreaming(input_node_bitmap_, vid_range, dst_edge_partitions, input_page_bitmap_, MaxNumPagesToPin, ProcessedVertices, version_range, OUTEDGE, d_type);
            //GroupNodeListsIntoPageListsForStreaming(input_node_bitmap_, vid_range, dst_edge_partitions, input_page_bitmap_, MaxNumPagesToPin, ProcessedVertices, version_range, INEDGE, d_type);
        } else {
            if (d_type == INSERT) {
                GroupNodeListsIntoPageListsForStreaming(input_node_bitmap_, delta_nwsm_input_node_bitmap_[0], vid_range, dst_edge_partitions, input_page_bitmap_, MaxNumPagesToPin, ProcessedVertices, version_range, e_type, INSERT);

            } else if (d_type == DELETE) {
                version_range.SetBegin(version_range.GetEnd());
                GroupNodeListsIntoPageListsForStreaming(input_node_bitmap_, delta_nwsm_input_node_bitmap_[1], vid_range, dst_edge_partitions, input_page_bitmap_, MaxNumPagesToPin, ProcessedVertices, version_range, e_type, DELETE);

            } else {
                GroupNodeListsIntoPageListsForStreaming(input_node_bitmap_, delta_nwsm_input_node_bitmap_[0], vid_range, dst_edge_partitions, input_page_bitmap_, MaxNumPagesToPin, ProcessedVertices, version_range, e_type, INSERT);
                version_range.SetBegin(version_range.GetEnd());
                GroupNodeListsIntoPageListsForStreaming(input_node_bitmap_, delta_nwsm_input_node_bitmap_[1], vid_range, dst_edge_partitions, input_page_bitmap_, MaxNumPagesToPin, ProcessedVertices, version_range, e_type, DELETE);

            }
        }
	}

    void CountNumPagesToProcess(Range<int64_t> src_vector_chunks, Range<int64_t> dst_edge_subchunks, Range<int> version_range, EdgeType e_type, DynamicDBType d_type=INSERT) {
        if (e_type == INOUTEDGE) {
            LOG_ASSERT(false);
            CountNumPagesToProcess_(src_vector_chunks, dst_edge_subchunks, num_pages_to_process_per_subchunk[INSERT], block_ranges_per_subchunk[INSERT], version_range, OUTEDGE, INSERT);
            CountNumPagesToProcess_(src_vector_chunks, dst_edge_subchunks, num_pages_to_process_per_subchunk[INSERT], block_ranges_per_subchunk[INSERT], version_range, INEDGE, INSERT);
        } else {
            if (d_type == INSERT) {
                CountNumPagesToProcess_(src_vector_chunks, dst_edge_subchunks, num_pages_to_process_per_subchunk[INSERT], block_ranges_per_subchunk[INSERT], version_range, e_type, INSERT);
            } else if (d_type == DELETE) {
                version_range.SetBegin(version_range.GetEnd());
                CountNumPagesToProcess_(src_vector_chunks, dst_edge_subchunks, num_pages_to_process_per_subchunk[DELETE], block_ranges_per_subchunk[DELETE], version_range, e_type, DELETE);
            } else {
                CountNumPagesToProcess_(src_vector_chunks, dst_edge_subchunks, num_pages_to_process_per_subchunk[INSERT], block_ranges_per_subchunk[INSERT], version_range, e_type, INSERT);
                version_range.SetBegin(version_range.GetEnd());
                CountNumPagesToProcess_(src_vector_chunks, dst_edge_subchunks, num_pages_to_process_per_subchunk[DELETE], block_ranges_per_subchunk[DELETE], version_range, e_type, DELETE);
            }
        }
    }
  
  private:

	void CheckNumPagesToProcess(Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, TG_Vector<TG_Vector<PageID>>& num_pages_to_process, TG_Vector<TG_Vector<Range<PageID>>>& block_ranges) {
        abort(); //XXX - tslee not used
        ALWAYS_ASSERT(false);
	}
	
    void CountNumPagesToProcess_(Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, TG_Vector<TG_Vector<TG_Vector<PageID>>>& num_pages_to_process, TG_Vector<TG_Vector<TG_Vector<Range<PageID>>>>& block_ranges, Range<int> version_range, EdgeType e_type, DynamicDBType d_type=INSERT);


	int64_t PrePinMemoryHitPages (Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, Range<int> version_range, EdgeType e_type, DynamicDBType d_type=INSERT);

	PageID PreFetchingIssueParallelRead(PageID start_page_id, PageID last_page_id, diskaio::DiskAioInterface** my_io, int32_t task_id, int32_t version_id, EdgeType e_type, DynamicDBType d_type);

	PageID IssueParallelReadBatchSequential (task_ctxt_t* ctxt, PageID& pid_offset) {
		LOG_ASSERT (false);
	}

	PageID IssueParallelReadByMRU (PageID num_pages_to_request, PageID old_pid, task_ctxt_t* ctxt, PageID& pid_offset) {
		LOG_ASSERT (false);
	}

	inline virtual void CallbackTask(diskaio::DiskAioRequestUserInfo &user_info) {
		ALWAYS_ASSERT (user_info.db_info.page_id >= taskid_to_ctxt[user_info.db_info.d_type][user_info.db_info.version_id][user_info.task_id].start_page_id && user_info.db_info.page_id <= taskid_to_ctxt[user_info.db_info.d_type][user_info.db_info.version_id][user_info.task_id].last_page_id);
		ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == PARTIAL_LIST);
		//fprintf(stdout, "page %ld is read and enqueued into taskid_to_ctxt[%ld][%ld][%ld] at %p\n", user_info.db_info.page_id, user_info.db_info.d_type, user_info.db_info.version_id, user_info.task_id, &taskid_to_work_queue[user_info.db_info.d_type][user_info.db_info.version_id][user_info.task_id]);
        taskid_to_work_queue[user_info.db_info.d_type][user_info.db_info.version_id][user_info.task_id].enqueue(user_info.db_info.page_id);
	}
  
    public:


    void ClearInputNodeBitMap(Range<int64_t> idx_range) {
        if (UserArguments::VECTOR_PARTITIONS == 1) {
            input_node_bitmap_.ClearAll();
            if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
                delta_nwsm_input_node_bitmap_[INSERT].ClearAll();
                delta_nwsm_input_node_bitmap_[DELETE].ClearAll();
            }
        } else {
            input_node_bitmap_.ClearRange(idx_range);
            if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
                delta_nwsm_input_node_bitmap_[INSERT].ClearRange(idx_range);
                delta_nwsm_input_node_bitmap_[DELETE].ClearRange(idx_range);
            }
        }
    }

	void ClearAllAtVOI() {
        if (UserArguments::ITERATOR_MODE == PARTIAL_LIST || lv_ == 0) {
            input_node_bitmap_.ClearAll();
            delta_nwsm_input_node_bitmap_[INSERT].ClearAll();
            delta_nwsm_input_node_bitmap_[DELETE].ClearAll();
        } else {
            for (int p = 0; p < PartitionStatistics::num_machines(); p++) {
                per_machine_input_node_bitmap_[p].ClearAll();
            }
        }
    }

	void ClearAtVOI(node_t vid) {
	    LOG_ASSERT(false);
    }
    
    TwoLevelBitMap<node_t>& GetDeltaNwsmActiveVerticesBitMap(DynamicDBType type) {
        if (type == INSERT) {
            return delta_nwsm_input_node_bitmap_[0];
        } else if (type == DELETE) {
            return delta_nwsm_input_node_bitmap_[1];
        } else {
            LOG_ASSERT(false);
            return delta_nwsm_input_node_bitmap_[0];
        }
    }
    
    virtual int64_t GetActiveVerticesTotalCount() {
        ALWAYS_ASSERT(lv_ == 0);
        int64_t cnts = 0;
        cnts += input_node_bitmap_.GetTotalSingleThread();
        return cnts;
    }
    
    TwoLevelBitMap<node_t>& GetActiveVerticesBitMap() {
		ALWAYS_ASSERT (UserArguments::CURRENT_LEVEL == 0);
        ALWAYS_ASSERT (lv_ == 0);
        return input_node_bitmap_;
    }
    
    void MarkAllAtVOI() {
        input_node_bitmap_.SetAll();
    }

    void MarkAtVOI(node_t vid) {
		if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
			ALWAYS_ASSERT (input_node_vid_range_.contains(vid));
			input_node_bitmap_.Set_Atomic(vid - input_node_vid_range_.GetBegin());
		} else {
			if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
				vid = PartitionStatistics::DegreeOrderToVid(vid);
			}
			ALWAYS_ASSERT(vid >= 0 && vid < PartitionStatistics::num_total_nodes());
            if (lv_ == 0) {
                input_node_bitmap_.Set_Atomic(vid - input_node_vid_range_.GetBegin());
            } else {
                int p = vid / PartitionStatistics::my_num_internal_nodes();
                per_machine_input_node_bitmap_[p].Set_Atomic(vid - PartitionStatistics::per_machine_first_node_id(p));
            }
		}
	}
    
    void MarkAtVOI(int subquery_idx, node_t vid) {
        if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
            ALWAYS_ASSERT (input_node_vid_range_.contains(vid));
            dq_nodes[subquery_idx-1]->Set_Atomic(vid - input_node_vid_range_.GetBegin());
        } else {
			if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
				vid = PartitionStatistics::DegreeOrderToVid(vid);
			}
			ALWAYS_ASSERT(vid >= 0 && vid < PartitionStatistics::num_total_nodes());
            /*if (lv_ == 0) {
                dq_nodes[subquery_idx-1]->Set_Atomic(vid - input_node_vid_range_.GetBegin());
            } else {*/
                ALWAYS_ASSERT(dq_nodes[subquery_idx-1]->Get_Atomic(vid));
                dq_nodes[subquery_idx-1]->Set_Atomic(vid);
            //}
        }
	}
    
    void MarkAtVOIUnsafe(node_t vid) {
        input_node_bitmap_.Set(vid - input_node_vid_range_.GetBegin());
	}

	bool IsMarkedAtVOI(node_t vid) {
		if (UserArguments::ITERATOR_MODE == PARTIAL_LIST || lv_ == 0) {
			ALWAYS_ASSERT(input_node_vid_range_.contains(vid));
			return input_node_bitmap_.Get(vid - input_node_vid_range_.GetBegin());
		} else {
			if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
				vid = PartitionStatistics::DegreeOrderToVid(vid);
			}
            int p = vid / PartitionStatistics::my_num_internal_nodes();
            return per_machine_input_node_bitmap_[p].Get(vid - PartitionStatistics::per_machine_first_node_id(p));
			//return full_input_node_bitmap_.Get(vid);
        }
	}

	bool HasActivatedVertex() {
		ALWAYS_ASSERT(lv_ == 0);
        return input_node_bitmap_.IsSet() || delta_nwsm_input_node_bitmap_[INSERT].IsSet() || delta_nwsm_input_node_bitmap_[DELETE].IsSet() ;
	}

	TwoLevelBitMap<PageID>& GetInputPageBitMap() {
		return input_page_bitmap_;
	}
	TwoLevelBitMap<node_t>& GetInputNodeBitMap() {
		return input_node_bitmap_;
	}
	Range<node_t> GetInputNodeVidRange() {
		return input_node_vid_range_;
	}
	Range<node_t> GetVidRangeBeingProcessed() {
		return vid_range_being_processed;
	}

	void ClearAll() {
        if (lv_ > 0 && UserArguments::ITERATOR_MODE == FULL_LIST) {
            for (int p = 0; p < PartitionStatistics::num_machines(); p++) {
                per_machine_input_node_bitmap_[p].ClearAll();
            }
        }
		input_page_bitmap_.ClearAll();
	}
    
	void Initialize(UserProgram_t* user_program, int lv, Range<int> version_range = Range<int>(0, UserArguments::MAX_VERSION));
    
    void ClearDqNodes() {
        for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
            dq_nodes[subquery_idx]->ClearAll();
            for (int i = 0; i < UserArguments::NUM_THREADS; i++)
                per_thread_subquery_timer[i][subquery_idx].clear_all_timers();
        }
    }
    
    void MarkStartingVertices(TwoLevelBitMap<int64_t>& Va_new, TwoLevelBitMap<int64_t>& Va_old, TwoLevelBitMap<int64_t>& Vdv, TwoLevelBitMap<int64_t>& Va, TwoLevelBitMap<node_t>** VdE) {
#ifdef NEIGHBOR_PRUNING
        for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
            Range<node_t> vid_range;
            vid_range.Set(PartitionStatistics::my_first_node_id(), PartitionStatistics::my_last_node_id());
            dq_nodes[subquery_idx]->InvokeIfMarked([&](node_t vid) {
                node_t idx = vid - PartitionStatistics::my_first_node_id();
//#ifdef OPTIMIZE_WINDOW_SHARING
                Va.Set_Atomic(idx);
//#endif
            }, vid_range);
            //TwoLevelBitMap<int64_t>::UnionToRight(*dq_nodes[subquery_idx], Va);
        }
//#ifdef OPTIMIZE_WINDOW_SHARING
        TwoLevelBitMap<int64_t>::Intersection(Va, Va_new, Va);
//#endif

#else // ifndef NEIGHBOR_PRUNING
				// XXX fuck
        for (int subquery_idx = 1; subquery_idx < SubQueries.size(); subquery_idx++) {
						Va_new.InvokeIfMarked([&](node_t idx) {
								node_t vid = idx + PartitionStatistics::my_first_node_id();
								dq_nodes[subquery_idx]->Set_Atomic(vid);
						});
        }
//#ifdef OPTIMIZE_WINDOW_SHARING
				Va.CopyFrom(Va_new);
//#endif

#endif
        return;
    }
    
    void MarkStartingVerticesPartialListMode(TwoLevelBitMap<int64_t>& Va_new, TwoLevelBitMap<int64_t>& Va_old, TwoLevelBitMap<int64_t>& Vdv, TwoLevelBitMap<int64_t>& Va, TwoLevelBitMap<int64_t>& Va_delta_insert, TwoLevelBitMap<int64_t>& Va_delta_delete, TwoLevelBitMap<node_t>** VdE) {
        turbo_timer st_timer;
        int64_t cnts_subq1 = 0;
        int64_t cnts_subq2 = 0;
        // Mark starting vertices for each subquery
        st_timer.start_timer(0);
        for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
            if (!is_dq_nodes_empty_at_prev_ss[subquery_idx] || (UserArguments::SUPERSTEP == 0))
                dq_nodes[subquery_idx]->ClearAll();
        }
        st_timer.stop_timer(0);

        st_timer.start_timer(1);
        std::list<TwoLevelBitMap<int64_t>*> dq_nodes_list;
        INVARIANT(SubQueries.size() == 2);
        for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
            std::list<TwoLevelBitMap<int64_t>*> bitmap_list;
            if (use_Vdv_or_Va[subquery_idx]) {
                bitmap_list.push_back(&Vdv);
            } else {
                bitmap_list.push_back(&Va_new);
            }

            for (int i = 0; i < start_from_VdE[subquery_idx].size(); i++) {
                if (start_from_VdE[subquery_idx][i]) {
                    bitmap_list.push_back(&VdE[i][ALL]);
                }
            }
            st_timer.start_timer(2);
            if (bitmap_list.size() == 1) { // SubQ1: VdV join E_old --> union to Va
                cnts_subq1 = dq_nodes[subquery_idx]->CopyFrom(*bitmap_list.front());
            } else if (bitmap_list.size() == 2) { // SubQ2: V' join dE --> union to Va_delta
                cnts_subq2 = TwoLevelBitMap<int64_t>::Intersection(*bitmap_list.front(), *bitmap_list.back(), *dq_nodes[subquery_idx]);
                //fprintf(stdout, "[%ld] (%d, %d) MarkStartingVertices dq_nodes[%d] after intersect Total = %ld, intersect size %ld, %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, subquery_idx, dq_nodes[subquery_idx]->GetTotal(), bitmap_list.front()->GetTotal(), bitmap_list.back()->GetTotal());
            } else { // currently, this not happen
                INVARIANT(false);
                TwoLevelBitMap<int64_t>::InvokeIfMarkedOnIntersection([&](node_t idx) {
                    dq_nodes[subquery_idx]->Set_Atomic(idx);
                }, bitmap_list);
            }
            st_timer.stop_timer(2);
        }
        st_timer.stop_timer(1);
        st_timer.start_timer(3);
        for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
            //fprintf(stdout, "[%ld] (%d, %d) MarkStartingVertices dq_nodes[%d] after union Total = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, subquery_idx, dq_nodes[subquery_idx]->GetTotal());
            if (use_Vdv_or_Va[subquery_idx]) {
                if (cnts_subq1 != 0) {
                    TwoLevelBitMap<int64_t>::UnionToRight(*dq_nodes[subquery_idx], Va);
                    is_dq_nodes_empty_at_prev_ss[subquery_idx] = false;
                } else {
                    is_dq_nodes_empty_at_prev_ss[subquery_idx] = true;
                }
            } else {
                if (cnts_subq2 != 0) {
                    TwoLevelBitMap<int64_t>::UnionToRight(*dq_nodes[subquery_idx], Va_delta_insert);
                    TwoLevelBitMap<int64_t>::UnionToRight(*dq_nodes[subquery_idx], Va_delta_delete);
                    is_dq_nodes_empty_at_prev_ss[subquery_idx] = false;
                } else {
                    is_dq_nodes_empty_at_prev_ss[subquery_idx] = true;
                }
            }
        }
        st_timer.stop_timer(3);
//#ifdef PRINT_PROFILING_TIMERS
        //fprintf(stdout, "[%ld] MarkStartingVertices %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), st_timer.get_timer(0), st_timer.get_timer(1), st_timer.get_timer(2), st_timer.get_timer(3));
        //fprintf(stdout, "[%ld] MarkStarting Vertices |V_active| = %ld |V_insert_active| = %ld, |V_delete_active| = %ld\n", PartitionStatistics::my_machine_id(), Va.GetTotal(), Va_delta_insert.GetTotal(), Va_delta_delete.GetTotal());
//#endif
    }

	void SetIncScatterFunction(void (TurbographImplementation::*IncAdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[])) {
		IncAdjScatter = IncAdjScatter_;
	}
	void SetScatterFunction(void (UserProgram_t::*AdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[])) {
		AdjScatter = AdjScatter_;
	}

    void SetScatterFunction_PULL(void (UserProgram_t::*AdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[])) {
		AdjScatter_PULL = AdjScatter_;
	}
	
    template <typename... Ts>
    void AddIncScatter(SubQueryFunc SubQuery, int subquery_idx, bool use_Vdv_or_Va_, Ts... ts) {
        // Create dq_nodes && flags
        if (dq_nodes.size() < subquery_idx) {
            INVARIANT(dq_nodes.size() == subquery_idx - 1);
            
            // Increase vector size & Initialize flags
            //dq_nodes.push_back(TwoLevelBitMap<node_t>());
            dq_nodes.push_back(new TwoLevelBitMap<node_t>());
            use_Vdv_or_Va.push_back(use_Vdv_or_Va_);
            is_dq_nodes_empty_at_prev_ss.push_back(false);
            std::vector<bool> tmp_vec = {ts ...};
            start_from_VdE.push_back(tmp_vec);

            // Initialize bitmap
            if (lv_ == 0 && (UserArguments::ITERATOR_MODE != FULL_LIST))
                dq_nodes[subquery_idx-1]->Init(PartitionStatistics::my_num_internal_nodes());
            else
                dq_nodes[subquery_idx-1]->Init(PartitionStatistics::num_total_nodes());
        }

        // Register subqueries
        INVARIANT((SubQueries.size() + 1) == subquery_idx);
        SubQueries.push_back(SubQuery);
				UserArguments::NUM_TOTAL_SUBQUERIES = SubQueries.size();
    }
    
    void AddPruningScatter(SubQueryFunc PruningQuery, int bfs_lv, int pruning_lv, int subquery_idx) {
        while (PruningSubQueries.size() < bfs_lv) {
            std::vector<SubQueryFunc> tmp_vec;
            PruningSubQueries.push_back(tmp_vec);
            std::vector<bool> tmp_bool_vec;
            is_exist_pruning_subqueries.push_back(tmp_bool_vec);
        }
        while (PruningSubQueries[bfs_lv-1].size() < subquery_idx-1) {
            PruningSubQueries[bfs_lv-1].push_back(nullptr);
            is_exist_pruning_subqueries[bfs_lv-1].push_back(false);
        }
        PruningSubQueries[bfs_lv-1].push_back(PruningQuery);
        is_exist_pruning_subqueries[bfs_lv-1].push_back(true);
    }

    inline bool prune_by_VdE(int subquery_idx, node_t vid) {
        node_t idx = (lv_ == 0 && (UserArguments::ITERATOR_MODE != FULL_LIST)) ? vid - PartitionStatistics::my_first_node_id() : vid;
        //fprintf(stdout, "Prune_by_VdE Adj_win lv_ %d Dump dq_nodes[%d], vid = %ld\n", lv_, subquery_idx, vid);
        //dq_nodes[subquery_idx-1]->Dump();
        return dq_nodes[subquery_idx-1]->Get(idx);
        /*bool ret_val = false;
        for (int i = 0; i < start_from_VdE[subquery_idx-1].size(); i++) {
            if (start_from_VdE[subquery_idx-1][i]) {
                ret_val = ret_val || VdE[i][ALL].Get(vid);
            }
        }
        return ret_val;*/
    }
    
    inline void set_dq_nodes(int subquery_idx, node_t vid) {
        /*if (dq_nodes.size() <= subquery_idx-1) {
            fprintf(stdout, "dq_nodes.size %ld <= subq_idx %d\n", dq_nodes.size(), subquery_idx-1);
            INVARIANT(dq_nodes.size() > subquery_idx-1);
        }*/
        node_t idx = vid;
        //node_t idx = (lv_ == 0 && (UserArguments::ITERATOR_MODE != FULL_LIST)) ? vid - PartitionStatistics::my_first_node_id() : vid; //XXX for single machine... ??? why wtf
        /*if (dq_nodes[subquery_idx-1]->num_entries() <= idx || idx < 0) {
            fprintf(stdout, "dq_nodes[%d]->num_entries %ld <= idx %d\n", subquery_idx-1, dq_nodes[subquery_idx-1]->num_entries(), idx);
            INVARIANT(dq_nodes[subquery_idx-1]->num_entries() > idx);
        }*/
        dq_nodes[subquery_idx-1]->Set_Atomic(idx);
    }

    inline void AllreduceDQNodes(std::vector<std::vector<bool>>& is_exist_pruning_subqueries_) {
        if (is_exist_pruning_subqueries_.size() <= 1) return;
        for (int i = 0; i < dq_nodes.size(); i++) {
            if (is_exist_pruning_subqueries_[1].size() <= i) continue;
            if (!is_exist_pruning_subqueries_[1][i]) continue;
            RequestRespond::TurboAllReduceInPlaceAsync<char, BOR>((char*)dq_nodes[i]->GetBitMap(), dq_nodes[i]->container_size(), sizeof(char), 0);
            RequestRespond::PopAllreduce();
            RequestRespond::TurboAllReduceInPlaceAsync<char, BOR>((char*)dq_nodes[i]->two_level_data(), dq_nodes[i]->two_level_container_size(), sizeof(char), 0);
            RequestRespond::PopAllreduce();
            
            //fprintf(stdout, "[%ld] lv %d subquery %d dq_nodes total %ld\n", PartitionStatistics::my_machine_id(), lv_, i, dq_nodes[i]->GetTotal());
        }
    }
	
    static void RunPrefetchingDiskIoForLocalScatterGather(TG_AdjWindow<UserProgram_t>* ew, Range<int64_t> sched_input_vector_chunk, TG_Vector<Range<int64_t>>* sched_output_vector_chunks, TG_Vector<bool>* sched_output_vector_chunks_active_flag, Range<int64_t> subchunk_idxs, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);


	void PrintEdgeWindowIoInformation() {
		int64_t prev_adjlist_edges_bytes = adjlist_io_bytes_edges.load();
		int64_t prev_adjlist_etc_bytes = adjlist_io_bytes_etc.load();
		int64_t prev_adjlist_edges_cached_bytes = adjlist_io_bytes_edges_cached.load();
		int64_t prev_adjlist_etc_cached_bytes = adjlist_io_bytes_etc_cached.load();
        adjlist_io_bytes_edges.store(0L);
        adjlist_io_bytes_etc.store(0L);
        adjlist_io_bytes_edges_cached.store(0L);
        adjlist_io_bytes_etc_cached.store(0L);

		MPI_Allreduce(MPI_IN_PLACE, &prev_adjlist_edges_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
		MPI_Allreduce(MPI_IN_PLACE, &prev_adjlist_etc_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
		MPI_Allreduce(MPI_IN_PLACE, &prev_adjlist_edges_cached_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
		MPI_Allreduce(MPI_IN_PLACE, &prev_adjlist_etc_cached_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

		if (PartitionStatistics::my_machine_id() == 0) {
			fprintf(stdout, "[AdjListIoBytes] LV-%ld (%ld + %ld) (%ld + %ld) (in MB)\n", lv_, (int64_t) prev_adjlist_edges_bytes / (1024 * 1024L), (int64_t) prev_adjlist_edges_cached_bytes / (1024*1024L), (int64_t) prev_adjlist_etc_bytes / (1024*1024L), (int64_t) prev_adjlist_etc_cached_bytes / (1024*1024L));
		}
        if (UserArguments::UPDATE_VERSION >= 1) {
            for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
                int64_t edge_bytes = adjlist_io_bytes_edges_subqueries[subquery_idx].load();
                MPI_Allreduce(MPI_IN_PLACE, &edge_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
                if (PartitionStatistics::my_machine_id() == 0) {
                    fprintf(stdout, "\t[AdjListIoBytes-SQ%d] LV-%ld %ld (in MB)\n", subquery_idx, lv_, (int64_t) edge_bytes / (1024 * 1024L));
                }
                adjlist_io_bytes_edges_subqueries[subquery_idx].store(0L);
            }
        }
	}

    void ResetTimers() {
        local_scatter_gather_timer.clear_all_timers();
        for (int i = 0; i < UserArguments::NUM_THREADS; i++)
            per_thread_lsg_timer[i].clear_all_timers();
		//local_scatter_gather_timer.reset_timer(0);
		//local_scatter_gather_timer.reset_timer(1);
    }

    void WaitAllTasks() {
        while (tasks_to_wait.size() > 0) {
            tasks_to_wait.front().wait();
            tasks_to_wait.pop();
        }
    }

    void CompleteAioRequests() {
        Turbo_bin_aio_handler::WaitMyPendingDiskIO(false, 0);
    }

    static void Scatter_MatAdj(TG_AdjWindow<UserProgram_t>* obj, MaterializedAdjacencyLists* mat_adj, bool clear_vid_window, bool check_vid_window, int partition_id=-1) {
        ALWAYS_ASSERT (mat_adj->SanityCheckUsingDegree(true));
        ALWAYS_ASSERT (mat_adj->SanityCheck(true, true, true));
                
        int64_t tid = TG_ThreadContexts::thread_id;
        int64_t cnt = 0;
        for (int64_t adj_idx = 0; adj_idx < mat_adj->GetNumAdjacencyLists(); adj_idx++) {
            node_t src_vid = mat_adj->GetSlotVid(adj_idx);
            node_t src_vid_idx = src_vid - obj->vid_range_to_process.GetBegin();
           
            if (!obj->fulllist_latest_flags.Get_Atomic(src_vid_idx)) {
                obj->fulllist_latest_flags.Set_Atomic(src_vid_idx);
            }
            if (clear_vid_window) {
                //INVARIANT(obj->current_vid_window.Get_Atomic(src_vid_idx));
                obj->current_vid_window.Clear_Atomic(src_vid_idx);
                obj->current_vid_window_being_processed.Set_Atomic(src_vid_idx);
            } else {
                if (obj->per_machine_input_node_bitmap_[partition_id].Get_Atomic(src_vid_idx)) {
                    obj->per_machine_input_node_bitmap_[partition_id].Clear_Atomic(src_vid_idx);
                    //fprintf(stdout, "[%ld] obj->per_machine_input_node_bitmap[%ld] Get %ld = false\n", PartitionStatistics::my_machine_id(), partition_id, src_vid);
                } else {
                    //INVARIANT(obj->per_machine_input_node_bitmap_[partition_id].Get_Atomic(src_vid_idx));
                    continue;
                }
            }
            if (obj->AdjScatter == NULL) continue;;
            if (check_vid_window && !obj->input_node_bitmap_.Get(src_vid_idx)) continue;

            node_t* nbr_vids[3] = {nullptr};
            int64_t nbr_sz[3] = {0};
            mat_adj->GetAdjList(adj_idx, nbr_vids, nbr_sz);

            // Processing
            if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
                node_t src_vid_deg_order = PartitionStatistics::VidToDegreeOrder(src_vid);
                if (UserArguments::USE_INCREMENTAL_SCATTER) {
                    if (UserArguments::RUN_SUBQUERIES) {
#ifdef OPTIMIZE_WINDOW_SHARING
                        for (auto subquery_idx = 0; subquery_idx < obj->dq_nodes.size(); subquery_idx++) {
                            obj->per_thread_subquery_timer[tid][subquery_idx].start_timer(0); 
														if (obj->dq_nodes[subquery_idx]->Get(src_vid)) {
																obj->adjlist_io_bytes_edges_subqueries[subquery_idx].fetch_add((nbr_sz[0] + nbr_sz[1] + nbr_sz[2]) * sizeof(node_t));
																((obj->user_program_)->*(obj->SubQueries[subquery_idx]))(src_vid_deg_order, nbr_vids, nbr_sz);
														}
                            obj->per_thread_subquery_timer[tid][subquery_idx].stop_timer(0); 
                        }
#else
												obj->per_thread_subquery_timer[tid][UserArguments::CURRENT_SUBQUERY_IDX].start_timer(0); 
												if (obj->dq_nodes[UserArguments::CURRENT_SUBQUERY_IDX]->Get(src_vid)) {
														obj->adjlist_io_bytes_edges_subqueries[UserArguments::CURRENT_SUBQUERY_IDX].fetch_add((nbr_sz[0] + nbr_sz[1] + nbr_sz[2]) * sizeof(node_t));
														((obj->user_program_)->*(obj->SubQueries[UserArguments::CURRENT_SUBQUERY_IDX]))(src_vid_deg_order, nbr_vids, nbr_sz);
												}
												obj->per_thread_subquery_timer[tid][UserArguments::CURRENT_SUBQUERY_IDX].stop_timer(0); 
#endif
                    } else {
                        ((obj->user_program_)->*(obj->IncAdjScatter))(src_vid_deg_order, nbr_vids, nbr_sz);
                    }
                } else {
                    ALWAYS_ASSERT(nbr_sz[1] == 0);
                    ALWAYS_ASSERT(nbr_sz[2] == 0);
                    ((obj->user_program_)->*(obj->AdjScatter))(src_vid_deg_order, nbr_vids, nbr_sz);
                }
            } else {
                LOG_ASSERT(false); // by syko @ 19.05.02
                /*if (UserArguments::USE_INCREMENTAL_SCATTER) {
                    ((obj->user_program_)->*(obj->IncAdjScatter))(src_vid, nbr_vids, nbr_sz);
                } else {
                    ALWAYS_ASSERT(nbr_sz[1] == 0);
                    ALWAYS_ASSERT(nbr_sz[2] == 0);
                    ((obj->user_program_)->*(obj->AdjScatter))(src_vid, nbr_vids[0], nbr_sz[0]);
                }*/
            }
            cnt++;
        }

        /*
        if (UserArguments::CURRENT_LEVEL == 0) {
            fprintf(stdout, "[%ld] %ld [Scatter_MatAdj] %p, cnt = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, mat_adj, cnt);
        }
        */
    }


    std::vector<std::vector<bool>>& GetPruningSubQueriesFlag() {
        return is_exist_pruning_subqueries;
    }
};


template<typename UserProgram_t>
inline void TG_AdjWindow<UserProgram_t>::ReleaseEdgeWindow (EdgeType e_type) {
	if (UserArguments::ITERATOR_MODE == IteratorMode_t::PARTIAL_LIST) {
		TurboDB::GetBufMgr()->UnpinPages(input_page_bitmap_);
	} else {
		if (lv_ != UserArguments::MAX_LEVEL - 1) {
			// TODO - Buffer Replacement
			MaterializedAdjacencyLists::DropMaterializedViewInMemory(lv_);
		} else {

		}
	}
}


template<typename UserProgram_t>
inline void TG_AdjWindow<UserProgram_t>::Close(EdgeType e_type) {
	if (taskid_to_ctxt != NULL) {
		INVARIANT(taskid_to_work_queue != NULL);
		INVARIANT(taskid_to_processed_page_queue != NULL);
		INVARIANT(taskid_to_overflow_cnt != NULL);

		int64_t pq = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS;
		int64_t pqr = pq * PartitionStatistics::num_subchunks_per_edge_chunk();
		int64_t num_versions = version_range_.length();
		int num_total_subchunks = PartitionStatistics::num_subchunks_per_edge_chunk() * PartitionStatistics::num_target_vector_chunks();
		for (int64_t db_type = 0; db_type < 2; db_type++) {
			for (int version = version_range_.GetBegin(); version <= version_range_.GetEnd(); version++) {
				delete[] taskid_to_ctxt[db_type][version];
				delete[] taskid_to_work_queue[db_type][version];
				delete[] taskid_to_processed_page_queue[db_type][version];
			}
			delete[] taskid_to_ctxt[db_type];
			delete[] taskid_to_work_queue[db_type];
			delete[] taskid_to_processed_page_queue[db_type];
		}

		delete[] taskid_to_ctxt;
		delete[] taskid_to_work_queue;
		delete[] taskid_to_processed_page_queue;
		delete[] taskid_to_overflow_cnt;

		delete processed_page_queue;

		taskid_to_ctxt = NULL;
		taskid_to_work_queue = NULL;
		taskid_to_processed_page_queue = NULL;
		taskid_to_overflow_cnt = NULL;

		std::string voi_name = "VOI_LV" + std::to_string(lv_ + 1);
		LocalStatistics::register_mem_alloc_info(voi_name.c_str(), 0);
	}
	
	current_vid_window.Close();
	current_vid_window_being_processed.Close();
	
	fulllist_latest_flags.Close();
	delta_nwsm_input_node_bitmap_[0].Close();
	delta_nwsm_input_node_bitmap_[1].Close();
	input_node_bitmap_.Close();
	input_page_bitmap_.Close();
	hit_page_bitmap_.Close();

	if (per_machine_input_node_bitmap_ != NULL) {
		for (int p = 0; p < PartitionStatistics::num_machines(); p++) {
			per_machine_input_node_bitmap_[p].Close();
		}
		delete[] per_machine_input_node_bitmap_;
		per_machine_input_node_bitmap_ = NULL;
	}
	if (adj_window_memory_allocator != NULL) {
		ReleaseEdgeWindow(e_type);
		if (adj_window_memory_allocator != NULL) {
			delete adj_window_memory_allocator;
			adj_window_memory_allocator = NULL;
		}
	}
	for (int subquery_idx = 0; subquery_idx < SubQueries.size(); subquery_idx++) {
		dq_nodes[subquery_idx]->Close();
	}
}

template<typename UserProgram_t>
TG_AdjWindow<UserProgram_t>::~TG_AdjWindow() {
	Close();
}


template<typename UserProgram_t>
void TG_AdjWindow<UserProgram_t>::Initialize(UserProgram_t* user_program, int lv, Range<int> version_range) {
	system_fprintf(0, stdout, "[%ld] TG_AdjWindow::Initialize Lv = %ld\n", PartitionStatistics::my_machine_id(), lv);
	ALWAYS_ASSERT (lv >= 0 && lv < UserArguments::MAX_LEVEL);

	// initialize
	lv_ = lv;
	version_range_ = version_range;
	user_program_ = user_program;
	input_node_vid_range_ = PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id());
	
	// initialize bitmap datastructures
	current_vid_window.Init(PartitionStatistics::my_num_internal_nodes());
	current_vid_window_being_processed.Init(PartitionStatistics::my_num_internal_nodes());
	
	fulllist_latest_flags.Init(PartitionStatistics::my_num_internal_nodes());
	delta_nwsm_input_node_bitmap_[0].Init(PartitionStatistics::my_num_internal_nodes());
	delta_nwsm_input_node_bitmap_[1].Init(PartitionStatistics::my_num_internal_nodes());
	input_node_bitmap_.Init(PartitionStatistics::my_num_internal_nodes());
	if (lv_ > 0 && UserArguments::ITERATOR_MODE == FULL_LIST) {
		//per_machine_input_node_bitmap_ = new BitMap<node_t>[PartitionStatistics::num_machines()];
		per_machine_input_node_bitmap_ = new TwoLevelBitMap<node_t>[PartitionStatistics::num_machines()];
		for (int p = 0; p < PartitionStatistics::num_machines(); p++) {
			per_machine_input_node_bitmap_[p].Init(PartitionStatistics::my_num_internal_nodes());
		}
	}

	input_page_bitmap_.Init(TurboDB::GetBufMgr()->GetNumberOfTotalPages());
	hit_page_bitmap_.Init(TurboDB::GetBufMgr()->GetNumberOfTotalPages());

	std::string voi_name = "VOI_LV" + std::to_string(lv+1);
	if (UserArguments::ITERATOR_MODE == FULL_LIST) {
		LocalStatistics::register_mem_alloc_info(voi_name.c_str(), (current_vid_window.container_size() + current_vid_window_being_processed.container_size() + fulllist_latest_flags.container_size() + delta_nwsm_input_node_bitmap_[0].container_size() + delta_nwsm_input_node_bitmap_[1].container_size() + input_node_bitmap_.container_size() + (lv > 0 ? PartitionStatistics::num_machines() * per_machine_input_node_bitmap_[0].container_size() : 0) + input_page_bitmap_.container_size() + hit_page_bitmap_.container_size()) / (1024 * 1024));
	} else {
		LocalStatistics::register_mem_alloc_info(voi_name.c_str(), (current_vid_window.container_size() + current_vid_window_being_processed.container_size() + fulllist_latest_flags.container_size() + delta_nwsm_input_node_bitmap_[0].container_size() + delta_nwsm_input_node_bitmap_[1].container_size() + input_node_bitmap_.container_size() + input_page_bitmap_.container_size() + hit_page_bitmap_.container_size()) / (1024 * 1024));
	}
	// initialize per-thread meta-data
	INVARIANT(core_id::core_counts() == UserArguments::NUM_THREADS);
	
	int64_t pq = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS;
	int64_t pqr = pq * PartitionStatistics::num_subchunks_per_edge_chunk();
	int64_t num_versions = version_range.length();
	int num_total_subchunks = PartitionStatistics::num_subchunks_per_edge_chunk() * PartitionStatistics::num_target_vector_chunks();
	
	taskid_to_num_processed_version.resize(pqr);
	
	if (taskid_to_ctxt == NULL) {
		taskid_to_overflow_cnt = new int64_t[pqr];
		processed_page_queue = new task_page_queue_t();

		taskid_to_ctxt = new task_ctxt_t**[2];
		taskid_to_work_queue = new task_page_queue_t**[2];
		taskid_to_processed_page_queue = new task_page_queue_t**[2];
		for (int64_t db_type = 0; db_type < 2; db_type++) {
			taskid_to_ctxt[db_type] = new task_ctxt_t*[num_versions];
			taskid_to_work_queue[db_type] = new task_page_queue_t*[num_versions];
			taskid_to_processed_page_queue[db_type] = new task_page_queue_t*[num_versions];

			for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
				taskid_to_ctxt[db_type][version] = new task_ctxt_t[num_total_subchunks];
				taskid_to_work_queue[db_type][version] = new task_page_queue_t[num_total_subchunks];
				taskid_to_processed_page_queue[db_type][version] = new task_page_queue_t[num_total_subchunks];
			}
		}
	}

	/*
	int64_t sum1 = sizeof(task_ctxt_t) * num_total_subchunks * version_range.length() * 2;
	int64_t sum2 = 2 * sizeof(task_page_queue_t) * num_total_subchunks * version_range.length() * 2;
	int64_t sum3 = sizeof(Range<PageID>) * pqr * pq * num_versions * 2;
	system_fprintf(0, stdout, "%ld %ld %ld\n", sum1, sum2, sum3);
	*/

	block_ranges_per_subchunk.resize(2);
	num_pages_to_process_per_subchunk.resize(2);
	for (int64_t db_type = 0; db_type < 2; db_type++) {
		block_ranges_per_subchunk[db_type].resize(num_versions);
		num_pages_to_process_per_subchunk[db_type].resize(num_versions);
		for (int64_t version = 0; version < num_versions; version++) {
			block_ranges_per_subchunk[db_type][version].resize(pq);
			num_pages_to_process_per_subchunk[db_type][version].resize(pq);
			for (int64_t i = 0; i < pq; i++) {
				block_ranges_per_subchunk[db_type][version][i].resize(pqr);
				num_pages_to_process_per_subchunk[db_type][version][i].resize(pqr);
			}
		}
	}
	// barrier
	PartitionStatistics::wait_for_all();
}

void (*turbo_callback::AsyncCallbackUserFunction)(diskaio::DiskAioRequestUserInfo &user_info) = TG_NWSM_CallbackTask;

void TG_NWSM_CallbackTask(diskaio::DiskAioRequestUserInfo &user_info) {
	ALWAYS_ASSERT (user_info.db_info.page_id >= 0);
	ALWAYS_ASSERT (user_info.frame_id >= 0);
	//ALWAYS_ASSERT (user_info.caller != NULL); //XXX is this right?

	TG_NWSMCallback* caller = (TG_NWSMCallback*) user_info.caller;

	if (caller != NULL) {
		caller->CallbackTask(user_info);
	}
}

template<typename UserProgram_t>
void TG_AdjWindow<UserProgram_t>::InitializeTaskDataStructures(Range<int64_t> src_vector_chunks, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
    int64_t pq = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS;
    int64_t pqr = pq * PartitionStatistics::num_subchunks_per_edge_chunk();
    if (UserArguments::USE_FULLIST_DB) pqr = 1;
    
    for (int64_t i = 0; i < pqr; i++) {
        taskid_to_num_processed_version[i] = 0;
        taskid_to_overflow_cnt[i] = 0;
        
        for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
            if ((d_type == INSERT || d_type == ALL) && TurboDB::GetTurboDB(e_type)->is_version_exist(INSERT, version)) {
                taskid_to_num_processed_version[i] += 1;
            }
            if ((d_type == DELETE || d_type == ALL) && (version == version_range.GetEnd()) && TurboDB::GetTurboDB(e_type)->is_version_exist(DELETE, version)) {
                taskid_to_num_processed_version[i] += 1;
            }
        }
    }

    for (int64_t db_type = 0; db_type < 2; db_type++) {
        for (int64_t i = src_vector_chunks.GetBegin(); i <= src_vector_chunks.GetEnd(); i++) {
            for (int subchunk_id = 0; subchunk_id < pqr; subchunk_id++) {
                int64_t task_id = subchunk_id;
                for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
                    taskid_to_ctxt[db_type][version][task_id].is_valid = false;
                    taskid_to_ctxt[db_type][version][task_id].DONE = false;
                    taskid_to_ctxt[db_type][version][task_id].IO_DONE = false;
                    taskid_to_ctxt[db_type][version][task_id].has_been_scheduled = false;
                    taskid_to_ctxt[db_type][version][task_id].start_page_id = block_ranges_per_subchunk[db_type][version][i][task_id].GetBegin();
                    taskid_to_ctxt[db_type][version][task_id].last_page_id = block_ranges_per_subchunk[db_type][version][i][task_id].GetEnd();
                    taskid_to_ctxt[db_type][version][task_id].num_pages_to_process = num_pages_to_process_per_subchunk[db_type][version][i][task_id];
                    taskid_to_ctxt[db_type][version][task_id].num_pages_processed = 0;
                    INVARIANT (taskid_to_processed_page_queue[db_type][version][task_id].size_approx() == 0);
                }
            }
        }
    }
}
template<typename UserProgram_t>
void TG_AdjWindow<UserProgram_t>::EmptyProcessedPageQueue(EdgeType e_type, DynamicDBType d_type) {
	#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
	{
		PageID page_to_unpin_list[128];
		turbo_buffer_manager* buf_mgr = TurboDB::GetBufMgr();
		while (true) {
			int64_t num_pages = processed_page_queue->try_dequeue_bulk(page_to_unpin_list, 128);
			if (num_pages == 0) break;
			ALWAYS_ASSERT(num_pages <= 128);
			buf_mgr->UnpinPageUnsafeBulk(-1, &page_to_unpin_list[0], num_pages);
		}
	}
}

template<typename UserProgram_t>
void TG_AdjWindow<UserProgram_t>::PrePinEdgePages(Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	turbo_timer prepin_timer;
	prepin_timer.start_timer(0);
	hit_page_bitmap_.ClearAll();
	prepin_timer.stop_timer(0);
	prepin_timer.start_timer(1);
	TurboDB::GetBufMgr()->GetHitPages(input_page_bitmap_, hit_page_bitmap_);
	prepin_timer.stop_timer(1);
	prepin_timer.start_timer(2);
	if (e_type == INOUTEDGE) {
		LOG_ASSERT(false);
		PrePinMemoryHitPages(src_edge_partitions, dst_edge_partitions, version_range, OUTEDGE);
		PrePinMemoryHitPages(src_edge_partitions, dst_edge_partitions, version_range, INEDGE);
	} else {
		if (d_type == INSERT) {
			PrePinMemoryHitPages(src_edge_partitions, dst_edge_partitions, version_range, e_type, INSERT);

		} else if (d_type == DELETE) {
			version_range.SetBegin(version_range.GetEnd());
			PrePinMemoryHitPages(src_edge_partitions, dst_edge_partitions, version_range, e_type, DELETE);

		} else {
			PrePinMemoryHitPages(src_edge_partitions, dst_edge_partitions, version_range, e_type, INSERT);
			version_range.SetBegin(version_range.GetEnd());
			PrePinMemoryHitPages(src_edge_partitions, dst_edge_partitions, version_range, e_type, DELETE);

		}
	}
	prepin_timer.stop_timer(2);
#ifdef PRINT_PROFILING_TIMERS
	fprintf(stdout, "[%ld] (%d, %d) PrePinEdgePages %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, prepin_timer.get_timer(0), prepin_timer.get_timer(1), prepin_timer.get_timer(2));
#endif
}

template<typename UserProgram_t>
ReturnStatus TG_AdjWindow<UserProgram_t>::RunLocalScatterGatherInMemory(Range<int64_t> start_dst_vector_chunks, Range<int64_t> src_vector_chunks, Range<int64_t> dst_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<int64_t> next_dst_vector_chunks, Range<node_t> vid_range, PageID MaxNumPagesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	int64_t num_total_processed_edges = 0;
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
	{
		//int64_t num_processed_edges_per_thread = 0;
//#pragma omp for schedule(guided, 1024)
#pragma omp for schedule(dynamic, 1024)
		for (int64_t src_vid = 0; src_vid < TurboDB::graph_in_memory.size(); src_vid++) {
			if (!input_node_bitmap_.Get(src_vid)) continue;
			node_t* nbr_vids[3];
			int64_t nbr_sz[3];
			nbr_vids[0] = TurboDB::graph_in_memory[src_vid].data();
			nbr_sz[0] = TurboDB::graph_in_memory[src_vid].size();
			//num_processed_edges_per_thread += nbr_sz[0];
			// Processing
			//((user_program_)->*IncAdjScatter)(src_vid, nbr_vids, nbr_sz);
			((user_program_)->*AdjScatter)(src_vid, nbr_vids, nbr_sz);
			/*if (UserArguments::USE_INCREMENTAL_SCATTER) {
				((user_program_)->*IncAdjScatter)(src_vid, nbr_vids, nbr_sz);
			} else {
				if (UserArguments::USE_PULL) {
					((user_program_)->*AdjScatter_PULL)(src_vid, nbr_vids[0], nbr_sz[0]);
				} else {
					((user_program_)->*AdjScatter)(src_vid, nbr_vids[0], nbr_sz[0]);
				}
			}*/
		}
		//std::atomic_fetch_add((std::atomic<int64_t>*) &num_total_processed_edges, num_processed_edges_per_thread);
		bool overflow = FlushUpdateBufferPerThread(0, 0, 0, 0);
	}
	FlushUpdateBuffer(0, 0, 0, 0, 1);
	FlushUpdateBuffer(0, 0, 0, 1, 1);
	fprintf(stdout, "# processed edges = %ld\n", num_total_processed_edges);
	return DONE;
}

template<typename UserProgram_t>
ReturnStatus TG_AdjWindow<UserProgram_t>::RunLocalScatterGather(Range<int64_t> start_dst_vector_chunks, Range<int64_t> src_vector_chunks, Range<int64_t> dst_vector_chunks, Range<int64_t> next_src_vector_chunks, Range<int64_t> next_dst_vector_chunks, Range<node_t> vid_range, PageID MaxNumPagesToPin, bool NoEvict, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == PARTIAL_LIST);
	ALWAYS_ASSERT (input_node_vid_range_.contains(vid_range));

	local_scatter_gather_timer.start_timer(0);
	ReturnStatus rt = PrepareForLocalScatterGather(src_vector_chunks, dst_vector_chunks, vid_range, MaxNumPagesToPin, NoEvict);
	if (rt == DONE) return rt;

	ALWAYS_ASSERT (start_dst_vector_chunks.GetBegin() == start_dst_vector_chunks.GetEnd());
	ALWAYS_ASSERT (src_vector_chunks.GetBegin() == src_vector_chunks.GetEnd());
	ALWAYS_ASSERT (dst_vector_chunks.GetBegin() == dst_vector_chunks.GetEnd());

	PartitionID start_dst_vector_chunk_id = start_dst_vector_chunks.GetBegin();
	PartitionID cur_dst_vector_chunk_id = dst_vector_chunks.GetBegin();
	PartitionID cur_src_vector_chunk_id = src_vector_chunks.GetBegin();
	PartitionID next_dst_vector_chunk_id = next_dst_vector_chunks.GetBegin();
	PartitionID next_src_vector_chunk_id = next_src_vector_chunks.GetBegin();

	bool HasPageToProcess = false;
	PageID num_pages_to_process = 0;
	PageID num_pages_to_request = 0;

	Range<PageID> cur_block_range(-1, -1);
	Range<int64_t> cur_src_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(src_vector_chunks);
	Range<int64_t> cur_dst_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(dst_vector_chunks);

	num_pages_to_request = 0;
	for (int64_t idx = 0; idx < PartitionStatistics::num_subchunks_per_edge_chunk(); idx++) {
		for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
			if ((d_type == INSERT || d_type == ALL) && TurboDB::GetTurboDB(e_type)->is_version_exist(INSERT, version)) {
				num_pages_to_request += num_pages_to_process_per_subchunk[INSERT][version][cur_src_vector_chunk_id][cur_dst_edge_subchunks.GetBegin() + idx];
			}

			if ((d_type == DELETE || d_type == ALL) && version == version_range.GetEnd() && TurboDB::GetTurboDB(e_type)->is_version_exist(DELETE, version)) {
				num_pages_to_request += num_pages_to_process_per_subchunk[DELETE][version][cur_src_vector_chunk_id][cur_dst_edge_subchunks.GetBegin() + idx];
			}
		}
	}

	PageID num_pages_to_request_init = num_pages_to_request;
	PageID num_pages_processed_per_partition = 0;

	HasPageToProcess = (num_pages_to_request_init > 0);
	int64_t num_subchunks_scheduled = 0;
	int64_t num_subchunks_to_be_processed = PartitionStatistics::num_subchunks_per_edge_chunk() * (version_range.GetEnd() - version_range.GetBegin() + 1);
	if (d_type == ALL) num_subchunks_to_be_processed *= 2;

	for (int idx = 0; idx < UserArguments::NUM_THREADS; idx++) {
		threadid_to_taskid_map[idx] = -1;
	}
	local_scatter_gather_timer.stop_timer(0);

	local_scatter_gather_timer.start_timer(1);

	if (num_pages_to_request_init == 0) {
		local_scatter_gather_timer.start_timer(2);
		for (int subchunk_idx = 0; subchunk_idx < PartitionStatistics::num_subchunks_per_edge_chunk(); subchunk_idx++) {
			int64_t task_id = PartitionStatistics::num_subchunks_per_edge_chunk() * cur_dst_vector_chunk_id + subchunk_idx;
			FlushUpdateBuffer(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id % PartitionStatistics::num_subchunks_per_edge_chunk(), 0);
		}
		local_scatter_gather_timer.stop_timer(2);
	} else {
		local_scatter_gather_timer.start_timer(3);
		int64_t shared_counters = 0;
		#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
		{
			ALWAYS_ASSERT (TG_ThreadContexts::thread_id == omp_get_thread_num());
			int64_t thread_id = omp_get_thread_num();
			int64_t per_thread_num_pages_processed = 0;
			turbo_timer timer;
			do {
				timer.start_timer(0);
				CompleteAioRequests();
				timer.stop_timer(0);
				task_ctxt_t* task_ctxt;

				timer.start_timer(1);
				bool scheduled = ScheduleNextTask(task_ctxt, cur_src_vector_chunk_id, cur_dst_vector_chunk_id, num_subchunks_scheduled, version_range, e_type, d_type);
				timer.stop_timer(1);
				
				if (!scheduled) break;

				timer.start_timer(2);
				PageID num_pages_processed = LaunchTask(task_ctxt, NoEvict, e_type);
				per_thread_num_pages_processed += num_pages_processed;
				timer.stop_timer(2);
				
				timer.start_timer(3);
				bool is_master = (thread_id == task_ctxt->master_thread_id);
				bool overflow = FlushUpdateBufferPerThread(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_ctxt->task_id % PartitionStatistics::num_subchunks_per_edge_chunk());
				if (overflow) {
					std::atomic_fetch_add((std::atomic<int64_t>*) &task_ctxt->overflow, 1L);
					std::atomic_fetch_add((std::atomic<int64_t>*) &taskid_to_overflow_cnt[task_ctxt->task_id], 1L);
				}
				timer.stop_timer(3);
				
				if (!is_master) {
					std::atomic_fetch_add((std::atomic<int64_t>*) &task_ctxt->num_threads, -1L);
				} else {
					timer.start_timer(4);
					while(std::atomic_load((std::atomic<int64_t>*) &task_ctxt->num_threads) != 1L) {
						CompleteAioRequests();
						_mm_pause();
					}
					timer.stop_timer(4);
					timer.start_timer(5);
					if (std::atomic_fetch_add((std::atomic<int64_t>*) &taskid_to_num_processed_version[task_ctxt->task_id], -1L) == 1L) {
						int64_t cur_subchunk_overflow_counter = std::atomic_load((std::atomic<int64_t>*) &taskid_to_overflow_cnt[task_ctxt->task_id]);

						FlushUpdateBuffer(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_ctxt->task_id % PartitionStatistics::num_subchunks_per_edge_chunk(), cur_subchunk_overflow_counter);
						std::atomic_fetch_add((std::atomic<int64_t>*) &task_ctxt->num_threads, -1L);
						std::atomic_store((std::atomic<int64_t>*) &task_ctxt->overflow, 0L);
						std::atomic_store((std::atomic<int64_t>*) &taskid_to_overflow_cnt[task_ctxt->task_id], 0L);
						threadid_to_taskid_map[thread_id] = -1;
					} else {
						std::atomic_fetch_add((std::atomic<int64_t>*) &task_ctxt->num_threads, -1L);
						std::atomic_store((std::atomic<int64_t>*) &task_ctxt->overflow, 0L);
					}
					std::atomic_fetch_add((std::atomic<PageID>*) &num_pages_processed_per_partition, (PageID) num_pages_processed);
					timer.stop_timer(5);
				}
				ALWAYS_ASSERT (num_pages_processed_per_partition >= 0);
				ALWAYS_ASSERT (num_pages_processed_per_partition <= num_pages_to_request_init);
			} while (std::atomic_load((std::atomic<int64_t>*) &num_subchunks_scheduled) != (num_subchunks_to_be_processed)
						|| std::atomic_load((std::atomic<PageID>*) &num_pages_processed_per_partition) < num_pages_to_request_init);
			//} while (std::atomic_load((std::atomic<int64_t>*) &num_subchunks_scheduled) != (PartitionStatistics::num_subchunks_per_edge_chunk() * (version_range.GetEnd() - version_range.GetBegin() + 1))
	
			//fprintf(stdout, "[%ld] (%ld, %ld, %ld) [lv:%ld/%ld] [Thread-%ld] LocalScatterGather (DstVectorChunk: %ld, # pages processed: %ld)\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, lv_, UserArguments::MAX_LEVEL, omp_get_thread_num(), (int64_t) cur_dst_vector_chunk_id, per_thread_num_pages_processed, timer.get_timer(0), timer.get_timer(1), timer.get_timer(2), timer.get_timer(3), timer.get_timer(4), timer.get_timer(5));
		}
		local_scatter_gather_timer.stop_timer(3);
	}
	local_scatter_gather_timer.stop_timer(1);
	return rt;
}

template<typename UserProgram_t>
bool TG_AdjWindow<UserProgram_t>::ScheduleNextTask (task_ctxt_t*& ctxt, PartitionID cur_src_vector_chunk_id, PartitionID cur_dst_vector_chunk_id, int64_t& num_subchunks_scheduled, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	int64_t thread_id = omp_get_thread_num();
	int64_t socket_id = NumaHelper::get_socket_id_by_omp_thread_id(thread_id);
	INVARIANT (thread_id == TG_ThreadContexts::thread_id);

	// NUMA-aware, only when IO Speed >> CPU computation
	bool numa_flags[2] = {true, false};

	for (int flag_idx = 0; flag_idx < 2; flag_idx++) {
		bool numa_aware = (UserArguments::NUM_THREADS > 1) ? numa_flags[flag_idx] : false;
		for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
			for (int64_t db_type = 0; db_type < 2; db_type++) {
				//fprintf(stdout, "[%ld][ScheduleNextTask] %ld %ld %ld %ld\n", pthread_self(), numa_aware, version, db_type, TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType)db_type, version));
				
				if (!(d_type == db_type || d_type == ALL)) continue;
				if ((db_type == (int) DELETE) && (version != version_range.GetEnd())) continue;
				if (!TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType)db_type, version)) continue;
				for (int64_t subchunk_idx = 0; subchunk_idx < PartitionStatistics::num_subchunks_per_edge_chunk(); subchunk_idx++) {
					int64_t task_id = PartitionStatistics::num_subchunks_per_edge_chunk() * cur_dst_vector_chunk_id + subchunk_idx;
					int64_t subchunk_id_to_process = task_id;
					ctxt = &taskid_to_ctxt[db_type][version][task_id];
					bool not_scheduled = false;
					if (numa_aware && task_id % NumaHelper::num_sockets() != socket_id) continue;
					if (numa_aware && !std::atomic_load((std::atomic<bool>*) &ctxt->IO_DONE)) continue; // it works for single-machine.. why?
					if (std::atomic_load((std::atomic<bool>*) &ctxt->DONE)) continue;
					if (std::atomic_compare_exchange_strong((std::atomic<bool>*) &ctxt->has_been_scheduled, &not_scheduled, true)) {
						ALWAYS_ASSERT (std::atomic_load((std::atomic<bool>*) &ctxt->is_valid) == false);
						ALWAYS_ASSERT (std::atomic_load((std::atomic<bool>*) &ctxt->DONE) == false);
						ALWAYS_ASSERT (std::atomic_load((std::atomic<bool>*) &ctxt->has_been_scheduled) == true);
						ALWAYS_ASSERT (std::atomic_load((std::atomic<int64_t>*) &ctxt->num_threads) == 0L);
						ALWAYS_ASSERT (std::atomic_load((std::atomic<int64_t>*) &ctxt->overflow) == 0L);

						//fprintf(stdout, "ScheduleNextTask, task_id = %ld, version = %d, thread_id = %ld, socket_id = %ld, subchunks = %ld\n", task_id, version, thread_id, socket_id, subchunk_id_to_process);
						ctxt->task_id = task_id;
						ctxt->version_id = version;
						ctxt->db_type = (DynamicDBType) db_type;
						
						ctxt->master_thread_id = thread_id;
						ctxt->master_socket_id = socket_id;

						ctxt->edge_chunk_src_id = cur_src_vector_chunk_id;
						ctxt->edge_chunk_dst_id = cur_dst_vector_chunk_id;

						ctxt->edge_dst_subchunk_id = subchunk_id_to_process;

						ctxt->task_page_queue = &taskid_to_work_queue[db_type][version][ctxt->task_id];
						ctxt->processed_page_queue = &taskid_to_processed_page_queue[db_type][version][ctxt->task_id];

						ctxt->num_pages_to_process = num_pages_to_process_per_subchunk[db_type][version][cur_src_vector_chunk_id][subchunk_id_to_process];
						ctxt->num_pages_processed = 0;
						ctxt->start_page_id = block_ranges_per_subchunk[db_type][version][cur_src_vector_chunk_id][subchunk_id_to_process].GetBegin();
						ctxt->last_page_id = block_ranges_per_subchunk[db_type][version][cur_src_vector_chunk_id][subchunk_id_to_process].GetEnd();
						ctxt->dst_vid_range = TurboDB::GetTurboDB(e_type)->GetVidRangeByEdgeSubchunkId(subchunk_id_to_process);

						ALWAYS_ASSERT (cur_src_vector_chunk_id >= 0 && cur_src_vector_chunk_id < num_pages_to_process_per_subchunk[db_type][version].size());
						ALWAYS_ASSERT (ctxt->processed_page_queue->size_approx() == 0);
						ALWAYS_ASSERT (cur_src_vector_chunk_id >= 0 && cur_src_vector_chunk_id < block_ranges_per_subchunk[db_type][version].size());
						ALWAYS_ASSERT (subchunk_id_to_process >= 0 && subchunk_id_to_process < block_ranges_per_subchunk[db_type][version][cur_src_vector_chunk_id].size());

						std::atomic_store((std::atomic<bool>*) &ctxt->DONE, (ctxt->num_pages_to_process == 0));
						std::atomic_store((std::atomic<int64_t>*) &ctxt->num_threads, 1L);
						std::atomic_store((std::atomic<bool>*) &ctxt->is_valid, true);
						std::atomic_fetch_add((std::atomic<int64_t>*) &num_subchunks_scheduled, 1L);
						return true;
					}
					while (!std::atomic_load((std::atomic<bool>*) &ctxt->is_valid)) {
						_mm_pause();
					}
					if (ctxt->num_pages_to_process == 0) continue;
					if (std::atomic_load((std::atomic<bool>*) &ctxt->DONE)) continue;
					std::atomic_fetch_add((std::atomic<int64_t>*) &ctxt->num_threads, 1L);
					return true;
				}
			}
		}
	}
	return false;
}

template<typename UserProgram_t>
PageID TG_AdjWindow<UserProgram_t>::LaunchTask (task_ctxt_t* ctxt, bool NoEvict, EdgeType e_type) {
	ALWAYS_ASSERT(ctxt != NULL);
	ALWAYS_ASSERT(ctxt->db_type == INSERT || ctxt->db_type == DELETE);
	ALWAYS_ASSERT(!NoEvict);
	int64_t thread_id = omp_get_thread_num();
	INVARIANT (thread_id == TG_ThreadContexts::thread_id);

	bool is_master = (thread_id == std::atomic_load((std::atomic<int64_t>*) &ctxt->master_thread_id));
	turbo_timer tim;

	if (std::atomic_load((std::atomic<bool>*) &taskid_to_ctxt[ctxt->db_type][ctxt->version_id][ctxt->task_id].DONE)) return 0;
	ALWAYS_ASSERT (ctxt->task_page_queue != NULL);

	TG_ThreadContexts::ctxt = ctxt;
	tim.start_timer(0);
	PageID pid_offset = 0;
	if (is_master) {
		int64_t pre_pinned_pages = 0;
		int64_t num_pages_requested = 0;
		int64_t min_working_sets = ctxt->num_pages_to_process;
		pre_pinned_pages += ctxt->task_page_queue->size_approx();
	}
	tim.stop_timer(0);

	Page* page_buffer = NULL;

	AdjList_Iterator_t adjlist_iter;
	Range<PageID> cur_block_range(ctxt->start_page_id, ctxt->last_page_id);
	adjlist_iter.Init(cur_block_range);

	PageID num_total_pages_processed = 0;
	bool is_vector_overflow = false;

	tim.start_timer(1);
	int64_t backoff = 1;
	PageID pid_io_done_list[16];
	while (std::atomic_load((std::atomic<PageID>*) &ctxt->num_pages_processed) < ctxt->num_pages_to_process) {
		PageID num_pages_processed = 0;
		PageID table_page_id;
		PageID pid = -1;
		while (taskid_to_work_queue[ctxt->db_type][ctxt->version_id][ctxt->task_id].try_dequeue(pid)) {
			tim.start_timer(2);
			ALWAYS_ASSERT(pid >= taskid_to_ctxt[ctxt->db_type][ctxt->version_id][ctxt->task_id].start_page_id && pid <= taskid_to_ctxt[ctxt->db_type][ctxt->version_id][ctxt->task_id].last_page_id);

			table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(ctxt->version_id, pid, e_type, ctxt->db_type);
			TurboDB::GetBufMgr()->GetPagePtr_Unsafe(table_page_id, page_buffer);
			num_pages_processed++;
			
			if (UserArguments::USE_DELTA_NWSM && ctxt->version_id == UserArguments::UPDATE_VERSION) {
				TG_ThreadContexts::run_delta_nwsm = true;
				ScatterPage_Edge(table_page_id, page_buffer, delta_nwsm_input_node_bitmap_[ctxt->db_type]);
				TG_ThreadContexts::run_delta_nwsm = false;
			} else {
				ScatterPage_Edge(table_page_id, page_buffer, input_node_bitmap_);
			}
			tim.stop_timer(2);
			
			processed_page_queue->enqueue(table_page_id);
			if (backoff != 1) backoff /= 2;
		}

		tim.start_timer(3);
		if (num_pages_processed == 0) {
			//usleep (backoff * 8);
			usleep (backoff);
			if (backoff != 128) backoff *= 2;
			//if (backoff != 1024) backoff *= 2;
		}

		if (num_pages_processed > 0) {
			PageID num_pages_processed_just_before = std::atomic_fetch_add((std::atomic<PageID>*) &ctxt->num_pages_processed, (PageID) num_pages_processed);
			ALWAYS_ASSERT (num_pages_processed_just_before + num_pages_processed <= ctxt->num_pages_to_process);
			num_total_pages_processed += num_pages_processed;
		}
		tim.stop_timer(3);
	}
	tim.stop_timer(1);

	std::atomic_store((std::atomic<bool>*) &ctxt->DONE, true);
	//fprintf(stdout, "[%ld] LaunchTask num_pages_to_process = %ld %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), ctxt->num_pages_to_process, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3));
	return (num_total_pages_processed);
}





template<typename UserProgram_t>
void TG_AdjWindow<UserProgram_t>::CountNumPagesToProcess_(Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, TG_Vector<TG_Vector<TG_Vector<PageID>>>& num_pages_to_process, TG_Vector<TG_Vector<TG_Vector<Range<PageID>>>>& block_ranges, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);

	int src_from = src_edge_partitions.GetBegin();
	int src_to = src_edge_partitions.GetEnd();
	int dst_from = dst_edge_partitions.GetBegin();
	int dst_to = dst_edge_partitions.GetEnd();

	#pragma omp parallel for collapse(2)
	for (int i = src_from; i <= src_to; i++) {
		for (int j = dst_from; j <= dst_to; j++) {
			int task_id = j;

			Range<node_t> src_node_range = Range<node_t> (PartitionStatistics::per_edge_partition_vid_range(i).GetBegin(), PartitionStatistics::per_edge_partition_vid_range(i).GetEnd());

			for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
				if (!TurboDB::GetTurboDB(e_type)->is_version_exist(d_type, version)) {
					num_pages_to_process[version][i][j] = 0;
					block_ranges[version][i][j].Set(-1, -1);
					continue;
				}
				PageID my_num_pages_to_process = 0;
				PageID first_page_id = -1;
				PageID last_page_id = -1;
				VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version, d_type);
				PageID start_pid = TurboDB::GetTurboDB(e_type)->GetFirstPageId(j, version, d_type);
				PageID last_pid = TurboDB::GetTurboDB(e_type)->GetLastPageId(j, version, d_type);
				PageID first_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, start_pid, e_type, d_type);
				PageID last_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, last_pid, e_type, d_type);
				Range<PageID> table_page_id_range(first_table_page_id, last_table_page_id);
				
				input_page_bitmap_.InvokeIfMarked([&](PageID table_pid) {
					PageID pid = TurboDB::GetTurboDB(e_type)->ConvertDirectTablePageIDToPageID(version, table_pid, e_type, d_type);
					ALWAYS_ASSERT(TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, pid, e_type, d_type) == table_pid);
					if (src_node_range.Overlapped(vidrangeperpage.Get(pid))) {
						if (first_page_id == -1) {
							first_page_id = pid;
						}
						last_page_id = pid;
						my_num_pages_to_process++;
					}
				}, table_page_id_range, true);

				hit_page_bitmap_.InvokeIfMarked([&](PageID table_pid) {
					PageID pid = TurboDB::GetTurboDB(e_type)->ConvertDirectTablePageIDToPageID(version, table_pid, e_type, d_type);
					ALWAYS_ASSERT(TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, pid, e_type, d_type) == table_pid);
					if (src_node_range.Overlapped(vidrangeperpage.Get(pid))) {
						if (first_page_id > pid || first_page_id == -1) first_page_id = pid;
						if (last_page_id < pid) last_page_id = pid;
						if (hit_page_bitmap_.Get(table_pid) && hit_page_bitmap_.Clear_Atomic(table_pid)) {
							taskid_to_work_queue[d_type][version][task_id].enqueue(pid);
						}
						my_num_pages_to_process++;
					}
				}, table_page_id_range, true);
				num_pages_to_process[version][i][j] = my_num_pages_to_process;
				block_ranges[version][i][j].Set(first_page_id, last_page_id);
			}
		}
	}
}

template<typename UserProgram_t>
int64_t TG_AdjWindow<UserProgram_t>::PrePinMemoryHitPages (Range<int64_t> src_edge_partitions, Range<int64_t> dst_edge_partitions, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);

	PageID num_pages_pinned = 0;
	int src_from = src_edge_partitions.GetBegin();
	int src_to = src_edge_partitions.GetEnd();
	int dst_from = dst_edge_partitions.GetBegin();
	int dst_to = dst_edge_partitions.GetEnd();

	PageID max_num_pages_to_prepin = (TurboDB::GetBufMgr()->GetNumberOfFrames() - PartitionStatistics::num_target_vector_chunks() * MIN_WORKING_PAGE_SET_SIZE);
	PageID max_num_pages_to_prepin_per_task = max_num_pages_to_prepin / (PartitionStatistics::num_subchunks_per_edge_chunk() * PartitionStatistics::num_target_vector_chunks());

	#pragma omp parallel for collapse(2)
	for (int i = src_from; i <= src_to; i++) {
		for (int j = dst_from; j <= dst_to; j++) {
			Range<node_t> src_node_range = Range<node_t> (PartitionStatistics::per_edge_partition_vid_range(i).GetBegin(), PartitionStatistics::per_edge_partition_vid_range(i).GetEnd());

			PageID my_num_pages_pinned = 0;

			for (int version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
				if (!TurboDB::GetTurboDB(e_type)->is_version_exist(d_type, version)) continue;
				VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version, d_type);
				PageID last_page_id = TurboDB::GetTurboDB(e_type)->GetLastPageId(j, version, d_type);
				PageID first_page_id = TurboDB::GetTurboDB(e_type)->GetFirstPageId(j, version, d_type);
				
				PageID first_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, first_page_id, e_type, d_type);
				PageID last_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, last_page_id, e_type, d_type);
				Range<PageID> table_page_id_range(first_table_page_id, last_table_page_id);

				AioRequest req;
				req.user_info.db_info.version_id = version;
				req.user_info.do_user_cb = false;
				req.user_info.func = (void*) &InvokeUserCallback;
				PartitionID my_machine_id = PartitionStatistics::my_machine_id();
				
				hit_page_bitmap_.InvokeIfMarked([&](PageID table_page_id) {
						if (my_num_pages_pinned > max_num_pages_to_prepin_per_task) {
							hit_page_bitmap_.Clear_Atomic(table_page_id);
							return;
						}
						
						PageID pid = TurboDB::GetTurboDB(e_type)->ConvertDirectTablePageIDToPageID(version, table_page_id, e_type, d_type);
						if (src_node_range.Overlapped(vidrangeperpage.Get(pid)) && input_page_bitmap_.Clear_Atomic(table_page_id)) {
							req.user_info.db_info.page_id = pid;
							req.user_info.db_info.table_page_id = table_page_id;
							TurboDB::GetBufMgr()->PrePinPageUnsafe(my_machine_id, req);
							my_num_pages_pinned++;
						}
					}, table_page_id_range, true);

				//std::atomic_fetch_add((std::atomic<PageID>*) &num_pages_pinned, my_num_pages_pinned);
			}
		}
	}
	//fprintf(stdout, "[%ld] PrePinMemoryHitPages %d\n", PartitionStatistics::my_machine_id(), num_pages_pinned);

	INVARIANT(TurboDB::GetBufMgr()->GetNumberOfFrames() > PartitionStatistics::num_subchunks_per_edge_chunk() * MIN_WORKING_PAGE_SET_SIZE);
	return num_pages_pinned;
}

template<typename UserProgram_t>
PageID TG_AdjWindow<UserProgram_t>::PreFetchingIssueParallelRead(PageID start_page_id, PageID last_page_id, diskaio::DiskAioInterface** my_io, int32_t task_id, int32_t version_id, EdgeType e_type, DynamicDBType d_type) {
	ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);

	Page* pPage = NULL;
	PageID table_page_id = 0;
	PageID num_pages_requested = 0;
	PageID total_num_pages = 0;
	PageID num_pages_requested_done = 0;
	PageID num_pages_requested_ongoing = 0;
	bool stop_prefetching = false;
	if (!TurboDB::GetTurboDB(e_type)->is_version_exist(d_type, version_id)) {
		return num_pages_requested;
	}

	ALWAYS_ASSERT (task_id >= 0);
	ALWAYS_ASSERT (start_page_id >= 0);
	ALWAYS_ASSERT (last_page_id < TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type));

	PageID start_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version_id, start_page_id, e_type, d_type);
	PageID last_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version_id, last_page_id, e_type, d_type);
	Range<PageID> table_page_id_range(start_table_page_id, last_table_page_id);

	input_page_bitmap_.InvokeIfMarked([&](PageID table_page_id) {
			input_page_bitmap_.Clear(table_page_id);

			PageID pid = TurboDB::GetTurboDB(e_type)->ConvertDirectTablePageIDToPageID(version_id, table_page_id, e_type, d_type);
			total_num_pages++;

			PageID old_table_pid = -1;
			ReturnStatus st = FAIL;

			AioRequest req;
			req.user_info.caller = this;
			req.user_info.task_id = task_id;
			req.user_info.db_info.page_id = pid;
			req.user_info.db_info.table_page_id = table_page_id;
			req.user_info.db_info.version_id = version_id;
			req.user_info.db_info.e_type = (int8_t)e_type;
			req.user_info.db_info.d_type = (int8_t)d_type;
			req.user_info.do_user_cb = true;
			req.user_info.func = (void*) &InvokeUserCallback;

			ALWAYS_ASSERT (req.user_info.db_info.page_id >= taskid_to_ctxt[req.user_info.db_info.d_type][req.user_info.db_info.version_id][req.user_info.task_id].start_page_id && req.user_info.db_info.page_id <= taskid_to_ctxt[req.user_info.db_info.d_type][req.user_info.db_info.version_id][req.user_info.task_id].last_page_id);

			int backoff = 128;
			while (my_io[READ_IO]->GetNumOngoing() >= PER_THREAD_MAXIMUM_ONGOING_DISK_AIO / 2 || my_io[WRITE_IO]->GetNumOngoing() >= PER_THREAD_MAXIMUM_ONGOING_DISK_AIO / 2) {
				int completed_write = my_io[WRITE_IO]->WaitForResponses(0);
				int completed_read = my_io[READ_IO]->WaitForResponses(0);
				if (completed_write + completed_read > 0) break;
				usleep (backoff);
				if (backoff <= 256 * 1024) backoff *= 2;
			}

/*
			if (processed_page_queue->try_pop(old_table_pid)) {
				bool replaced = false;
				st = TurboDB::GetBufMgr()->ReplacePageCallback(PartitionStatistics::my_machine_id(), req, old_table_pid, replaced, version_id, my_io);
				INVARIANT (replaced);
			} else {
				req.buf = (char*) pPage;
				st = TurboDB::GetBufMgr()->PinPageCallback(PartitionStatistics::my_machine_id(), req, my_io);
				pPage = (Page*) req.buf;
			}
*/
			int64_t num_avail_frames = TurboDB::GetTurboDB(e_type)->GetBufMgr()->GetNumberOfAvailableFrames();
			bool try_replace = (num_avail_frames <= PartitionStatistics::num_subchunks_per_edge_chunk() * PartitionStatistics::num_target_vector_chunks() * MIN_WORKING_PAGE_SET_SIZE);
			bool must_replace = (num_avail_frames <= MIN_WORKING_PAGE_SET_SIZE/2);
			if (must_replace) {
				while (!processed_page_queue->try_dequeue(old_table_pid)) {
					_mm_pause();
				}
				bool replaced = false;
				st = TurboDB::GetBufMgr()->ReplacePageCallback(PartitionStatistics::my_machine_id(), req, old_table_pid, replaced, version_id, my_io);
				INVARIANT (replaced);
			} else if (try_replace && processed_page_queue->try_dequeue(old_table_pid)) {
				bool replaced = false;
				st = TurboDB::GetBufMgr()->ReplacePageCallback(PartitionStatistics::my_machine_id(), req, old_table_pid, replaced, version_id, my_io);
				INVARIANT (replaced);
			} else {
				req.buf = (char*) pPage;
				st = TurboDB::GetBufMgr()->PinPageCallback(PartitionStatistics::my_machine_id(), req, my_io);
				pPage = (Page*) req.buf;
			}
			if (st == OK || st == ON_GOING) num_pages_requested++;
			if (st == DONE) num_pages_requested_done++;
			if (st == ON_GOING) num_pages_requested_ongoing++;
	}, table_page_id_range, true);

	//fprintf(stdout, "[%ld] Prefetch [%ld, %ld] on ver=%ld, subchunk_id=%ld, # pages requested=%ld (%ld + %ld) among %ld bits set\n", PartitionStatistics::my_machine_id(), start_page_id, last_page_id, version_id, task_id, (int64_t) num_pages_requested, (int64_t) num_pages_requested_done, (int64_t) num_pages_requested_ongoing, (int64_t) total_num_pages);
	ALWAYS_ASSERT (total_num_pages <= taskid_to_ctxt[d_type][version_id][task_id].num_pages_to_process);
	return (num_pages_requested);
}



template<typename UserProgram_t>
void TG_AdjWindow<UserProgram_t>::RunPrefetchingDiskIoForLocalScatterGather(TG_AdjWindow<UserProgram_t>* ew, Range<int64_t> sched_input_vector_chunk, TG_Vector<Range<int64_t>>* sched_output_vector_chunks, TG_Vector<bool>* sched_output_vector_chunks_active_flag, Range<int64_t> subchunk_idxs, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	ALWAYS_ASSERT (sched_input_vector_chunk.length() == 1);

	RequestRespond::InitializeIoInterface();
	diskaio::DiskAioInterface** my_io = RequestRespond::GetMyDiskIoInterface();

	int lookahead = 2;
	turbo_timer prefetch_timer;

	std::list<DynamicDBType> db_types = {INSERT, DELETE};
	int64_t prefetch_cnts = 0;
	int64_t cur_src_vector_chunk_id = sched_input_vector_chunk.GetBegin();
	for (int j = 0; j < (*sched_output_vector_chunks).size() - 1 + lookahead; j++) {
		for (int64_t subchunk_idx = subchunk_idxs.GetBegin(); subchunk_idx <= subchunk_idxs.GetEnd(); subchunk_idx++) {
			prefetch_timer.start_timer(0);
			if (j >= lookahead) {
				int64_t prefetch_cur_dst_vector_chunk_id = (*sched_output_vector_chunks)[j - lookahead].GetBegin();
				int64_t prefetch_subchunk_id = prefetch_cur_dst_vector_chunk_id * PartitionStatistics::num_subchunks_per_edge_chunk() + subchunk_idx;
				int64_t task_id = prefetch_subchunk_id;
				for (int64_t version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
					for (auto& db_type: db_types) {
						if ((d_type == db_type || d_type == ALL) && TurboDB::GetTurboDB(e_type)->is_version_exist(db_type, version)) {
							std::atomic_store((std::atomic<bool>*) &ew->taskid_to_ctxt[db_type][version][task_id].IO_DONE, true);
						}
					}
				}
			}
			prefetch_timer.stop_timer(0);
			if (j >= (*sched_output_vector_chunks).size() - 1) continue;

			int64_t cur_dst_vector_chunk_id = (*sched_output_vector_chunks)[j].GetBegin();
			int64_t subchunk_id = cur_dst_vector_chunk_id * PartitionStatistics::num_subchunks_per_edge_chunk() + subchunk_idx;
			INVARIANT (cur_dst_vector_chunk_id >= 0);

			prefetch_timer.start_timer(1);
			//for (int64_t version = version_range.GetBegin(); version <= version_range.GetEnd(); version++) {
			for (int64_t version = version_range.GetEnd(); version >= version_range.GetBegin(); version--) {
				for (auto& db_type: db_types) {
					if ((d_type == db_type || d_type == ALL) && TurboDB::GetTurboDB(e_type)->is_version_exist(db_type, version)) {
						PageID start_page_id = ew->block_ranges_per_subchunk[db_type][version][cur_src_vector_chunk_id][subchunk_id].GetBegin();
						PageID last_page_id = ew->block_ranges_per_subchunk[db_type][version][cur_src_vector_chunk_id][subchunk_id].GetEnd();

						if (ew->num_pages_to_process_per_subchunk[db_type][version][cur_src_vector_chunk_id][subchunk_id] == 0) {
							std::atomic_store((std::atomic<bool>*) &ew->taskid_to_ctxt[db_type][version][subchunk_id].IO_DONE, true);
						} else {
							int64_t num_requests = ew->PreFetchingIssueParallelRead(start_page_id, last_page_id, my_io, subchunk_id, version, e_type, (DynamicDBType) db_type);
							prefetch_cnts += num_requests;
						}
					}
				}
			}
			prefetch_timer.stop_timer(1);
		}
	}
	prefetch_timer.start_timer(2);
	DiskAioFactory::GetPtr()->WaitForAllResponses(my_io);
	prefetch_timer.stop_timer(2);
	//fprintf(stdout, "[%ld] RunPrefetchingDiskIoForLocalScatterGather prefetch_cnts = %ld, %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), prefetch_cnts, prefetch_timer.get_timer(0), prefetch_timer.get_timer(1), prefetch_timer.get_timer(2));
}

#endif
