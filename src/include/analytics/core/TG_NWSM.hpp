#pragma once
#ifndef TG_NWSM_HPP__
#define TG_NWSM_HPP__


#include "TG_NWSM_Utility.hpp"
#include "TG_NWSMTaskContext.hpp"
#include "TG_AdjWindow.hpp"
#include "MemoryMappedArray.hpp"

#include <tbb/parallel_for.h>
#include <tbb/concurrent_queue.h>

class TurbographImplementation;

class TG_NWSM_Base {
  public:

	virtual void Start(Range<int> update_version_range, EdgeType e_type, DynamicDBType d_type, std::function<void(node_t)> callback_after_apply = {}, std::function<bool(node_t)> iv_delta_write_if = [](node_t vid){ return true; }, std::function<void()> call_before_flush_iv = {}) = 0;
	virtual bool Inc_Start(Range<int> update_version_range, EdgeType e_type, DynamicDBType d_type, bool UseIncScatter, bool UseIncApply, bool AdvanceIv, std::function<void(node_t)> apply = {}, std::function<void(node_t)> callback_after_apply = [](node_t vid){}, std::function<bool(node_t)> iv_delta_write_if = [](node_t vid){ return true; }, std::function<void()> call_before_flush_iv = {}, int skip_scatter_gather = 0) = 0;
    virtual ReturnStatus UpdatePhaseHJ() = 0;

	virtual bool dq_nodes(int lv, int subquery_idx, node_t vid) = 0;
	virtual void set_dq_nodes(int lv, int subquery_idx, node_t vid) = 0;
	virtual void AllreduceDQNodes(int lv) = 0;
    virtual void Close() = 0;
	virtual bool IsMarkedAtVOI(int lv, node_t vid) = 0;
    virtual void MarkAllAtVOI(int lv) = 0;
	virtual void MarkAtVOI(int lv, node_t vid, bool activate=true) = 0;
	virtual void MarkAtVOI(int subquery_idx, int lv, node_t vid, bool activate=true) = 0;
	virtual void MarkAtVOIUnsafe(int lv, node_t vid, bool activate=true) = 0;
	virtual void ClearAtVOI(int lv, node_t vid) = 0;
	virtual void ClearAllAtVOI(int lv) = 0;
	virtual bool HasVertexInWindow(int lv, node_t vid) = 0;
	virtual bool HasVertexInWindow(int subquery_idx, int lv, node_t vid) = 0;
	virtual void GetAdjList(int lv, node_t vid, node_t*& data_ptr, int64_t& data_sz) = 0;
	virtual AdjacencyListMemoryPointer& GetAdjList(int lv, node_t vid) = 0;
    virtual void GetAdjList(int lv, node_t vid, node_t* data_ptr[], int64_t data_sz[]) = 0;
	virtual Range<node_t> GetVidRangeBeingProcessed(int lv) = 0;

    virtual TwoLevelBitMap<node_t>& GetDeltaNwsmActiveVerticesBitMap(DynamicDBType type) = 0;
    virtual TwoLevelBitMap<node_t>& GetActiveVerticesBitMap() = 0;
    virtual int64_t GetActiveVerticesTotalCount() = 0;
	virtual bool HasActivatedVertex() = 0;
	virtual bool IsActive(node_t vid) = 0;
	virtual void InitializeNestedWindowedStreamBuffer() = 0;
	virtual void SetIncScatterFunction(void (TurbographImplementation::*AdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]), int lv) = 0;
  virtual void ClearDqNodes() = 0;
    virtual void MarkStartingVertices(TwoLevelBitMap<int64_t>& Va_new, TwoLevelBitMap<int64_t>& Va_old, TwoLevelBitMap<int64_t>& Vdv, TwoLevelBitMap<int64_t>& Va, TwoLevelBitMap<node_t>** VdE) = 0;
    virtual void MarkStartingVerticesPartialListMode(TwoLevelBitMap<int64_t>& Va_new, TwoLevelBitMap<int64_t>& Va_old, TwoLevelBitMap<int64_t>& Vdv, TwoLevelBitMap<int64_t>& Va, TwoLevelBitMap<int64_t>& Va_delta_insert, TwoLevelBitMap<int64_t>& Va_delta_delete, TwoLevelBitMap<node_t>** VdE) = 0;
	
    virtual void SimulateCaching(TwoLevelBitMap<node_t>& active_vertices, Range<int> update_version_range, EdgeType e_type, DynamicDBType d_type) = 0;

    // internal APIs
	virtual void ReadInputVectors (std::vector<TG_DistributedVectorBase*>& vectors, Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) = 0;
	virtual void ReadInputVectors (TG_DistributedVectorBase* vec, Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) = 0;
	virtual void ReadInputVectors (Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) = 0;
	virtual void ReadInputVectorsAtApplyPhase (int vector_chunk_idx) = 0;
    virtual void FlushInputVectorsAndReadAndMergeAfterApplyPhase (int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) = 0;
    virtual void FlushInputVectorsAfterApplyPhase (int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) = 0;
    virtual void FlushInputVectorsAfterApplyPhase (TG_DistributedVectorBase* vec, int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) = 0;
    virtual void FlushInputVectorsAfterApplyPhase (std::vector<TG_DistributedVectorBase*>& vectors, int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) = 0;

	virtual bool SkipApplyPhase(bool skip) = 0;
    virtual void ResetTimers() = 0;
	virtual void PrintEdgeWindowIoInformation() = 0;
    virtual double GetLocalScatterExecutionTime(bool reset_timers) = 0;
    virtual void ReportNWSMTime(std::vector<std::vector<json11::Json>>& nwsm_time_breaker) = 0;

    virtual int64_t ComputeVOIMemorySize(int lv) {
        int64_t bytes = 0;
        int64_t num_entries = PartitionStatistics::my_num_internal_nodes();
        bytes += BitMap<int64_t>::compute_container_size(num_entries); // current_vid_window
        bytes += BitMap<int64_t>::compute_container_size(num_entries); // current_vid_window_being_processed
        bytes += BitMap<int64_t>::compute_container_size(num_entries); // fulllist_latest_flags
        bytes += (2 * BitMap<int64_t>::compute_container_size(num_entries)); // delta_nwsm_input_node_bitmap_
        bytes += BitMap<int64_t>::compute_container_size(num_entries); // input_node_bitmap_
		if (UserArguments::ITERATOR_MODE == FULL_LIST) {
            if (lv > 0) bytes += (PartitionStatistics::num_machines() * BitMap<int64_t>::compute_container_size(num_entries)); // per_machine_input_node_bitmap_
		}

        // XXX memory usages for input_page_bitmap_, hit_page_bitmap_ are omitted
        return bytes;
    }

    static std::atomic<int64_t> read_ggb_flag;
};



template <typename UserProgram_t>
class TG_NWSM : public TG_NWSM_Base {
	typedef TG_NWSMTaskContext task_ctxt_t;
	typedef tbb::concurrent_queue<PageID> task_page_queue_t;
    typedef void (UserProgram_t::*SubQueryFunc)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]);

	TG_AdjWindow<UserProgram_t>* adj_win;
	char* window_memory_ptr;
	int64_t window_memory_capacity;

	turbo_timer per_level_timer[8];    // Assume that the maximum level is no more than 8.
	turbo_timer apply_timer;
	turbo_timer inc_start_timer;

  protected:
	UserProgram_t* user_program_;
	void (UserProgram_t::*VertexApply)(node_t src_vid);
    std::function<void(node_t)> IncVertexApply;
    std::function<void(node_t)> CallbackAfterApply;
    std::function<bool(node_t)> IvDeltaWriteCondition;
    std::function<void()>       CallBeforeFlushIv;
    bool IncrementalProcessing;

    std::vector<MemoryMappedArray<bool>*> starting_vertices;

	PageID cur_begin_page_id_;
	PageID cur_end_page_id_;
	
    TwoLevelBitMap<node_t> cached_vertices;
	TwoLevelBitMap<node_t> tmp_node_bitmap;

  public:
	int iters;

  public:
	TG_NWSM(UserProgram_t* user_program = NULL)
		: user_program_(user_program), VertexApply(nullptr), IncVertexApply(nullptr) {
		adj_win = NULL;
		window_memory_ptr = NULL;
		window_memory_capacity = -1;
		iters = 0;
        skip_apply_phase = false;
        TG_NWSM_Base::read_ggb_flag.store(1L);
	}

	~TG_NWSM() {}

    void Close();

    void InitializeStaticProcessing() {
        if (UserArguments::ITERATOR_MODE == FULL_LIST) {
            for (int lv = 0; lv < UserArguments::MAX_LEVEL - 1; lv++) {
                VidToAdjacencyListMappingTable::VidToAdjTable.Clear(lv);
            }
        } else {

        }
        
        user_program_->reset_vector_states();
        PartitionStatistics::wait_for_all();	// BARRIER, 없애도 되나
    }

	virtual void InitializeNestedWindowedStreamBuffer() {
		if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {

		} else {
			RequestRespond::InitializeTempBitMapQueue(PartitionStatistics::max_num_nodes_per_vector_chunk(), DEFAULT_NIO_NUM_BITMAP_FOR_SENDER, DEFAULT_NIO_NUM_BITMAP_FOR_RECEIVER);
			RequestRespond::SetMaxDynamicSendBufferSize(DEFAULT_NIO_DYNAMIC_BUFFER_TOTAL_SIZE/2);
			RequestRespond::SetMaxDynamicRecvBufferSize(DEFAULT_NIO_DYNAMIC_BUFFER_TOTAL_SIZE/2);
			RequestRespond::SetMaxDynamicBufferSize(1 * (1024 * 1024 * 1024L)); // XXX
			int64_t max_dynamic_buffer_size = UserArguments::PHYSICAL_MEMORY - LocalStatistics::get_total_mem_size_allocated();
			InitializeWindowMemoryAllocator(max_dynamic_buffer_size * (1024 * 1024L));
		}
	}
    
    // Input
    //  - active_vertices: the set of active vertices to be processed at the current version
    // Output:
    //  - cached_vertices: the set of active vertices of which adjacency lists are cached
    //  - How much I/Os are reduced due to the caching?
    virtual void SimulateCaching(TwoLevelBitMap<node_t>& active_vertices, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
        // (active_vertices - cached_vertices) -> tmp_node_bitmap
        tmp_node_bitmap.ClearAll();
        active_vertices.InvokeIfMarked([&](node_t idx) {
            if (!cached_vertices.Get(idx)) {
                tmp_node_bitmap.Set_Atomic(idx);
            }
        });

        // Count(page_bitmap) = Expected I/O after caching
        Range<int64_t> total_output_vector_chunks(0, PartitionStatistics::num_target_vector_chunks() - 1);
        Range<int64_t> dst_edge_subchunks = PartitionStatistics::vector_chunks_to_edge_subchunks(total_output_vector_chunks);
        int64_t num_pages_without_caching = CountNumPagesToStream(active_vertices, dst_edge_subchunks, version_range, e_type, d_type);
        int64_t num_pages_with_caching = CountNumPagesToStream(tmp_node_bitmap, dst_edge_subchunks, version_range, e_type, d_type);

        MPI_Allreduce(MPI_IN_PLACE, &num_pages_without_caching, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
        MPI_Allreduce(MPI_IN_PLACE, &num_pages_with_caching, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

        system_fprintf(0, stdout, "[Caching Simulation] (%ld, %ld) %ld %ld\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, num_pages_without_caching, num_pages_with_caching);

        // Implement Caching
        // (active_vertices, cached_vertices) -> cached_vertices
        CachingStrategy_1(active_vertices, version_range);
    }

    void CachingStrategy_1(TwoLevelBitMap<node_t>& active_vertices, Range<int> update_version_range) {
        TwoLevelBitMap<node_t>::UnionToRight(active_vertices, cached_vertices);
    }

#ifdef OPTIMIZE_WINDOW_SHARING
	virtual void ProcessNestedWindowedStreamFullList (int lv, int machine_id, Range<int> update_version_range, EdgeType e_type = OUTEDGE) {
		ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == FULL_LIST);
		per_level_timer[lv-1].start_timer(0);

        //fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL);

		std::vector<Range<int64_t>> sched_input_vector_chunks;
		std::vector<Range<int64_t>> sched_output_vector_chunks;
		std::vector<bool> sched_input_vector_chunks_active_flag;
		std::vector<bool> sched_output_vector_chunks_active_flag;

		Range<int64_t> cur_input_vector_chunks;
		Range<int64_t> next_input_vector_chunks;
		Range<int64_t> cur_output_vector_chunks;
		Range<int64_t> next_output_vector_chunks;

		Range<node_t> vid_range;
		UserArguments::CURRENT_LEVEL = lv - 1;
		bool NeedUnpin = (lv == UserArguments::MAX_LEVEL);
		int64_t MaxNumBytesToPin = (NeedUnpin == false) ? adj_win[UserArguments::CURRENT_LEVEL].GetMemoryAllocator()->GetSize() : std::numeric_limits<int64_t>::max();
		ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);

		per_level_timer[lv-1].start_timer(1);

		for (int i = 0; i < sched_input_vector_chunks.size() - 1; i++) {
            //fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList] i %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL, i);
			// READ(vs^l.NextWindowRange())
			cur_input_vector_chunks = sched_input_vector_chunks[i];
			next_input_vector_chunks = sched_input_vector_chunks[i + 1];

			PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
			PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
			PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

			vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, cur_src_vector_chunk_id % UserArguments::VECTOR_PARTITIONS);

			per_level_timer[lv-1].start_timer(2);
			ReadInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
			per_level_timer[lv-1].stop_timer(2);

			// While (asjw^l = AsyncRead(...))
			// Un-Finalize LV-'lv' window
			per_level_timer[lv-1].start_timer(3);
			while (adj_win[UserArguments::CURRENT_LEVEL].RunScatterGather(cur_input_vector_chunks, next_input_vector_chunks, vid_range, MaxNumBytesToPin, !NeedUnpin, update_version_range, e_type) == ON_GOING) {

				// Foreach j in [1, q*]
				for (int j = 0; j < sched_output_vector_chunks.size() - 1; j++) {
                    //fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList] i %d j %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL, i, j);
					cur_output_vector_chunks = sched_output_vector_chunks[j];
					int target_machine_id = cur_output_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;
					Range<node_t> target_vid_range = PartitionStatistics::per_machine_vid_range(target_machine_id);
					next_output_vector_chunks = sched_output_vector_chunks[j + 1];

					per_level_timer[lv-1].start_timer(4);
					InitializeOutputVectors (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
					ReadVectorsFromRemote (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
					per_level_timer[lv-1].stop_timer(4);

					user_program_->set_context(cur_input_vector_chunks, cur_output_vector_chunks);

					PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
					PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
					PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

					PartitionID start_dst_vector_chunk_id = sched_output_vector_chunks[0].GetBegin();
					PartitionID cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
					PartitionID next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

					per_level_timer[lv-1].start_timer(6);
					// Nesting; ProcessNestedWindowedStreamFullList
					if (lv != UserArguments::MAX_LEVEL) {
                        ProcessNestedWindowedStreamFullList(lv + 1, target_machine_id, update_version_range);
					    
                    }
					UserArguments::CURRENT_LEVEL = lv - 1;
					per_level_timer[lv-1].stop_timer(6);

					per_level_timer[lv-1].start_timer(7);
					// AsyncSend(LGB^l)
                    bool overflow = false;
					for (int task_id = 0; task_id < PartitionStatistics::num_subchunks_per_edge_chunk(); task_id++) {
			            #pragma omp parallel num_threads(UserArguments::NUM_THREADS) reduction(|:overflow)
                        {
                            overflow = adj_win[UserArguments::CURRENT_LEVEL].FlushUpdateBufferPerThread(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id);
                        }
						adj_win[UserArguments::CURRENT_LEVEL].FlushUpdateBuffer(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id, overflow);
					}
					FlushOutputVectorLocalGatherBuffer (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
					per_level_timer[lv-1].stop_timer(7);
					ALWAYS_ASSERT (cur_output_vector_chunks.GetBegin() == cur_output_vector_chunks.GetEnd());
                }
                if (lv + 1 == UserArguments::MAX_LEVEL) {
                    adj_win[UserArguments::MAX_LEVEL - 1].WaitAllTasks();
                }
                FlushInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
            }
            per_level_timer[lv-1].stop_timer(3);
            UserArguments::CURRENT_LEVEL = lv - 1;
        }
		per_level_timer[lv-1].stop_timer(1);

		per_level_timer[lv-1].start_timer(8);
		if (lv == 1) {
			ProcessApply();
            for (int lv = 0; lv < UserArguments::MAX_LEVEL - 1; lv++) {
                if (lv == 0) MaterializedAdjacencyLists::VidToAdjTable.SetWindowFinalized(false, lv);
                // if not caching the mat.adjlists, Clear the window memory of the current level
                // VidToAdjacencyListMappingTable::VidToAdjTable.Clear(lv);
            }
        }
		per_level_timer[lv-1].stop_timer(8);
        
        per_level_timer[lv-1].stop_timer(0);

        ReportTimers(false);
    }

	virtual void ProcessNestedWindowedStreamPartialList (int lv, int machine_id, Range<int> update_version_range, EdgeType e_type = OUTEDGE, DynamicDBType d_type = INSERT);

#else
	virtual void ProcessNestedWindowedStreamFullList (int lv, int machine_id, Range<int> update_version_range, EdgeType e_type = OUTEDGE) {
		ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == FULL_LIST);
		per_level_timer[lv-1].start_timer(0);

        //fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL);

		std::vector<Range<int64_t>> sched_input_vector_chunks;
		std::vector<Range<int64_t>> sched_output_vector_chunks;
		std::vector<bool> sched_input_vector_chunks_active_flag;
		std::vector<bool> sched_output_vector_chunks_active_flag;

		Range<int64_t> cur_input_vector_chunks;
		Range<int64_t> next_input_vector_chunks;
		Range<int64_t> cur_output_vector_chunks;
		Range<int64_t> next_output_vector_chunks;

		Range<node_t> vid_range;
		UserArguments::CURRENT_LEVEL = lv - 1;
		bool NeedUnpin = (lv == UserArguments::MAX_LEVEL);
		if (lv == 1 && UserArguments::UPDATE_VERSION >= 1 && UserArguments::RUN_SUBQUERIES) {
			for (int subquery_idx = 0; subquery_idx < UserArguments::NUM_TOTAL_SUBQUERIES; subquery_idx++) {
				UserArguments::CURRENT_SUBQUERY_IDX = subquery_idx;
				fprintf(stdout, "[%ld] Process SubQuery %d\n", PartitionStatistics::my_machine_id(), subquery_idx);
				int64_t MaxNumBytesToPin = (NeedUnpin == false) ? adj_win[UserArguments::CURRENT_LEVEL].GetMemoryAllocator()->GetSize() : std::numeric_limits<int64_t>::max();
				ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);

				per_level_timer[lv-1].start_timer(1);

				for (int i = 0; i < sched_input_vector_chunks.size() - 1; i++) {
					//fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList] i %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL, i);
					// READ(vs^l.NextWindowRange())
					cur_input_vector_chunks = sched_input_vector_chunks[i];
					next_input_vector_chunks = sched_input_vector_chunks[i + 1];

					PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
					PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
					PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

					vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, cur_src_vector_chunk_id % UserArguments::VECTOR_PARTITIONS);

					per_level_timer[lv-1].start_timer(2);
					ReadInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
					per_level_timer[lv-1].stop_timer(2);

					// While (asjw^l = AsyncRead(...))
					// Un-Finalize LV-'lv' window
					per_level_timer[lv-1].start_timer(3);
					while (adj_win[UserArguments::CURRENT_LEVEL].RunScatterGather(cur_input_vector_chunks, next_input_vector_chunks, vid_range, MaxNumBytesToPin, !NeedUnpin, update_version_range, e_type) == ON_GOING) {

						// Foreach j in [1, q*]
						for (int j = 0; j < sched_output_vector_chunks.size() - 1; j++) {
							//fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList] i %d j %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL, i, j);
							cur_output_vector_chunks = sched_output_vector_chunks[j];
							int target_machine_id = cur_output_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;
							Range<node_t> target_vid_range = PartitionStatistics::per_machine_vid_range(target_machine_id);
							next_output_vector_chunks = sched_output_vector_chunks[j + 1];

							per_level_timer[lv-1].start_timer(4);
							InitializeOutputVectors (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
							ReadVectorsFromRemote (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
							per_level_timer[lv-1].stop_timer(4);

							user_program_->set_context(cur_input_vector_chunks, cur_output_vector_chunks);

							PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
							PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
							PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

							PartitionID start_dst_vector_chunk_id = sched_output_vector_chunks[0].GetBegin();
							PartitionID cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
							PartitionID next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

							per_level_timer[lv-1].start_timer(6);
							// Nesting; ProcessNestedWindowedStreamFullList
							if (lv != UserArguments::MAX_LEVEL) {
								ProcessNestedWindowedStreamFullList(lv + 1, target_machine_id, update_version_range);

							}
							UserArguments::CURRENT_LEVEL = lv - 1;
							per_level_timer[lv-1].stop_timer(6);

							per_level_timer[lv-1].start_timer(7);
							// AsyncSend(LGB^l)
							bool overflow = false;
							for (int task_id = 0; task_id < PartitionStatistics::num_subchunks_per_edge_chunk(); task_id++) {
#pragma omp parallel num_threads(UserArguments::NUM_THREADS) reduction(|:overflow)
								{
									overflow = adj_win[UserArguments::CURRENT_LEVEL].FlushUpdateBufferPerThread(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id);
								}
								adj_win[UserArguments::CURRENT_LEVEL].FlushUpdateBuffer(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id, overflow);
							}
							FlushOutputVectorLocalGatherBuffer (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
							per_level_timer[lv-1].stop_timer(7);
							ALWAYS_ASSERT (cur_output_vector_chunks.GetBegin() == cur_output_vector_chunks.GetEnd());
						}
						if (lv + 1 == UserArguments::MAX_LEVEL) {
							adj_win[UserArguments::MAX_LEVEL - 1].WaitAllTasks();
						}
						FlushInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
					}
					per_level_timer[lv-1].stop_timer(3);
					UserArguments::CURRENT_LEVEL = lv - 1;
				}
				per_level_timer[lv-1].stop_timer(1);

				per_level_timer[lv-1].start_timer(8);
				if (lv == 1) {
					ProcessApplyTemp();
					for (int lv_ = 0; lv_ < UserArguments::MAX_LEVEL - 1; lv_++) {
						if (lv_ == 0) MaterializedAdjacencyLists::VidToAdjTable.SetWindowFinalized(false, lv_);
						// if not caching the mat.adjlists, Clear the window memory of the current level
						// VidToAdjacencyListMappingTable::VidToAdjTable.Clear(lv);
					}
				}
				per_level_timer[lv-1].stop_timer(8);
			}
			if (lv == 1) {
				ProcessApplyTemp2();
				for (int lv_ = 0; lv_ < UserArguments::MAX_LEVEL - 1; lv_++) {
					if (lv_ == 0) MaterializedAdjacencyLists::VidToAdjTable.SetWindowFinalized(false, lv_);
					// if not caching the mat.adjlists, Clear the window memory of the current level
					// VidToAdjacencyListMappingTable::VidToAdjTable.Clear(lv);
				}
			}
		} else {
			int64_t MaxNumBytesToPin = (NeedUnpin == false) ? adj_win[UserArguments::CURRENT_LEVEL].GetMemoryAllocator()->GetSize() : std::numeric_limits<int64_t>::max();
			ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);

			per_level_timer[lv-1].start_timer(1);

			for (int i = 0; i < sched_input_vector_chunks.size() - 1; i++) {
				//fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList] i %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL, i);
				// READ(vs^l.NextWindowRange())
				cur_input_vector_chunks = sched_input_vector_chunks[i];
				next_input_vector_chunks = sched_input_vector_chunks[i + 1];

				PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
				PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
				PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

				vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, cur_src_vector_chunk_id % UserArguments::VECTOR_PARTITIONS);

				per_level_timer[lv-1].start_timer(2);
				ReadInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
				per_level_timer[lv-1].stop_timer(2);

				// While (asjw^l = AsyncRead(...))
				// Un-Finalize LV-'lv' window
				per_level_timer[lv-1].start_timer(3);
				while (adj_win[UserArguments::CURRENT_LEVEL].RunScatterGather(cur_input_vector_chunks, next_input_vector_chunks, vid_range, MaxNumBytesToPin, !NeedUnpin, update_version_range, e_type) == ON_GOING) {

					// Foreach j in [1, q*]
					for (int j = 0; j < sched_output_vector_chunks.size() - 1; j++) {
						//fprintf(stdout, "[%ld] %ld [%ld/%ld] [ProcessNestedWindowedStreamFullList] i %d j %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, lv, UserArguments::MAX_LEVEL, i, j);
						cur_output_vector_chunks = sched_output_vector_chunks[j];
						int target_machine_id = cur_output_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;
						Range<node_t> target_vid_range = PartitionStatistics::per_machine_vid_range(target_machine_id);
						next_output_vector_chunks = sched_output_vector_chunks[j + 1];

						per_level_timer[lv-1].start_timer(4);
						InitializeOutputVectors (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
						ReadVectorsFromRemote (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
						per_level_timer[lv-1].stop_timer(4);

						user_program_->set_context(cur_input_vector_chunks, cur_output_vector_chunks);

						PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
						PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
						PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

						PartitionID start_dst_vector_chunk_id = sched_output_vector_chunks[0].GetBegin();
						PartitionID cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
						PartitionID next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

						per_level_timer[lv-1].start_timer(6);
						// Nesting; ProcessNestedWindowedStreamFullList
						if (lv != UserArguments::MAX_LEVEL) {
							ProcessNestedWindowedStreamFullList(lv + 1, target_machine_id, update_version_range);

						}
						UserArguments::CURRENT_LEVEL = lv - 1;
						per_level_timer[lv-1].stop_timer(6);

						per_level_timer[lv-1].start_timer(7);
						// AsyncSend(LGB^l)
						bool overflow = false;
						for (int task_id = 0; task_id < PartitionStatistics::num_subchunks_per_edge_chunk(); task_id++) {
#pragma omp parallel num_threads(UserArguments::NUM_THREADS) reduction(|:overflow)
							{
								overflow = adj_win[UserArguments::CURRENT_LEVEL].FlushUpdateBufferPerThread(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id);
							}
							adj_win[UserArguments::CURRENT_LEVEL].FlushUpdateBuffer(start_dst_vector_chunk_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, task_id, overflow);
						}
						FlushOutputVectorLocalGatherBuffer (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
						per_level_timer[lv-1].stop_timer(7);
						ALWAYS_ASSERT (cur_output_vector_chunks.GetBegin() == cur_output_vector_chunks.GetEnd());
					}
					if (lv + 1 == UserArguments::MAX_LEVEL) {
						adj_win[UserArguments::MAX_LEVEL - 1].WaitAllTasks();
					}
					FlushInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
				}
				per_level_timer[lv-1].stop_timer(3);
				UserArguments::CURRENT_LEVEL = lv - 1;
			}
			per_level_timer[lv-1].stop_timer(1);

			per_level_timer[lv-1].start_timer(8);
			if (lv == 1) {
				ProcessApply();
				for (int lv_ = 0; lv_ < UserArguments::MAX_LEVEL - 1; lv_++) {
					if (lv_ == 0) MaterializedAdjacencyLists::VidToAdjTable.SetWindowFinalized(false, lv_);
					// if not caching the mat.adjlists, Clear the window memory of the current level
					// VidToAdjacencyListMappingTable::VidToAdjTable.Clear(lv);
				}
			}
			per_level_timer[lv-1].stop_timer(8);
		}

		per_level_timer[lv-1].stop_timer(0);

		ReportTimers(false);
	}

	virtual void ProcessNestedWindowedStreamPartialList (int lv, int machine_id, Range<int> update_version_range, EdgeType e_type = OUTEDGE, DynamicDBType d_type = INSERT) {
		ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == PARTIAL_LIST);
				
		per_level_timer[lv-1].start_timer(0);


		Range<node_t> vid_range;
		UserArguments::CURRENT_LEVEL = lv - 1;
		bool NeedUnpin = (lv == UserArguments::MAX_LEVEL);
		if (lv == 1 && UserArguments::UPDATE_VERSION >= 1 && UserArguments::RUN_SUBQUERIES) {
			for (int subquery_idx = 0; subquery_idx < UserArguments::NUM_TOTAL_SUBQUERIES; subquery_idx++) {
				UserArguments::CURRENT_SUBQUERY_IDX = subquery_idx;
				
				std::vector<Range<int64_t>> sched_input_vector_chunks;
				TG_Vector<Range<int64_t>> sched_output_vector_chunks;
				std::vector<bool> sched_input_vector_chunks_active_flag;
				TG_Vector<bool> sched_output_vector_chunks_active_flag;

				Range<int64_t> cur_input_vector_chunks;
				Range<int64_t> next_input_vector_chunks;
				Range<int64_t> cur_output_vector_chunks;
				Range<int64_t> next_output_vector_chunks;
				std::queue<std::future<void>> reqs_to_wait;
				PageID MaxNumPagesToPin = (NeedUnpin == false) ? TurboDB::GetBufMgr()->GetNumberOfAvailableFrames() - (UserArguments::NUM_THREADS * 16) : std::numeric_limits<PageID>::max();

				ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);
				fprintf(stdout, "[%ld] Process SubQuery %d sched_input_vector_chunks.size = %ld, sched_output_vector_chunks.size = %ld\n", PartitionStatistics::my_machine_id(), subquery_idx, sched_input_vector_chunks.size(), sched_output_vector_chunks.size());

				per_level_timer[lv-1].start_timer(2);
				for (int i = 0; i < sched_input_vector_chunks.size() - 1; i++) {
					cur_input_vector_chunks = sched_input_vector_chunks[i];
					next_input_vector_chunks = sched_input_vector_chunks[i + 1];

					PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
					PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
					PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

					vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, cur_src_vector_chunk_id % UserArguments::VECTOR_PARTITIONS);

					per_level_timer[lv-1].start_timer(3);
					ReadInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
					per_level_timer[lv-1].stop_timer(3);

					per_level_timer[lv-1].start_timer(5);
					for (int j = 0; j < sched_output_vector_chunks.size() - 1; j++) {
						cur_output_vector_chunks = sched_output_vector_chunks[j];
						int target_machine_id = cur_output_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;
						Range<node_t> target_vid_range = PartitionStatistics::per_machine_vid_range(target_machine_id);
						next_output_vector_chunks = sched_output_vector_chunks[j + 1];

						per_level_timer[lv-1].start_timer(6);
						InitializeOutputVectors (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
						per_level_timer[lv-1].stop_timer(6);
						per_level_timer[lv-1].start_timer(11);
						ReadVectorsFromRemote (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
						per_level_timer[lv-1].stop_timer(11);

						user_program_->set_context(cur_input_vector_chunks, cur_output_vector_chunks);

						PartitionID start_dst_vector_chunk_id = sched_output_vector_chunks[0].GetBegin();
						PartitionID cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
						PartitionID next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

						per_level_timer[lv-1].start_timer(7);
						if (lv == 1 && UserArguments::MAX_LEVEL == 1) {
							per_level_timer[lv-1].start_timer(4);
							if (UserArguments::GRAPH_IN_MEMORY) {
								while (adj_win[UserArguments::CURRENT_LEVEL].RunLocalScatterGatherInMemory(sched_output_vector_chunks[0], cur_input_vector_chunks, cur_output_vector_chunks, next_input_vector_chunks, next_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type) == ON_GOING) {
									if (lv != UserArguments::MAX_LEVEL) {
										ProcessNestedWindowedStreamPartialList(lv + 1, target_machine_id, update_version_range, e_type, d_type);
									}
									UserArguments::CURRENT_LEVEL = lv - 1;
								}
							} else {
								// Optimization - LocalScatterGather (Partial List Iterating Mode) with MAX_LEVEL == 1
								if (j == 0 && UserArguments::MAX_LEVEL == 1) {
									// TODO - can we overlap the below operations (IdentifyAnd..., RunPrefetching...)
									per_level_timer[lv-1].start_timer(1);
									Range<int64_t> total_output_vector_chunks(0, PartitionStatistics::num_target_vector_chunks() - 1);
									adj_win[UserArguments::CURRENT_LEVEL].IdentifyAndCountInputVerticesAndPages(cur_input_vector_chunks, total_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type);
									per_level_timer[lv-1].stop_timer(1);
									reqs_to_wait.push(Aio_Helper::EnqueueAsynchronousTask(TG_AdjWindow<UserProgram_t>::RunPrefetchingDiskIoForLocalScatterGather, &adj_win[UserArguments::CURRENT_LEVEL], cur_input_vector_chunks, &sched_output_vector_chunks, &sched_output_vector_chunks_active_flag, Range<int64_t>(0, PartitionStatistics::num_subchunks_per_edge_chunk() - 1), update_version_range, e_type, d_type));
								}
								per_level_timer[lv-1].stop_timer(4);

								while (adj_win[UserArguments::CURRENT_LEVEL].RunLocalScatterGather(sched_output_vector_chunks[0], cur_input_vector_chunks, cur_output_vector_chunks, next_input_vector_chunks, next_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type) == ON_GOING) {
									if (lv != UserArguments::MAX_LEVEL) {
										ProcessNestedWindowedStreamPartialList(lv + 1, target_machine_id, update_version_range, e_type, d_type);
									}
									UserArguments::CURRENT_LEVEL = lv - 1;
								}
							}
						} else {
						}
						UserArguments::CURRENT_LEVEL = lv - 1;
						per_level_timer[lv-1].stop_timer(7);

						per_level_timer[lv-1].start_timer(8);
						FlushOutputVectorLocalGatherBuffer (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
						ALWAYS_ASSERT (cur_output_vector_chunks.GetBegin() == cur_output_vector_chunks.GetEnd());
						per_level_timer[lv-1].stop_timer(8);
					}
					while (reqs_to_wait.size() > 0) {
						reqs_to_wait.front().get();
						reqs_to_wait.pop();
					}
					FlushInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
					per_level_timer[lv-1].stop_timer(5);
				}
				per_level_timer[lv-1].start_timer(10);
				adj_win[UserArguments::CURRENT_LEVEL].EmptyProcessedPageQueue(e_type, d_type);
				per_level_timer[lv-1].stop_timer(10);

				per_level_timer[lv-1].start_timer(9);
				if (lv == 1) ProcessApplyTemp();
				per_level_timer[lv-1].stop_timer(9);
			}
			per_level_timer[lv-1].start_timer(9);
			if (lv == 1) ProcessApplyTemp2();// BARRIER
			per_level_timer[lv-1].stop_timer(9);
		} else {
			std::vector<Range<int64_t>> sched_input_vector_chunks;
			TG_Vector<Range<int64_t>> sched_output_vector_chunks;
			std::vector<bool> sched_input_vector_chunks_active_flag;
			TG_Vector<bool> sched_output_vector_chunks_active_flag;

			Range<int64_t> cur_input_vector_chunks;
			Range<int64_t> next_input_vector_chunks;
			Range<int64_t> cur_output_vector_chunks;
			Range<int64_t> next_output_vector_chunks;
			std::queue<std::future<void>> reqs_to_wait;
			PageID MaxNumPagesToPin = (NeedUnpin == false) ? TurboDB::GetBufMgr()->GetNumberOfAvailableFrames() - (UserArguments::NUM_THREADS * 16) : std::numeric_limits<PageID>::max();

			ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);

			per_level_timer[lv-1].start_timer(2);
			for (int i = 0; i < sched_input_vector_chunks.size() - 1; i++) {
				cur_input_vector_chunks = sched_input_vector_chunks[i];
				next_input_vector_chunks = sched_input_vector_chunks[i + 1];

				PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
				PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
				PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

				vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, cur_src_vector_chunk_id % UserArguments::VECTOR_PARTITIONS);

				per_level_timer[lv-1].start_timer(3);
				ReadInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
				per_level_timer[lv-1].stop_timer(3);

				per_level_timer[lv-1].start_timer(5);
				for (int j = 0; j < sched_output_vector_chunks.size() - 1; j++) {
					cur_output_vector_chunks = sched_output_vector_chunks[j];
					int target_machine_id = cur_output_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;
					Range<node_t> target_vid_range = PartitionStatistics::per_machine_vid_range(target_machine_id);
					next_output_vector_chunks = sched_output_vector_chunks[j + 1];

					per_level_timer[lv-1].start_timer(6);
					InitializeOutputVectors (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
					per_level_timer[lv-1].stop_timer(6);
					per_level_timer[lv-1].start_timer(11);
					ReadVectorsFromRemote (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
					per_level_timer[lv-1].stop_timer(11);

					user_program_->set_context(cur_input_vector_chunks, cur_output_vector_chunks);

					PartitionID start_dst_vector_chunk_id = sched_output_vector_chunks[0].GetBegin();
					PartitionID cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
					PartitionID next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

					per_level_timer[lv-1].start_timer(7);
					if (lv == 1 && UserArguments::MAX_LEVEL == 1) {
						per_level_timer[lv-1].start_timer(4);
						if (UserArguments::GRAPH_IN_MEMORY) {
							while (adj_win[UserArguments::CURRENT_LEVEL].RunLocalScatterGatherInMemory(sched_output_vector_chunks[0], cur_input_vector_chunks, cur_output_vector_chunks, next_input_vector_chunks, next_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type) == ON_GOING) {
								if (lv != UserArguments::MAX_LEVEL) {
									ProcessNestedWindowedStreamPartialList(lv + 1, target_machine_id, update_version_range, e_type, d_type);
								}
								UserArguments::CURRENT_LEVEL = lv - 1;
							}
						} else {
							// Optimization - LocalScatterGather (Partial List Iterating Mode) with MAX_LEVEL == 1
							if (j == 0 && UserArguments::MAX_LEVEL == 1) {
								// TODO - can we overlap the below operations (IdentifyAnd..., RunPrefetching...)
								per_level_timer[lv-1].start_timer(1);
								Range<int64_t> total_output_vector_chunks(0, PartitionStatistics::num_target_vector_chunks() - 1);
								adj_win[UserArguments::CURRENT_LEVEL].IdentifyAndCountInputVerticesAndPages(cur_input_vector_chunks, total_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type);
								per_level_timer[lv-1].stop_timer(1);
								reqs_to_wait.push(Aio_Helper::EnqueueAsynchronousTask(TG_AdjWindow<UserProgram_t>::RunPrefetchingDiskIoForLocalScatterGather, &adj_win[UserArguments::CURRENT_LEVEL], cur_input_vector_chunks, &sched_output_vector_chunks, &sched_output_vector_chunks_active_flag, Range<int64_t>(0, PartitionStatistics::num_subchunks_per_edge_chunk() - 1), update_version_range, e_type, d_type));
							}
							per_level_timer[lv-1].stop_timer(4);

							while (adj_win[UserArguments::CURRENT_LEVEL].RunLocalScatterGather(sched_output_vector_chunks[0], cur_input_vector_chunks, cur_output_vector_chunks, next_input_vector_chunks, next_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type) == ON_GOING) {
								if (lv != UserArguments::MAX_LEVEL) {
									ProcessNestedWindowedStreamPartialList(lv + 1, target_machine_id, update_version_range, e_type, d_type);
								}
								UserArguments::CURRENT_LEVEL = lv - 1;
							}
						}
					} else {
					}
					UserArguments::CURRENT_LEVEL = lv - 1;
					per_level_timer[lv-1].stop_timer(7);

					per_level_timer[lv-1].start_timer(8);
					FlushOutputVectorLocalGatherBuffer (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
					ALWAYS_ASSERT (cur_output_vector_chunks.GetBegin() == cur_output_vector_chunks.GetEnd());
					per_level_timer[lv-1].stop_timer(8);
				}
				while (reqs_to_wait.size() > 0) {
					reqs_to_wait.front().get();
					reqs_to_wait.pop();
				}
				FlushInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
				per_level_timer[lv-1].stop_timer(5);
			}
			per_level_timer[lv-1].start_timer(10);
			adj_win[UserArguments::CURRENT_LEVEL].EmptyProcessedPageQueue(e_type, d_type);
			per_level_timer[lv-1].stop_timer(10);

			per_level_timer[lv-1].start_timer(9);
			if (lv == 1) ProcessApply();// BARRIER
			per_level_timer[lv-1].stop_timer(9);
		}
		per_level_timer[lv-1].stop_timer(2);

		per_level_timer[lv-1].stop_timer(0);
		
        //ReportTimers(false);
		return;
	}
#endif
	
    virtual void PrintEdgeWindowIoInformation() {
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
			adj_win[lv].PrintEdgeWindowIoInformation();
		}
	}

	void GlobalSynchronization() {
		PartitionStatistics::wait_for_all(); // BARRIER, 없앨 수 있나
		
        apply_timer.start_timer(9);
		user_program_->wait_for_vector_io();
		apply_timer.stop_timer(9);

		apply_timer.start_timer(10);
		user_program_->wait_on_rr(false);
		apply_timer.stop_timer(10);
		
#ifndef OPTIMIZE_WINDOW_SHARING
		PartitionStatistics::wait_for_all(); // BARRIER, 없앨 수 있나 //XXX tslee add
#endif
	}
	
	void ProcessApplyTemp() {
		apply_timer.start_timer(0);

		// Global Synchronization Barrier
		apply_timer.start_timer(1);
		GlobalSynchronization();// BARRIER
		apply_timer.stop_timer(1);

		// Update Phase
		apply_timer.start_timer(3);
		ClearAll();
		apply_timer.stop_timer(3);
        apply_timer.stop_timer(0);
	}
	void ProcessApplyTemp2() {
		// Update Phase
		apply_timer.start_timer(3);
		UpdatePhaseHJ();
		apply_timer.stop_timer(3);
	}

	void ProcessApply() {
		apply_timer.start_timer(0);

		// Global Synchronization Barrier
		apply_timer.start_timer(1);
		GlobalSynchronization();// BARRIER
		apply_timer.stop_timer(1);
		// Update Phase
		apply_timer.start_timer(3);
		ClearAll();
		UpdatePhaseHJ();
		apply_timer.stop_timer(3);
        apply_timer.stop_timer(0);
	}


	virtual void ReadInputVectorsAtApplyPhase (int vector_chunk_idx) {
        for(auto& vec : user_program_->Vectors() ) {
            vec->ReadInputVectorsAtApplyPhase(vector_chunk_idx);
        }
    }
	
    virtual void ReadInputVectors (std::vector<TG_DistributedVectorBase*>& vectors, Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) {
        int start_partition_id = start_input_vector_chunks.GetBegin();
        int cur_partition_id = cur_input_vector_chunks.GetBegin();
        int next_partition_id = next_input_vector_chunks.GetBegin();

        for(auto& vec : vectors) {
            vec->ReadInputVector(start_partition_id, cur_partition_id, next_partition_id, UserArguments::SUPERSTEP);
        }
    }
	
    virtual void ReadInputVectors (TG_DistributedVectorBase* vec, Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) {
        ALWAYS_ASSERT (vec != NULL);
		
        int start_partition_id = start_input_vector_chunks.GetBegin();
        int cur_partition_id = cur_input_vector_chunks.GetBegin();
		int next_partition_id = next_input_vector_chunks.GetBegin();

        vec->ReadInputVector(start_partition_id, cur_partition_id, next_partition_id, UserArguments::SUPERSTEP);
    }

	virtual void ReadInputVectors (Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) {
		int start_partition_id = start_input_vector_chunks.GetBegin();
        int cur_partition_id = cur_input_vector_chunks.GetBegin();
		int next_partition_id = next_input_vector_chunks.GetBegin();

        for(auto& vec : user_program_->Vectors() ) {
            vec->ReadInputVector(start_partition_id, cur_partition_id, next_partition_id, UserArguments::SUPERSTEP);
        }
	}

	void FlushInputVectors (Range<int64_t> start_input_vector_chunks, Range<int64_t> cur_input_vector_chunks, Range<int64_t> next_input_vector_chunks) {
	    // Doing Nothing
    }
    
    virtual void FlushInputVectorsAfterApplyPhase (std::vector<TG_DistributedVectorBase*>& vectors, int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) {
        for(auto& vec : vectors) {
            vec->FlushInputVector(vector_chunk_idx, superstep, iv_delta_write_if);
        }
    }
    
    virtual void FlushInputVectorsAfterApplyPhase (TG_DistributedVectorBase* vec, int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) {
        ALWAYS_ASSERT (vec != NULL);
        vec->FlushInputVector(vector_chunk_idx, superstep, iv_delta_write_if);
    }

    virtual void FlushInputVectorsAfterApplyPhase (int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) {
        for(auto& vec : user_program_->Vectors() ) {
            vec->FlushInputVector(vector_chunk_idx, superstep, iv_delta_write_if);
        }
    }
    virtual void FlushInputVectorsAndReadAndMergeAfterApplyPhase (int vector_chunk_idx, int superstep, const std::function<bool(node_t)>& iv_delta_write_if) {
        for(auto& vec : user_program_->Vectors() ) {
            vec->FlushInputVectorAndReadAndMerge(vector_chunk_idx, superstep, iv_delta_write_if);
        }
    }

	void InitializeOutputVectors (Range<int64_t> start_output_vector_chunks, Range<int64_t> cur_output_vector_chunks, Range<int64_t> next_output_vector_chunks) {
        turbo_timer timer;
        int start_partition_id = start_output_vector_chunks.GetBegin();
        int cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
		int next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

        timer.start_timer(0);
        bool clear_lgb = false;
        for(auto& vec : user_program_->Vectors() ) {
            clear_lgb |= vec->InitializeOutputVector(start_partition_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id);
        }
        timer.stop_timer(0);

        timer.start_timer(1);
        if (TG_DistributedVectorBase::lgb_dirty_flags[TG_DistributedVectorBase::lgb_toggle].data() != NULL && !UserArguments::USE_PULL) {
            if (clear_lgb) {
                //fprintf(stdout, "ClearAll LGB[%d]\n", TG_DistributedVectorBase::lgb_toggle);
                TG_DistributedVectorBase::lgb_dirty_flags[TG_DistributedVectorBase::lgb_toggle].ClearAll();
            }
        }
        timer.stop_timer(1);
        
#ifdef REPORT_PROFILING_TIMERS
        //fprintf(stdout, "[%ld] InitializeOV %.4f %.4f\n", PartitionStatistics::my_machine_id(), timer.get_timer(0), timer.get_timer(1));
#endif
    }
	
    static void ReadGGB (UserProgram_t* user_program, bool IncrementalProcessing, int up, int ss) {
        if (!IncrementalProcessing) {
            TG_NWSM_Base::read_ggb_flag.store(0L);
            return;
        }
        for (auto& vec : user_program->Vectors() ) {
            vec->ReadGGB(up, ss);
        }
        TG_NWSM_Base::read_ggb_flag.store(0L);
    }
	
    void ReadVectorsFromRemote (Range<int64_t> start_output_vector_chunks, Range<int64_t> cur_output_vector_chunks, Range<int64_t> next_output_vector_chunks) {
		int start_partition_id = start_output_vector_chunks.GetBegin();
        int cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
		int next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

        for(auto& vec : user_program_->Vectors() ) {
            if (UserArguments::USE_PULL) {
                vec->PullInputVector(start_partition_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id);
            }
        }
    }

	void FlushOutputVectorLocalGatherBuffer (Range<int64_t> start_output_vector_chunks, Range<int64_t> cur_output_vector_chunks, Range<int64_t> next_output_vector_chunks) {
        int start_partition_id = start_output_vector_chunks.GetBegin();
        int cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
		int next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

        bool toggle_lgb = false;

        for(auto& vec : user_program_->Vectors() ) {
            toggle_lgb |= vec->FlushLocalGatherBuffer(start_partition_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, 0);
            vec->ClearPulledMessagesInLocalGatherBuffer(start_partition_id, cur_dst_vector_chunk_id, next_dst_vector_chunk_id, 0);
        }
        if (!UserArguments::USE_PULL && TG_DistributedVectorBase::lgb_dirty_flags[0].data() != NULL) {
            if (toggle_lgb) TG_DistributedVectorBase::ToggleLgb();
        }
    }


	void ScheduleVectorWindow(int machine_id, std::vector<Range<int64_t>>& sched_input_vector_chunks, std::vector<Range<int64_t>>& sched_output_vector_chunks, std::vector<bool>& sched_input_vector_chunks_active_flag, std::vector<bool>& sched_output_vector_chunks_active_flag) {
		user_program_->ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);
	}
	
    virtual ReturnStatus UpdatePhaseHJWithoutScatterGather(int skip_scatter_gather=0) {
        bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
        UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
        
        //system_fprintf(0, stdout, "[%ld][UpdatePhaseHJWithoutScatterGather] %p\n", PartitionStatistics::my_machine_id(), CallBeforeFlushIv);
        
        turbo_timer update_phase_timer;
        update_phase_timer.start_timer(0);
        for (int64_t j = 0 ; j <  ((this->user_program_)->Vectors()).size() ; j++) {
            ((this->user_program_)->Vectors())[j]->CreateIvForNextSuperstep(UserArguments::SUPERSTEP+1);
        }
        update_phase_timer.stop_timer(0);

        update_phase_timer.start_timer(1);
        if (AdvanceIv && UserArguments::OV_FLUSH) {
            for (int64_t j = 0 ; j <  ((this->user_program_)->Vectors()).size() ; j++) {
                ((this->user_program_)->Vectors())[j]->FlushOv(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
            }
        }
        update_phase_timer.stop_timer(1);

        for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
            Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + chunk_idx);
            node_t my_first_vid = PartitionStatistics::my_first_internal_vid();
            Range<int64_t> idx_range(vid_range.GetBegin() - my_first_vid, vid_range.GetEnd() - my_first_vid);
                
            update_phase_timer.start_timer(6);
            adj_win[0].ClearInputNodeBitMap(idx_range);
            update_phase_timer.stop_timer(6);
            // XXX very ugly
            update_phase_timer.start_timer(2);
            if (UserArguments::INC_STEP == -1) {
                if (CallBeforeFlushIv) CallBeforeFlushIv();

            }
            update_phase_timer.stop_timer(2);
            update_phase_timer.start_timer(3);
            if (UserArguments::INC_STEP == 4 && AdvanceIv) {
                if (CallBeforeFlushIv) CallBeforeFlushIv();
            }
            update_phase_timer.stop_timer(3);

            update_phase_timer.start_timer(4);
            if (AdvanceIv) {
                FlushInputVectorsAndReadAndMergeAfterApplyPhase(chunk_idx, UserArguments::SUPERSTEP + 1, IvDeltaWriteCondition);
            }
            update_phase_timer.stop_timer(4);
        }
        
        update_phase_timer.start_timer(5);
        if (AdvanceIv) {
            for(auto& vec : user_program_->Vectors() ) {
                if (skip_scatter_gather == 0)
                    vec->ClearOvFlagsForIncrementalProcessing();
                vec->CreateOvForNextSuperstep(UserArguments::SUPERSTEP+1);
            }
        }
        update_phase_timer.stop_timer(5);

        UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;

//#ifdef PRINT_PROFILING_TIMERS
        fprintf(stdout, "[%ld] UpdatePhaseOnly %.4f %.4f %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), update_phase_timer.get_timer(0), update_phase_timer.get_timer(1), update_phase_timer.get_timer(6), update_phase_timer.get_timer(2), update_phase_timer.get_timer(3), update_phase_timer.get_timer(4), update_phase_timer.get_timer(5));
//#endif
        return OK;
    }

    ReturnStatus UpdatePhaseHJ() {
        bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
        UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
        
        //system_fprintf(0, stdout, "UpdatePhaseHJ (%ld, %ld) Overflow? %ld\n", (int64_t) UserArguments::UPDATE_VERSION, (int64_t) UserArguments::SUPERSTEP, (int64_t) (TG_DistributedVectorBase::update_delta_buffer_overflow? 1L: 0L));
        
        // Versioned Array AdvanceSuperstep
        apply_timer.start_timer(13);
        for (int64_t j = 0 ; j <  ((this->user_program_)->Vectors()).size() ; j++) {
            ((this->user_program_)->Vectors())[j]->CreateIvForNextSuperstep(UserArguments::SUPERSTEP+1);
        }
        apply_timer.stop_timer(13);

        apply_timer.start_timer(5);
        if (!TG_DistributedVectorBase::update_delta_buffer_overflow) {
            TG_DistributedVectorBase::SortAndMergeUpdatedListOfVertices();
        }
        apply_timer.stop_timer(5);

        // Flush OV & Apply
        apply_timer.start_timer(14);
        Range<int64_t> input_vector_chunks = PartitionStatistics::my_vector_chunks();
        for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
            apply_timer.start_timer(6);
            Range<int64_t> start_input_vector_chunks(input_vector_chunks.GetBegin(), input_vector_chunks.GetBegin());
            Range<int64_t> cur_input_vector_chunks(input_vector_chunks.GetBegin() + chunk_idx, input_vector_chunks.GetBegin() + chunk_idx);
            ReadInputVectorsAtApplyPhase(chunk_idx);
            apply_timer.stop_timer(6);

            apply_timer.start_timer(15);
            for (int64_t j = 0 ; j <  ((this->user_program_)->Vectors()).size() ; j++) {
                ((this->user_program_)->Vectors())[j]->ReadAndMergeSpilledWritesFromDisk(chunk_idx);
            }
            apply_timer.stop_timer(15);

            apply_timer.start_timer(11);
            if (UserArguments::OV_FLUSH) {
                for (int64_t j = 0 ; j <  ((this->user_program_)->Vectors()).size() ; j++) {
                    ((this->user_program_)->Vectors())[j]->FlushOv(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
                }
            }
            apply_timer.stop_timer(11);

            apply_timer.start_timer(8);

            Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + chunk_idx);
            node_t my_first_vid = PartitionStatistics::my_first_internal_vid();
            Range<int64_t> idx_range(vid_range.GetBegin() - my_first_vid, vid_range.GetEnd() - my_first_vid);
                
            adj_win[0].ClearInputNodeBitMap(idx_range); 
            int64_t NumChangesElementsInOV = TG_DistributedVectorBase::write_vec_changed_idx.load();
            apply_timer.start_timer(7);

            turbo_timer tslee_fuck;
            
            if (!this->skip_apply_phase && (this->VertexApply != NULL || UserArguments::USE_INCREMENTAL_APPLY)) {
                //if (UserArguments::INC_STEP == -1 && UserArguments::SUPERSTEP != 0) { // XXX for gb pagerank simulation.. gb force apply for first iteration of pagerank
#ifdef TEMP_SINGLE_MACHINE
                if (PartitionStatistics::num_machines() == 1) {
#else
                if (false) {
#endif
                    int64_t ggb_total = 0;
                    //int64_t ggb_total = TG_DistributedVectorBase::GetGgbMsgReceivedFlags()->GetTotal();
                    //tslee_fuck.start_timer(0);
                    TG_DistributedVectorBase::GetGgbMsgReceivedFlags()->TwoLevelBitMap<int64_t>::InvokeIfMarked([&](node_t idx) {
                        node_t vid = idx + PartitionStatistics::my_first_internal_vid();
                        IncVertexApply(vid);
                    });
                    //tslee_fuck.stop_timer(0);
                    //tslee_fuck.start_timer(1);
                    TG_DistributedVectorBase::ClearOvMessageReceivedAll();
                    //tslee_fuck.stop_timer(1);
                    //fprintf(stdout, "Fuck %.3f %.3f total %ld\n", tslee_fuck.get_timer(0), tslee_fuck.get_timer(1), ggb_total);
                } else {
                    if (UserArguments::USE_INCREMENTAL_APPLY) {
                        if (UserArguments::USE_PULL || TG_DistributedVectorBase::update_delta_buffer_overflow) {
                        //if (true) { // XXX
                            int64_t chunk_size = (((vid_range.length() / UserArguments::NUM_THREADS) + 63) / 64) * 64;
//#pragma omp parallel for schedule(static, chunk_size)
#pragma omp parallel for
                            for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
                                IncVertexApply(vid);
                            }
                        } else {
#pragma omp parallel for
                            for(int64_t i = 0 ; i < NumChangesElementsInOV ; i++) {
                                node_t vid = TG_DistributedVectorBase::write_vec_changed[i];
                                if (!vid_range.contains(vid)) continue;
                                IncVertexApply(vid);
                            }
                        }
                    } else {
                        if (UserArguments::USE_PULL || TG_DistributedVectorBase::update_delta_buffer_overflow) {
                        //if (true) {
//#pragma omp parallel for
                            int64_t chunk_size = (((vid_range.length() / UserArguments::NUM_THREADS) + 63) / 64) * 64;
//#pragma omp parallel for schedule(static, chunk_size)
#pragma omp parallel for
                            for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
                                ((this->user_program_)->*VertexApply)(vid);
                            }
                        } else {
#pragma omp parallel for
                            for(int64_t i = 0 ; i < NumChangesElementsInOV ; i++) {
                                node_t vid = TG_DistributedVectorBase::write_vec_changed[i];
                                if (!vid_range.contains(vid)) continue;
                                ((this->user_program_)->*VertexApply)(vid);
                            }
                        }
                    }
                }
            }
            apply_timer.stop_timer(7);
            
            apply_timer.start_timer(12);
            if (UserArguments::INC_STEP == -1) {
                if (CallBeforeFlushIv) CallBeforeFlushIv();
            }
            if (UserArguments::INC_STEP == 4 && AdvanceIv) {
                if (CallBeforeFlushIv) CallBeforeFlushIv();
            }
            apply_timer.stop_timer(12);

            apply_timer.start_timer(17);
            if (AdvanceIv) {
                FlushInputVectorsAndReadAndMergeAfterApplyPhase(chunk_idx, UserArguments::SUPERSTEP + 1, IvDeltaWriteCondition);
            }
            apply_timer.stop_timer(17);

            apply_timer.stop_timer(8);
        }
        apply_timer.start_timer(16);
        if (AdvanceIv) {
            for(auto& vec : user_program_->Vectors() ) {
                vec->ClearOvFlagsForIncrementalProcessing();
                vec->CreateOvForNextSuperstep(UserArguments::SUPERSTEP+1);
            }
        }
        apply_timer.stop_timer(16);
        
        UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
        apply_timer.stop_timer(14);
        return OK;   
    }

  public:

	virtual void Start(Range<int> update_version_range, EdgeType e_type = UserArguments::EDGE_DB_TYPE, DynamicDBType d_type = UserArguments::DYNAMIC_DB_TYPE, std::function<void(node_t)> callback_after_apply = {}, std::function<bool(node_t)> iv_delta_write_if = [](node_t vid){ return true; }, std::function<void()> call_before_flush_iv = {}) {
        InitializeStaticProcessing();	// 안에 BARRIER 있음
        TG_NWSM_Base::read_ggb_flag.store(0L);
        
        UserArguments::USE_INCREMENTAL_SCATTER = false;
        UserArguments::USE_INCREMENTAL_APPLY = true;
        CallBeforeFlushIv = call_before_flush_iv;
        IncVertexApply = callback_after_apply;
        IvDeltaWriteCondition = iv_delta_write_if;
        AdvanceIv = true;
        IncrementalProcessing = false;

        if (!(this->user_program_)->PerSuperstepCheckFinished()) {
            if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) ProcessNestedWindowedStreamPartialList(1, PartitionStatistics::my_machine_id(), update_version_range, e_type, d_type);
            else ProcessNestedWindowedStreamFullList(1, PartitionStatistics::my_machine_id(), update_version_range);
        } else {
            UpdatePhaseHJWithoutScatterGather();
            user_program_->SetFinished(true);
        }
        TG_NWSM_Base::read_ggb_flag.store(1L);
    }
	
    virtual bool Inc_Start(Range<int> update_version_range, EdgeType e_type, DynamicDBType d_type, bool UseIncScatter, bool UseIncApply, bool advance_iv, std::function<void(node_t)> apply = {}, std::function<void(node_t)> callback_after_apply = [](node_t vid){}, std::function<bool(node_t)> iv_delta_write_if = [](node_t vid){ return true; }, std::function<void()> call_before_flush_iv = { }, int skip_scatter_gather=0) {
        //inc_start_timer.clear_all_timers();

        inc_start_timer.start_timer(0);
        user_program_->reset_vector_states();
        //TG_NWSM<UserProgram_t>::read_ggb_flag = true;
        std::future<void> reqs_to_wait = Aio_Helper::EnqueueAsynchronousTask(TG_NWSM::ReadGGB, user_program_, IncrementalProcessing, UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
        //TG_NWSM::ReadGGB(user_program_, IncrementalProcessing, UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
        //ReadGGB();  // TODO - (1) Skip it when 'is_finished == false', (2) Make it asynchronous
        for (int lv = 0; lv < UserArguments::MAX_LEVEL - 1; lv++) {
            if (lv == 0) MaterializedAdjacencyLists::VidToAdjTable.SetWindowFinalized(false, lv);
        }
        inc_start_timer.stop_timer(0);
        
        inc_start_timer.start_timer(5);
//		PartitionStatistics::wait_for_all();	// BARRIER, 이 부분 없앨 수 있음.
        bool is_finished;
        if (skip_scatter_gather == 1) is_finished = true;
        else is_finished = (this->user_program_)->PerSuperstepCheckFinished();	// BARRIER,
        inc_start_timer.stop_timer(5);

        inc_start_timer.start_timer(4);
        reqs_to_wait.get();
        inc_start_timer.stop_timer(4);

        UserArguments::USE_INCREMENTAL_SCATTER = UseIncScatter;
        UserArguments::USE_INCREMENTAL_APPLY = UseIncApply;
        IncVertexApply = apply;
        IvDeltaWriteCondition = iv_delta_write_if;
        AdvanceIv = advance_iv;
        CallBeforeFlushIv = call_before_flush_iv;
        IncrementalProcessing = true;

        bool nwsm_processed = false;
        inc_start_timer.start_timer(6);
        inc_start_timer.stop_timer(6);
        inc_start_timer.start_timer(1);
        if (!is_finished) {
            inc_start_timer.start_timer(2);
            if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
                ProcessNestedWindowedStreamPartialList(1, PartitionStatistics::my_machine_id(), update_version_range, e_type, d_type);
	    } else {
                ProcessNestedWindowedStreamFullList(1, PartitionStatistics::my_machine_id(), update_version_range);
            }
            nwsm_processed = true;
            inc_start_timer.stop_timer(2);
        } else {
            inc_start_timer.start_timer(3);
            UpdatePhaseHJWithoutScatterGather(skip_scatter_gather);
            inc_start_timer.stop_timer(3);
            while (TG_NWSM_Base::read_ggb_flag.load() == 1L) {
                _mm_pause();
            }
        }
        ReportTimers(false);
        inc_start_timer.stop_timer(1);
        UserArguments::USE_INCREMENTAL_SCATTER = false;
        UserArguments::USE_INCREMENTAL_APPLY = false;
        TG_NWSM_Base::read_ggb_flag.store(1L);
        
//#ifdef PRINT_PROFILING_TIMERS
        fprintf(stdout, "[%ld] IncStart %.4f %.4f %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), inc_start_timer.get_timer(0), inc_start_timer.get_timer(4), inc_start_timer.get_timer(5), inc_start_timer.get_timer(1), inc_start_timer.get_timer(2), inc_start_timer.get_timer(3), inc_start_timer.get_timer(6));
//#endif
        return nwsm_processed;
    }
	
    virtual AdjacencyListMemoryPointer& GetAdjList(int lv, node_t vid) {
        if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
            vid = PartitionStatistics::DegreeOrderToVid(vid);
        }
        return VidToAdjacencyListMappingTable::VidToAdjTable.Get(vid, lv - 1);
    }
	
    virtual void GetAdjList(int lv, node_t vid, node_t* data_ptr[], int64_t data_sz[]) {
        if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
            vid = PartitionStatistics::DegreeOrderToVid(vid);
        }
        
        AdjacencyListMemoryPointer& u_ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(vid, lv - 1);
        MaterializedAdjacencyLists* u_adj_ptr = u_ptr.ptr;

        data_ptr[0] = u_adj_ptr->GetBeginPtr(u_ptr.slot_id);
        data_ptr[1] = u_adj_ptr->GetBeginPtrOfDeltaInsert(u_ptr.slot_id);
        data_ptr[2] = u_adj_ptr->GetBeginPtrOfDeltaDelete(u_ptr.slot_id);
        data_sz[0] = u_adj_ptr->GetNumEntriesOfCurrent(u_ptr.slot_id);
        data_sz[1] = u_adj_ptr->GetNumEntriesOfDeltaInsert(u_ptr.slot_id);
        data_sz[2] = u_adj_ptr->GetNumEntriesOfDeltaDelete(u_ptr.slot_id);
        return;
    }

    virtual void GetAdjList(int lv, node_t vid, node_t*& data_ptr, int64_t& data_sz) {
        if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			vid = PartitionStatistics::DegreeOrderToVid(vid);
		}
		VidToAdjacencyListMappingTable::VidToAdjTable.Get(lv-1, vid, data_ptr, data_sz);
		return;
	}
	template <int LV>
	void GetAdjList(node_t vid, node_t*& data_ptr, int64_t& data_sz) {
		if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			vid = PartitionStatistics::DegreeOrderToVid(vid);
		}
		VidToAdjacencyListMappingTable::VidToAdjTable.Get<LV-1>(vid, data_ptr, data_sz);
		return;
	}
	
    inline virtual void ClearAllAtVOI(int lv) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].ClearAllAtVOI();
    }
	
    inline virtual void ClearAtVOI(int lv, node_t vid) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].ClearAtVOI(vid);
    }
    
    virtual TwoLevelBitMap<node_t>& GetDeltaNwsmActiveVerticesBitMap(DynamicDBType type) {
       return adj_win[0].GetDeltaNwsmActiveVerticesBitMap(type); 
    }
    virtual TwoLevelBitMap<node_t>& GetActiveVerticesBitMap() {
       return adj_win[0].GetActiveVerticesBitMap(); 
    }
    
    virtual int64_t GetActiveVerticesTotalCount() {
       return adj_win[0].GetActiveVerticesTotalCount(); 
    }

    inline virtual bool dq_nodes(int lv, int subquery_idx, node_t vid) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		return adj_win[lv - 1].prune_by_VdE(subquery_idx, vid);
    }
    
    inline virtual void set_dq_nodes(int lv, int subquery_idx, node_t vid) {
		adj_win[lv].set_dq_nodes(subquery_idx, vid);
    }

    inline virtual void AllreduceDQNodes(int lv) {
        adj_win[lv].AllreduceDQNodes(adj_win[0].GetPruningSubQueriesFlag());
    }
	
    inline virtual void MarkAllAtVOI(int lv) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].MarkAllAtVOI();
    }

	inline virtual void MarkAtVOI(int lv, node_t vid, bool activate=true) {
        if (!activate) return;
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].MarkAtVOI(vid);
	}
	
    inline virtual void MarkAtVOI(int subquery_idx, int lv, node_t vid, bool activate=true) {
        if (!activate) return;
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].MarkAtVOI(subquery_idx, vid);
	}
	
    inline virtual void MarkAtVOIUnsafe(int lv, node_t vid, bool activate=true) {
        if (!activate) return;
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].MarkAtVOIUnsafe(vid);
	}

	template <int LV>
	bool HasVertexInWindow(node_t vid) {
		if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			vid = PartitionStatistics::DegreeOrderToVid(vid);
		}
		return adj_win[LV-1].HasVertexInWindow(vid);
	}

	bool HasVertexInWindow(int lv, node_t vid) {
		if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			vid = PartitionStatistics::DegreeOrderToVid(vid);
		}
		return adj_win[lv-1].HasVertexInWindow(vid);
	}
	
    bool HasVertexInWindow(int subquery_idx, int lv, node_t vid) {
		if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			vid = PartitionStatistics::DegreeOrderToVid(vid);
		}
		return adj_win[lv-1].HasVertexInWindow(subquery_idx, vid);
	}
    
    virtual void ClearDqNodes() {
      for (int i = 0; i < UserArguments::MAX_LEVEL; i++) {
        adj_win[i].ClearDqNodes();
      }
    }
    
    virtual void MarkStartingVertices(TwoLevelBitMap<int64_t>& Va_new, TwoLevelBitMap<int64_t>& Va_old, TwoLevelBitMap<int64_t>& Vdv, TwoLevelBitMap<int64_t>& Va, TwoLevelBitMap<node_t>** VdE) {
        // Find starting vertices for lv 0 window
        for (int i = 0; i < UserArguments::MAX_LEVEL; i++) {
            if (i == 0) adj_win[i].MarkStartingVertices(Va_new, Va_old, Vdv, Va, VdE);
            //else adj_win[i].ClearDqNodes();
        }
    }
    
    virtual void MarkStartingVerticesPartialListMode(TwoLevelBitMap<int64_t>& Va_new, TwoLevelBitMap<int64_t>& Va_old, TwoLevelBitMap<int64_t>& Vdv, TwoLevelBitMap<int64_t>& Va, TwoLevelBitMap<int64_t>& Va_delta_insert, TwoLevelBitMap<int64_t>& Va_delta_delete, TwoLevelBitMap<node_t>** VdE) {
        // Find starting vertices for lv 0 window
        for (int i = 0; i < UserArguments::MAX_LEVEL; i++) {
            if (i == 0) adj_win[i].MarkStartingVerticesPartialListMode(Va_new, Va_old, Vdv, Va, Va_delta_insert, Va_delta_delete, VdE);
            //else adj_win[i].ClearDqNodes();
        }
    }

	virtual void SetIncScatterFunction(void (TurbographImplementation::*AdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]), int lv) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].SetIncScatterFunction(AdjScatter_);
	}
    virtual void AddScatter(void (UserProgram_t::*AdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]), int lv) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].SetScatterFunction(AdjScatter_);
	}
	virtual void SetScatterFunction_PULL(void (UserProgram_t::*AdjScatter_)(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]), int lv) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].SetScatterFunction_PULL(AdjScatter_);
	}
    virtual void SetApplyFunction(void(UserProgram_t::*VertexApply_)(node_t src_vid)) {
		this->VertexApply = VertexApply_;
	}
    template <typename... Ts>
	void AddIncScatter(SubQueryFunc SubQuery, int lv, int subquery_idx, bool use_Vdv_or_Va, Ts... ts) {
		ALWAYS_ASSERT (lv >= 1 && lv <= UserArguments::MAX_LEVEL);
		adj_win[lv - 1].AddIncScatter(SubQuery, subquery_idx, use_Vdv_or_Va, ts...);
    }
    void AddPruningScatter(SubQueryFunc PruningQuery, int bfs_lv, int pruning_lv, int subquery_idx) {
		ALWAYS_ASSERT (pruning_lv >= 1 && pruning_lv <= UserArguments::MAX_LEVEL);
		adj_win[0].AddPruningScatter(PruningQuery, bfs_lv, pruning_lv, subquery_idx);
    }
	
    // Incremental processing
    virtual bool SkipApplyPhase(bool skip) {
        skip_apply_phase = skip;
        return true;
    }
    
    virtual bool IsActive(node_t vid) {
        return IsMarkedAtVOI(1, vid);
    }

	virtual bool HasActivatedVertex() {
		return adj_win[0].HasActivatedVertex();
	}

	virtual bool IsMarkedAtVOI(int lv, node_t vid) {
		return adj_win[lv - 1].IsMarkedAtVOI(vid);
	}

	Range<node_t> GetInputNodeVidRange(int lv) {
		return adj_win[lv - 1].GetInputNodeVidRange();
	}
	virtual Range<node_t> GetVidRangeBeingProcessed(int lv) {
		return adj_win[lv - 1].GetVidRangeBeingProcessed();
	}

	void ClearAll() {
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
			adj_win[lv].ClearAll();
		}
	}

	// TODO - How to distribute the memory resource?
	void InitializeWindowMemoryAllocator (int64_t total_bytes) {
		ALWAYS_ASSERT (window_memory_ptr == NULL);
		ALWAYS_ASSERT (window_memory_capacity == -1);
		ALWAYS_ASSERT (total_bytes > 0);

		std::vector<int64_t> bytes_per_window;
		user_program_->ApplyWindowMemoryAssignmentPolicy(total_bytes, bytes_per_window);

        if (UserArguments::MAX_LEVEL > 1) VidToAdjacencyListMappingTable::VidToAdjTable.Initialize(UserArguments::MAX_LEVEL);

		// Allocate memory and Initialize the window memory allocator
		char* ptr = NumaHelper::alloc_numa_memory(total_bytes);
		window_memory_ptr = ptr;
		window_memory_capacity = total_bytes;
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
			INVARIANT (bytes_per_window[lv] > 0);
			adj_win[lv].InitializeAdjWindowMemoryAllocator(ptr, bytes_per_window[lv]);
			ptr += bytes_per_window[lv];
			std::string win_name = "AdjWindowBuffer_LV" + std::to_string(lv+1);
			LocalStatistics::register_mem_alloc_info(win_name.c_str(), (bytes_per_window[lv]) / (1024 * 1024));
		}
		for (int lv = 0; lv < UserArguments::MAX_LEVEL - 1; lv++) {
			BlockMemoryStackAllocator* ptr = (BlockMemoryStackAllocator*) adj_win[lv].GetMemoryAllocator();
			VidToAdjacencyListMappingTable::VidToAdjTable.InitializeBlockMemory(ptr, lv);
		}
	}

	void InitUserProgram(UserProgram_t* user_program);
    
    virtual double GetLocalScatterExecutionTime(bool reset_timers) {
        double total_time = 0.0;
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
			total_time += adj_win[lv].GetLocalScatterExecutionTime(reset_timers);
		}
        return total_time;
    }
    
    virtual void ReportNWSMTime(std::vector<std::vector<json11::Json>>& nwsm_time_breaker) {
        nwsm_time_breaker[0].push_back(per_level_timer[0].get_timer(1));
        nwsm_time_breaker[1].push_back(per_level_timer[0].get_timer(3));
        nwsm_time_breaker[2].push_back(per_level_timer[0].get_timer(6));
        nwsm_time_breaker[3].push_back(per_level_timer[0].get_timer(11));
        nwsm_time_breaker[4].push_back(per_level_timer[0].get_timer(7) - per_level_timer[0].get_timer(4));
        nwsm_time_breaker[5].push_back(per_level_timer[0].get_timer(8));
        nwsm_time_breaker[6].push_back(per_level_timer[0].get_timer(9));
        nwsm_time_breaker[7].push_back(inc_start_timer.get_timer(4));
        nwsm_time_breaker[8].push_back(inc_start_timer.get_timer(5));
        nwsm_time_breaker[9].push_back(inc_start_timer.get_timer(3));
        nwsm_time_breaker[10].push_back(inc_start_timer.get_timer(6));
        
        per_level_timer[0].clear_all_timers();
        apply_timer.clear_all_timers();
        inc_start_timer.clear_all_timers();
        for (int i = 0; i < UserArguments::MAX_LEVEL; i++) {
            adj_win[i].ResetTimers();
        }
    }

  private:

	virtual void ReportTimers(bool reset_timers = true) {
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
			adj_win[lv].PrintBreakDownTimer(reset_timers);
		}
//#ifdef PRINT_PROFILING_TIMERS
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
            fprintf(stdout, "[%ld] (%ld, %ld, %d) [LV-%lld/%lld, %lld ProcessNestedStream%s]\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f, %.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n"
			        , (int64_t) PartitionStatistics::my_machine_id()
                    , UserArguments::UPDATE_VERSION
                    , UserArguments::SUPERSTEP
                    , UserArguments::INC_STEP
			        , (int64_t) lv
			        , (int64_t) UserArguments::MAX_LEVEL
			        , (int64_t) UserArguments::INC_STEP
                    , (UserArguments::ITERATOR_MODE == PARTIAL_LIST) ? "PartialList" : "FullList"
                    , per_level_timer[lv].get_timer(0)
			        , per_level_timer[lv].get_timer(1)
			        , per_level_timer[lv].get_timer(2)
			        , per_level_timer[lv].get_timer(3)
			        , per_level_timer[lv].get_timer(4)
			        , per_level_timer[lv].get_timer(5)
			        , per_level_timer[lv].get_timer(6)
			        , per_level_timer[lv].get_timer(11)
			        , per_level_timer[lv].get_timer(7)
			        , per_level_timer[lv].get_timer(8)
			        , per_level_timer[lv].get_timer(9)
			        , per_level_timer[lv].get_timer(10)
			        , apply_timer.get_timer(0)
			        , apply_timer.get_timer(1)
			        , apply_timer.get_timer(2)
			        , apply_timer.get_timer(3)
			        , apply_timer.get_timer(4)
			        , apply_timer.get_timer(5)
			        , apply_timer.get_timer(6)
			        , apply_timer.get_timer(7)
			        , apply_timer.get_timer(8)
			        , apply_timer.get_timer(9)
			        , apply_timer.get_timer(10)
			        , apply_timer.get_timer(11)
			        , apply_timer.get_timer(12)
			        , apply_timer.get_timer(13)
			        , apply_timer.get_timer(14)
			        , apply_timer.get_timer(15)
			        , apply_timer.get_timer(16)
			        , apply_timer.get_timer(17));
            /*if (reset_timers) {
                per_level_timer[lv].clear_all_timers();
                apply_timer.clear_all_timers();
            }*/
        }
//#endif
	}

    ReturnStatus SynchronizeCutVertices() {
		user_program_->synchronize_vectors();
		return OK;
	}

  private:
    bool skip_apply_phase;
    bool AdvanceIv;
	
    virtual void ResetTimers() {
		for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
            per_level_timer[lv].clear_all_timers();
            apply_timer.clear_all_timers();
        }
        for (int i = 0; i < UserArguments::MAX_LEVEL; i++) {
            adj_win[i].ResetTimers();
        }
    }

};


template <typename UserProgram_t>
void TG_NWSM<UserProgram_t>::InitUserProgram(UserProgram_t* user_program) {
    user_program_ = user_program;
    user_program_->InitNWSMBase(this);
    if (adj_win != NULL) { 
        delete[] adj_win;
    }
    adj_win = new TG_AdjWindow<UserProgram_t>[UserArguments::MAX_LEVEL];
    for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
        adj_win[lv].Initialize(user_program, lv);
    }
    PartitionStatistics::wait_for_all();
}


template <typename UserProgram_t>
void TG_NWSM<UserProgram_t>::ProcessNestedWindowedStreamPartialList (int lv, int machine_id, Range<int> update_version_range, EdgeType e_type, DynamicDBType d_type) {
    ALWAYS_ASSERT (UserArguments::ITERATOR_MODE == PARTIAL_LIST);

    per_level_timer[lv-1].start_timer(0);

    std::vector<Range<int64_t>> sched_input_vector_chunks;
    TG_Vector<Range<int64_t>> sched_output_vector_chunks;
    std::vector<bool> sched_input_vector_chunks_active_flag;
    TG_Vector<bool> sched_output_vector_chunks_active_flag;

    Range<int64_t> cur_input_vector_chunks;
    Range<int64_t> next_input_vector_chunks;
    Range<int64_t> cur_output_vector_chunks;
    Range<int64_t> next_output_vector_chunks;
    std::queue<std::future<void>> reqs_to_wait;

    Range<node_t> vid_range;
    UserArguments::CURRENT_LEVEL = lv - 1;
    bool NeedUnpin = (lv == UserArguments::MAX_LEVEL);
    PageID MaxNumPagesToPin = (NeedUnpin == false) ? TurboDB::GetBufMgr()->GetNumberOfAvailableFrames() - (UserArguments::NUM_THREADS * 16) : std::numeric_limits<PageID>::max();
    ScheduleVectorWindow(machine_id, sched_input_vector_chunks, sched_output_vector_chunks, sched_input_vector_chunks_active_flag, sched_output_vector_chunks_active_flag);


    per_level_timer[lv-1].start_timer(2);
    for (int i = 0; i < sched_input_vector_chunks.size() - 1; i++) {
        cur_input_vector_chunks = sched_input_vector_chunks[i];
        next_input_vector_chunks = sched_input_vector_chunks[i + 1];

        PartitionID start_src_vector_chunk_id = sched_input_vector_chunks[0].GetBegin();
        PartitionID cur_src_vector_chunk_id = cur_input_vector_chunks.GetBegin();
        PartitionID next_src_vector_chunk_id = next_input_vector_chunks.GetBegin();

        vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, cur_src_vector_chunk_id % UserArguments::VECTOR_PARTITIONS);

        per_level_timer[lv-1].start_timer(3);
        ReadInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
        per_level_timer[lv-1].stop_timer(3);

        per_level_timer[lv-1].start_timer(5);
        for (int j = 0; j < sched_output_vector_chunks.size() - 1; j++) {
            cur_output_vector_chunks = sched_output_vector_chunks[j];
            int target_machine_id = cur_output_vector_chunks.GetBegin() / UserArguments::VECTOR_PARTITIONS;
            Range<node_t> target_vid_range = PartitionStatistics::per_machine_vid_range(target_machine_id);
            next_output_vector_chunks = sched_output_vector_chunks[j + 1];

            per_level_timer[lv-1].start_timer(6);
            InitializeOutputVectors (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
            per_level_timer[lv-1].stop_timer(6);
            per_level_timer[lv-1].start_timer(11);
            ReadVectorsFromRemote (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
            per_level_timer[lv-1].stop_timer(11);

            user_program_->set_context(cur_input_vector_chunks, cur_output_vector_chunks);

            PartitionID start_dst_vector_chunk_id = sched_output_vector_chunks[0].GetBegin();
            PartitionID cur_dst_vector_chunk_id = cur_output_vector_chunks.GetBegin();
            PartitionID next_dst_vector_chunk_id = next_output_vector_chunks.GetBegin();

            per_level_timer[lv-1].start_timer(7);
            if (lv == 1 && UserArguments::MAX_LEVEL == 1) {
                per_level_timer[lv-1].start_timer(4);
                if (UserArguments::GRAPH_IN_MEMORY) {
                    while (adj_win[UserArguments::CURRENT_LEVEL].RunLocalScatterGatherInMemory(sched_output_vector_chunks[0], cur_input_vector_chunks, cur_output_vector_chunks, next_input_vector_chunks, next_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type) == ON_GOING) {
                        if (lv != UserArguments::MAX_LEVEL) {
                            ProcessNestedWindowedStreamPartialList(lv + 1, target_machine_id, update_version_range, e_type, d_type);
                        }
                        UserArguments::CURRENT_LEVEL = lv - 1;
                    }
                } else {
                    // Optimization - LocalScatterGather (Partial List Iterating Mode) with MAX_LEVEL == 1
                    if (j == 0 && UserArguments::MAX_LEVEL == 1) {
                        // TODO - can we overlap the below operations (IdentifyAnd..., RunPrefetching...)
                        per_level_timer[lv-1].start_timer(1);
                        Range<int64_t> total_output_vector_chunks(0, PartitionStatistics::num_target_vector_chunks() - 1);
                        adj_win[UserArguments::CURRENT_LEVEL].IdentifyAndCountInputVerticesAndPages(cur_input_vector_chunks, total_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type);
                        per_level_timer[lv-1].stop_timer(1);
                        reqs_to_wait.push(Aio_Helper::EnqueueAsynchronousTask(TG_AdjWindow<UserProgram_t>::RunPrefetchingDiskIoForLocalScatterGather, &adj_win[UserArguments::CURRENT_LEVEL], cur_input_vector_chunks, &sched_output_vector_chunks, &sched_output_vector_chunks_active_flag, Range<int64_t>(0, PartitionStatistics::num_subchunks_per_edge_chunk() - 1), update_version_range, e_type, d_type));
                    }
                    per_level_timer[lv-1].stop_timer(4);

                    while (adj_win[UserArguments::CURRENT_LEVEL].RunLocalScatterGather(sched_output_vector_chunks[0], cur_input_vector_chunks, cur_output_vector_chunks, next_input_vector_chunks, next_output_vector_chunks, vid_range, MaxNumPagesToPin, !NeedUnpin, update_version_range, e_type, d_type) == ON_GOING) {
                        if (lv != UserArguments::MAX_LEVEL) {
                            ProcessNestedWindowedStreamPartialList(lv + 1, target_machine_id, update_version_range, e_type, d_type);
                        }
                        UserArguments::CURRENT_LEVEL = lv - 1;
                    }
                }
            } else {
            }
            UserArguments::CURRENT_LEVEL = lv - 1;
            per_level_timer[lv-1].stop_timer(7);

            per_level_timer[lv-1].start_timer(8);
            FlushOutputVectorLocalGatherBuffer (sched_output_vector_chunks[0], cur_output_vector_chunks, next_output_vector_chunks);
            ALWAYS_ASSERT (cur_output_vector_chunks.GetBegin() == cur_output_vector_chunks.GetEnd());
            per_level_timer[lv-1].stop_timer(8);
        }
        while (reqs_to_wait.size() > 0) {
            reqs_to_wait.front().get();
            reqs_to_wait.pop();
        }
        FlushInputVectors (sched_input_vector_chunks[0], cur_input_vector_chunks, next_input_vector_chunks);
        per_level_timer[lv-1].stop_timer(5);
    }
    per_level_timer[lv-1].stop_timer(2);

    per_level_timer[lv-1].start_timer(10);
    adj_win[UserArguments::CURRENT_LEVEL].EmptyProcessedPageQueue(e_type, d_type);
    per_level_timer[lv-1].stop_timer(10);

    per_level_timer[lv-1].start_timer(9);
    if (lv == 1) ProcessApply();// BARRIER
    per_level_timer[lv-1].stop_timer(9);

    per_level_timer[lv-1].stop_timer(0);
    
    ReportTimers(false);
    return;
}


template <typename UserProgram_t>
void TG_NWSM<UserProgram_t>::Close() {
    if (adj_win != NULL) {
        for (int i = 0; i < UserArguments::MAX_LEVEL; i++) {
            adj_win[i].Close();
        }
        delete[] adj_win;
        adj_win = NULL;
    }
    if (window_memory_ptr != NULL) {
        NumaHelper::free_numa_memory(window_memory_ptr, window_memory_capacity);
        window_memory_ptr = NULL;
        window_memory_capacity = -1;
    }
}

std::atomic<int64_t> TG_NWSM_Base::read_ggb_flag;

#endif
