#ifndef MATERIALIZED_ADJACENCY_LISTS_H
#define MATERIALIZED_ADJACENCY_LISTS_H


#include "analytics/core/RequestRespond.hpp"
#include "analytics/core/TypeDef.hpp"
#include "analytics/core/Global.hpp"
#include "analytics/datastructure/page.hpp"
#include "analytics/datastructure/disk_aio_factory.hpp"
#include "analytics/util/atom.hpp"
#include "analytics/util/timer.hpp"
#include "analytics/util/kxsort.hpp"
#include "analytics/util/BlockMemoryStackAllocator.hpp"
#include "analytics/util/MemoryAllocator.hpp"
#include "analytics/util/TG_NWSM_Utility.hpp"

struct AdjacencyListMemoryPointer {
	MaterializedAdjacencyLists* ptr;
	int32_t slot_id;
};

class VidToAdjacencyListMappingTablePerWindow {

  public:
	friend class MaterializedAdjacencyLists;

	VidToAdjacencyListMappingTablePerWindow() : reference_counter_(0), valid_(-1), vid_to_adj_(nullptr) {
		Invalidate();
		vid_range_.Set(-1, -1);
		mat_adj_container_.clear();
		block_memory_stack_allocator_ = NULL;
		SetCurrentBlockSizeInByte(0);
        is_window_finalized_.store(false);
    }

	~VidToAdjacencyListMappingTablePerWindow() {
		DeleteAll();
		if (block_memory_stack_allocator_ != NULL) {
			delete block_memory_stack_allocator_;
			block_memory_stack_allocator_ = NULL;
		}
	}

    void SetWindowFinalized(bool f) {
        is_window_finalized_.store(f);
    }

    bool IsWindowFinalized() {
        return is_window_finalized_.load();
    }

	void InitializeVidToAdjTable (Range<node_t> vid_range) {
		INVARIANT (!IsValid());
		INVARIANT (!BeingReferenced());

		ContainerLock();
		ALWAYS_ASSERT (mat_adj_container_.size() == 0);
		ContainerUnlock();

		INVARIANT (vid_to_adj_ == NULL);
		INVARIANT (GetBlockMemoryStackAllocator().GetStackPointer() == 0);
        AdjacencyListMemoryPointer* vid_to_adj = (AdjacencyListMemoryPointer*) GetBlockMemoryStackAllocator().Allocate(sizeof(AdjacencyListMemoryPointer) * vid_range.length());
#pragma omp parallel for
        for (int64_t i = 0; i < vid_range.length(); i++) {
			vid_to_adj[i].ptr = nullptr;
			vid_to_adj[i].slot_id = -1;
		}
		vid_to_adj_ = vid_to_adj;
		vid_range_ = vid_range;
	}

	void InitializeBlockMemory(BlockMemoryStackAllocator* ptr) {
		ALWAYS_ASSERT (block_memory_stack_allocator_ == NULL);
		block_memory_stack_allocator_ = ptr;
	}

    void Clear() {
        ALWAYS_ASSERT(block_memory_stack_allocator_ != NULL);
		Invalidate();
        while (BeingReferenced()) {
            _mm_pause();
        }
        vid_range_.Set(-1, -1);
		ContainerLock();
        mat_adj_container_.clear();
		ContainerUnlock();
        block_memory_stack_allocator_->FreeAll();
        SetCurrentBlockSizeInByte(0);
        vid_to_adj_ = NULL;
    }

    inline AdjacencyListMemoryPointer& Get(node_t vid) {
        ALWAYS_ASSERT (vid_range_.contains(vid));
        ALWAYS_ASSERT (vid_to_adj_[vid - vid_range_.GetBegin()].ptr != nullptr);
        ALWAYS_ASSERT (vid_to_adj_[vid - vid_range_.GetBegin()].slot_id >= 0);
        return vid_to_adj_[vid - vid_range_.GetBegin()];
    }

	void Put(std::vector<void*>& containers);
	void Put(void* container);
	bool Has(node_t vid);
	
    void PutWithCopy(void* container, Range<node_t> mat_adj_vid_range);

    bool TryAcquireReference() {
		reference_counter_++;
        if (!IsValid()) {
		    reference_counter_--;
            return false;
        } else {
            return true;
        }
    }

	inline void AcquireReference(bool wait = false) {
		reference_counter_++;

        if (wait) {
            int backoff = 1;
            while (!IsValid()) {
		        reference_counter_--;
                usleep(backoff * 1024);
                if (backoff != 4096) backoff = 2 * backoff;
		        reference_counter_++;
            }
        }
        ALWAYS_ASSERT(BeingReferenced());
		//fprintf(stdout, "AcquireReference %ld\n", reference_counter_.load());
	}
	inline void ReleaseReference() {
		reference_counter_--;
		//fprintf(stdout, "ReleaseReference %ld\n", reference_counter_.load());
	}

	inline bool BeingReferenced() {
		return reference_counter_.load() > 0;
	}

	inline void Validate() {
		valid_.store(1);
	}

	inline void Invalidate(bool wait = false) {
		valid_.store(-1);
        if (wait) {
            while (BeingReferenced()) {
                usleep(64 * 1024);
            }
        }
        ALWAYS_ASSERT(!IsValid());
	}

	inline bool IsValid() {
		return valid_.load() > 0;
	}
	void InitializeContainer(Range<node_t> vid_range);
	void DeleteAll();

	void DeleteVidToAdjTable();

	void SanityCheck(bool cur = true, bool ins = true, bool del = true);

	void Lock() {
		//	  lock_.lock();
	}
	void Unlock() {
		//	  lock_.unlock();
	}
	void ContainerLock() {
		container_lock_.lock();
	}
	void ContainerUnlock() {
		container_lock_.unlock();
	}
	
    inline void AddCurrentBlockSizeInByte(int64_t bytes) {
        //system_fprintf(0, stdout, "[%ld][AddCurrentBlockSizeInByte] (%ld) was %ld is %ld\n", UserArguments::tmp, (int64_t) pthread_self(), block_size_in_byte_, block_size_in_byte_ + bytes);
		//block_size_in_byte_ += bytes;
		//block_size_in_byte_ += bytes;
        std::atomic_fetch_add((std::atomic<int64_t>*) &block_size_in_byte_, bytes);
    }

	inline void SetCurrentBlockSizeInByte(int64_t bytes) {
        //fprintf(stdout, "[%ld][SetCurrentBlockSizeInByte] (%ld) was %ld is %ld\n", UserArguments::tmp, (int64_t) pthread_self(), block_size_in_byte_, bytes);
		block_size_in_byte_ = bytes;
	}
	inline void SetMaterializedVidRange(Range<node_t> vid_range) {
		vid_range_ = vid_range;
	}
	void SetAdjacencyListMemoryPointer(AdjacencyListMemoryPointer* ptr) {
		vid_to_adj_ = ptr;
	}

	inline bool DoesBlockExist() {
        return !(vid_range_.GetBegin() == -1 && vid_range_.GetEnd() == -1);
    }

	inline Range<node_t> GetMaterializedVidRange() {
		return vid_range_;
	}
	inline int64_t GetCurrentBlockSizeInByte() {
		return block_size_in_byte_;
	}
	BlockMemoryStackAllocator& GetBlockMemoryStackAllocator() {
		return *block_memory_stack_allocator_;
	}
	std::vector<void*>& GetMatAdjContainer() {
		return mat_adj_container_;
	}
	AdjacencyListMemoryPointer* GetAdjacencyListMemoryPointer() {
		return vid_to_adj_;
	}


  private:
	int64_t block_size_in_byte_;
	BlockMemoryStackAllocator* block_memory_stack_allocator_;
	std::vector<void*> mat_adj_container_;

	AdjacencyListMemoryPointer* vid_to_adj_;
	Range<node_t> vid_range_;

	atom container_lock_;
	atom lock_;

    std::atomic<bool> is_window_finalized_;
	std::atomic<int32_t> valid_;
	std::atomic<int32_t> reference_counter_;
};

class VidToAdjacencyListMappingTable {
  public:
	friend class MaterializedAdjacencyLists;

	static VidToAdjacencyListMappingTable VidToAdjTable;

	VidToAdjacencyListMappingTable() {
		vid_to_adj_tbls = NULL;
	}
	~VidToAdjacencyListMappingTable() {
		if (vid_to_adj_tbls != NULL) {
			delete[] vid_to_adj_tbls;
			vid_to_adj_tbls = NULL;
		}
	}
	static int64_t ComputeVidToAdjTableSize (Range<node_t> vid_range) {
		return (sizeof(AdjacencyListMemoryPointer) * vid_range.length());
	}

	void Initialize(int max_lv) {
		vid_to_adj_tbls = new VidToAdjacencyListMappingTablePerWindow[max_lv];
	}

    void Clear(int lv) {
        ALWAYS_ASSERT(vid_to_adj_tbls != NULL);
        vid_to_adj_tbls[lv].Clear();
    }

	void InitializeVidToAdjTable (Range<node_t> vid_range, int lv) {
		vid_to_adj_tbls[lv].InitializeVidToAdjTable(vid_range);
	}
	void InitializeBlockMemory(BlockMemoryStackAllocator* ptr, int lv) {
		vid_to_adj_tbls[lv].InitializeBlockMemory(ptr);
	}
	void InitializeContainer(Range<node_t> vid_range, int lv) {
		vid_to_adj_tbls[lv].InitializeContainer(vid_range);
	}

	template <int LV> AdjacencyListMemoryPointer& Get(node_t vid) {
		return vid_to_adj_tbls[LV].Get(vid);
	}
	
    inline AdjacencyListMemoryPointer& Get(node_t vid, int lv) {
		return vid_to_adj_tbls[lv].Get(vid);
	}
    
    void SetWindowFinalized(bool f, int lv) {
		vid_to_adj_tbls[lv].SetWindowFinalized(f);
    }
    
    bool IsWindowFinalized(int lv) {
		return vid_to_adj_tbls[lv].IsWindowFinalized();
    }

    void PutWithCopy(void* container, Range<node_t> mat_adj_vid_range, int lv) {
		vid_to_adj_tbls[lv].PutWithCopy(container, mat_adj_vid_range);
    }

	void Put(void* container, int lv) {
		vid_to_adj_tbls[lv].Put(container);
	}

    void Put(std::vector<void*>& containers, int lv) {
		vid_to_adj_tbls[lv].Put(containers);
	}
	
    bool Has(node_t vid, int lv) {
		return vid_to_adj_tbls[lv].Has(vid);
	}
	template <int LV> 
	void Get(node_t vid, node_t*& data_ptr, int64_t& data_sz);
/*
	template <int LV> void Get(node_t vid, node_t*& data_ptr, int64_t& data_sz) {
		AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get<LV>(vid);
		MaterializedAdjacencyLists* adj_ptr = ptr.ptr;
		data_ptr = adj_ptr->GetBeginPtr(ptr.slot_id);
		data_sz = adj_ptr->GetNumEntries(ptr.slot_id);
		return;
	}
*/

	void Get(int lv, node_t vid, node_t*& data_ptr, int64_t& data_sz);

	template <int LV> void Put(void* container) {
		vid_to_adj_tbls[LV].Put(container);
	}
	template <int LV> bool Has(node_t vid) {
		return vid_to_adj_tbls[LV].Has(vid);
	}

	void DeleteAll(int lv) {
		vid_to_adj_tbls[lv].DeleteAll();
	}

	inline void AcquireReference(int lv, bool wait = false) {
		vid_to_adj_tbls[lv].AcquireReference(wait);
	}
	inline void ReleaseReference(int lv) {
		vid_to_adj_tbls[lv].ReleaseReference();
	}
	inline bool BeingReferenced(int lv) {
		return vid_to_adj_tbls[lv].BeingReferenced();
	}
	inline void Validate(int lv) {
		vid_to_adj_tbls[lv].Validate();
	}
	inline void Invalidate(int lv, bool wait = false) {
		vid_to_adj_tbls[lv].Invalidate(wait);
	}
	inline bool IsValid(int lv) {
		return vid_to_adj_tbls[lv].IsValid();
	}

	void Lock(int lv) {
		vid_to_adj_tbls[lv].Lock();
	}
	void Unlock(int lv) {
		vid_to_adj_tbls[lv].Unlock();
	}
	void ContainerLock(int lv) {
		vid_to_adj_tbls[lv].ContainerLock();
	}
	void ContainerUnlock(int lv) {
		vid_to_adj_tbls[lv].ContainerUnlock();
	}

    inline void AddCurrentBlockSizeInByte(int64_t bytes, int lv) {
		vid_to_adj_tbls[lv].AddCurrentBlockSizeInByte(bytes);
    }

	inline void SetCurrentBlockSizeInByte(int64_t bytes, int lv) {
		vid_to_adj_tbls[lv].SetCurrentBlockSizeInByte(bytes);
	}
	inline void SetMaterializedVidRange(Range<node_t> vid_range, int lv) {
		vid_to_adj_tbls[lv].SetMaterializedVidRange(vid_range);
	}
	inline void SetAdjacencyListMemoryPointer(AdjacencyListMemoryPointer* ptr, int lv) {
		vid_to_adj_tbls[lv].SetAdjacencyListMemoryPointer(ptr);
	}
	inline bool DoesBlockExist(int lv) {
		return vid_to_adj_tbls[lv].DoesBlockExist();
    }

	inline Range<node_t> GetMaterializedVidRange(int lv) {
		return vid_to_adj_tbls[lv].GetMaterializedVidRange();
	}
	inline int64_t	   GetCurrentBlockSizeInByte(int lv) {
		return vid_to_adj_tbls[lv].GetCurrentBlockSizeInByte();
	}
	BlockMemoryStackAllocator& GetBlockMemoryStackAllocator(int lv) {
		return vid_to_adj_tbls[lv].GetBlockMemoryStackAllocator();
	}
	std::vector<void*>& GetMatAdjContainer(int lv) {
		return vid_to_adj_tbls[lv].GetMatAdjContainer();
	}
	AdjacencyListMemoryPointer* GetAdjacencyListMemoryPointer(int lv) {
		return vid_to_adj_tbls[lv].GetAdjacencyListMemoryPointer();
	}

  private:
	VidToAdjacencyListMappingTablePerWindow* vid_to_adj_tbls;
};



class MaterializedAdjacencyLists {
  public:
	friend class VidToAdjacencyListMappingTable;

	MaterializedAdjacencyLists() {}

	void Initialize(int64_t bytes, int64_t capacity, char* data) {
		vpage_.Initialize(bytes, capacity, data);
	}
	void Initialize(SimpleContainer cont, int64_t bytes) {
		vpage_.Initialize(bytes, cont.capacity, cont.data);
		current_slot_idx_ = -1;
		deleted_ = false;
	}
	void Initialize(VariableSizedPage<Slot32>& vp) {
		vpage_ = vp;
		current_slot_idx_ = -1;
		deleted_ = false;
	}

	void MarkDeleted () {
		ALWAYS_ASSERT (deleted_ == false);
		deleted_ = true;
	}

	void DeleteContainer() {
		ALWAYS_ASSERT (deleted_);
		ALWAYS_ASSERT (this->Data() != nullptr);
		void* buf_ptr = (void*) (this->Data());
		size_t buf_size = (size_t) (this->Size());
		TurboFree (buf_ptr, buf_size);
	}

    void GetAdjList(int32_t slot_id, node_t* data_ptr[], int64_t data_sz[]) {
        data_ptr[0] = GetBeginPtr(slot_id);
        data_ptr[1] = GetBeginPtrOfDeltaInsert(slot_id);
        data_ptr[2] = GetBeginPtrOfDeltaDelete(slot_id);
        data_sz[0]  = GetNumEntriesOfCurrent(slot_id);
        data_sz[1]  = GetNumEntriesOfDeltaInsert(slot_id);
        data_sz[2]  = GetNumEntriesOfDeltaDelete(slot_id);
    }

	static VidToAdjacencyListMappingTable& VidToAdjTable;

	static PageID TotalNumAvailableFrames;
	static PageID TotalNumUsedFrames;
	static atom BufferLock;

	static PageID TotalNumFrames() {
		return TotalNumAvailableFrames;
	}
	static PageID TotalNumFreeFrames() {
		return TotalNumAvailableFrames - std::atomic_load<PageID>((std::atomic<PageID>*) &TotalNumUsedFrames);
	}
	static void IncrementNumUsedFrames(PageID cnt) {
		std::atomic_fetch_add<PageID>((std::atomic<PageID>*) &TotalNumUsedFrames, cnt);
	}
	static void DecrementNumUsedFrames(PageID cnt) {
		std::atomic_fetch_add<PageID>((std::atomic<PageID>*) &TotalNumUsedFrames, -cnt);
	}
	
    static void LockBufferIfEnoughFreeFrames(PageID cnt, bool keep_trying) {
        MaterializedAdjacencyLists::LockBuffer();
        int backoff = 1024;
        while (MaterializedAdjacencyLists::TotalNumFreeFrames() < cnt) {
            MaterializedAdjacencyLists::UnlockBuffer();
            if (!keep_trying) break;
            if (backoff < 1024) backoff = 2 * backoff;
            usleep(backoff * 16);
            MaterializedAdjacencyLists::LockBuffer();
        }
    }

	static void LockBuffer() {
		BufferLock.lock();
	}
	static void UnlockBuffer() {
		BufferLock.unlock();
	}

	static void DropMaterializedViewInMemory (int lv);
	static void SlideBlockMemoryWindow (Range<node_t> bitmap_vid_range, BitMap<node_t>* requested_vertices, Range<node_t> vid_range, int lv, int64_t BytesOfNextBlock);
	static void EvictAsNeededAndPrepareForMergeDelta(BitMap<node_t>* fulllist_latest_flags, Range<node_t> bitmap_vid_range, BitMap<node_t>* requested_vertices, Range<node_t> vid_range, int lv, int64_t BytesOfNextBlock, std::vector<bool>* to_keep, std::function<void(void*)> cb);
	static void MergeDeltaWithCachedCurrent(BitMap<node_t>* fulllist_latest_flags, Range<node_t> bitmap_vid_range, BitMap<node_t>* requested_vertices, Range<node_t> vid_range, int lv, Range<int64_t> dst_edge_partitions, std::vector<bool>* to_keep, std::function<void(void*)> cb);

	// vids: requested vertices.
	// vid_range: range of requested vertices.
	// pids: requested pages.
	// page_cnts: # of requested pages.
	// PidRangesPerEdgePartition: range of page ids to request per edge partition.
	static void MaterializeAndSend(BitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, BitMap<PageID>* pids, int64_t to, PageID page_cnts, Range<PageID>* PidRangesPerEdgePartition, bool UsePermanentMemory);

	static void MaterializeAndEnqueue(tbb::concurrent_queue<UdfSendRequest>* req_buffer_queue, Range<node_t> bitmap_vid_range, TwoLevelBitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, int64_t to, int64_t lv, int64_t* stop_flag, bool is_full_list, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>> CurPid);
    static void CopyMaterializedAdjListsFromCacheAndEnqueue(tbb::concurrent_queue<UdfSendRequest>* req_buffer_queue, BitMap<node_t>* vids, Range<node_t> vid_range, std::vector<node_t> vertices, int64_t size_to_request, int64_t adjlist_data_size, int64_t slot_data_size, Range<int64_t> dst_edge_partitions, int64_t to, int64_t lv, int64_t* stop_flag, bool is_full_list, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);

	static void SendMaterializedAdjListWithUDF(MaterializedAdjacencyLists mat_adj, int64_t to);

	static void SendDataWithUdfRequest(UserCallbackRequest udf_req, char* data_buffer, int64_t data_size, SimpleContainer cont_to_return) {
		RequestRespond::SendRequestReturnContainer(udf_req, data_buffer, data_size, cont_to_return);
	}

	void SortByDegreeOrder() {
		INVARIANT (UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
		ALWAYS_ASSERT (Data() != NULL);
		ALWAYS_ASSERT (GetNumAdjacencyLists() > 0);
		ALWAYS_ASSERT (GetNumEdges() > 0);
		ALWAYS_ASSERT (!IsEmpty());
		node_t* begin = (node_t*) Data();
		int64_t sz = Size() / sizeof(node_t);
		int64_t prev_idx = 0;
		//fprintf(stdout, "[%ld] SortByDegree; num_adjlists:%ld\n", PartitionStatistics::my_machine_id(), GetNumAdjacencyLists());
		for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
			node_t src_vid = GetSlot(adj_idx)->src_vid;
			node_t src_degree_order = PartitionStatistics::VidToDegreeOrder(src_vid);
			GetSlot(adj_idx)->src_vid = src_degree_order;
			int64_t from = prev_idx;
			int64_t to = GetSlot(adj_idx)->end_offset;
			prev_idx = to;
			ALWAYS_ASSERT (from >= 0 && from < sz);
			ALWAYS_ASSERT (to >= 0 && to < sz);
			ALWAYS_ASSERT (from <= to);
			//fprintf(stdout, "[%ld] SortByDegree; %ld/%ld\tsrc_vid=%ld\n", PartitionStatistics::my_machine_id(), adj_idx, GetNumAdjacencyLists(), src_vid);
			std::sort (&begin[from], &begin[to], std::less<node_t>());
		}
	}

	void MaterializePartialList(
	    BitMap<node_t>* vids,
	    Range<node_t> vid_range,
	    Range<int64_t> dst_edge_partitions,
	    bool UsePermanentMemory,
	    int lv,
        Range<int> version_range,
        EdgeType e_type,
        DynamicDBType d_type) {
		
		// Return
	}
	
    //XXX tslee - temp for implementing merge
    ReturnStatus MaterializeAndMergePartialList(BitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);
        
    void MaterializeFullList(TwoLevelBitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, bool use_fulllist_db);

    ReturnStatus MaterializeFullListAsCurrentAndDelta(TwoLevelBitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, bool use_fulllist_db=false);
    ReturnStatus MaterializeFullListAsCurrentAndDeltaForFullListDB(BitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid);
    
    void ComputeSizeByDegreeArraysAndFilterByDegree(VECTOR<node_t>& total_dummy_src_vertices_cnts, VECTOR<node_t>& total_src_vertices_cnt, VECTOR<node_t>& total_edges_cnt, BitMap<node_t>* vids, Range<node_t>& vid_range, Range<int>& version_range, node_t first_vid_of_bitmap, FixedSizeVector<FixedSizeVector<int32_t>>& degrees, FixedSizeVector<FixedSizeVector<int32_t>>& delta_degrees, int version_begin, int version_end, Range<int64_t> dst_edge_partitions, DynamicDBType db_type);

    void FormatContainer(std::vector<node_t>& total_dummy_src_vertices_cnts, std::vector<node_t>& total_src_vertices_cnt, std::vector<node_t>& total_edges_cnt, VariableSizedPage<Slot32>* vpage, BitMap<node_t>* vids, Range<node_t> vid_range, int dbtype, std::vector<std::vector<int64_t>>& degrees, std::vector<std::vector<int64_t>>& offset_to_fill_from, node_t first_vid_of_bitmap, int64_t size_to_request, int64_t adjlist_data_size);
    void FormatContainer(VECTOR<node_t>& total_dummy_src_vertices_cnts, VECTOR<node_t>& total_src_vertices_cnt, VECTOR<node_t>& total_edges_cnt, VariableSizedPage<Slot32>* vpage, BitMap<node_t>* vids, Range<node_t> vid_range, FixedSizeVector<FixedSizeVector<int32_t>>& degrees, FixedSizeVector<FixedSizeVector<int32_t>>& delta_degrees, FixedSizeVector<FixedSizeVector<int32_t>>& offset_to_fill_from, FixedSizeVector<FixedSizeVector<int32_t>>& delta_offset_to_fill_from, node_t first_vid_of_bitmap, int64_t size_to_request, int64_t adjlist_data_size);
	
    void FilterByDegreeAndComputeSizeUsingDegreeArray(node_t& total_dummy_src_vertices_cnts, node_t& total_src_vertices_cnt, node_t& total_edges_cnt, BitMap<node_t>* vids, Range<node_t>& vid_range, Range<int>& version_range, node_t first_vid_of_bitmap);

    void FindPageIdsToReadFrom(BitMap<node_t>* vids, Range<node_t>& vid_range, Range<int>& version_range, FixedSizeVector<FixedSizeVector<PageID>>& CurPid, Range<int64_t> dst_edge_partitions, EdgeType db_e_type, EdgeType e_type, DynamicDBType dbtype);
    
    void CopyAdjListsInPagesToContainer(VECTOR<std::queue<PageID>>& pages_read, VECTOR<std::queue<PageID>>& delta_pages_read, PageID& unpin_counts, EdgeType& e_type, int& dbtype, BitMap<node_t>* vids, Range<node_t> vid_range, node_t first_vid_of_bitmap, FixedSizeVector<FixedSizeVector<int32_t>>& offset_to_fill_from, FixedSizeVector<FixedSizeVector<int32_t>>& delta_offset_to_fill_from, char*	tmp_container, int64_t& adjlist_data_size, FixedSizeVector<FixedSizeVector<int32_t>>& degrees);
    void CopyAdjListsInPagesToContainer(std::queue<PageID>& pending_pages, PageID& pending_page_counts, PageID& unpin_counts, EdgeType& e_type, DynamicDBType& d_type, BitMap<node_t>* vids, const Range<node_t> vid_range, const node_t first_vid_of_bitmap, FixedSizeVector<int32_t>& offset_to_fill_from, const char* tmp_container, int64_t& adjlist_data_size);
    
    
	char* Data() {
		return vpage_.GetData();
	}
	int64_t Size() {
		return vpage_.GetSize();
	}

	ReturnStatus GetNextAdjList (TG_AdjacencyListIterator& iter) {
		current_slot_idx_++;
		if (current_slot_idx_ == vpage_.NumEntry()) return DONE;
		if (current_slot_idx_ == 0) {
			iter.Init(GetSlot(current_slot_idx_)->src_vid, (node_t*) Data(), GetSlot(current_slot_idx_)->end_offset);
		} else {
			int64_t past_offset = GetSlot(current_slot_idx_ - 1)->end_offset;
			int64_t cur_offset = GetSlot(current_slot_idx_)->end_offset;
			iter.Init(GetSlot(current_slot_idx_)->src_vid, (node_t*) Data(), cur_offset - past_offset);
		}
		return OK;
	}

	node_t& GetNumAdjacencyLists() {
		return vpage_.NumEntry();
	}

	int64_t GetNumEdges() {
		int64_t NumSrcVertices = GetNumAdjacencyLists();
		int64_t NumEdges = (Size() - NumSrcVertices * sizeof(Slot32) - sizeof(node_t)) / sizeof(node_t);
		return NumEdges;
	}

	inline node_t GetSlotDegreeOrder(int64_t slot_index) {
		if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			node_t degree_order = GetSlot(slot_index)->src_vid;
			return degree_order;
		} else {
			node_t vid = GetSlot(slot_index)->src_vid;
			return PartitionStatistics::VidToDegreeOrder(vid);
		}
	}

	inline node_t GetSlotVid(int64_t slot_index) {
		if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
			node_t degree_order = GetSlot(slot_index)->src_vid;
			return PartitionStatistics::DegreeOrderToVid(degree_order);
		} else {
			node_t vid = GetSlot(slot_index)->src_vid;
			return vid;
		}
	}


	void GroupNodeListsIntoPageLists_(TwoLevelBitMap<node_t>& vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, TwoLevelBitMap<PageID>& pids) {
		Range<node_t> grouped_vid_range;
		PageID MaxNumPagesToPin = 1024 * 1024;	// XXX
		GroupNodeListsIntoPageLists(vids, vid_range, dst_edge_partitions, pids, MaxNumPagesToPin, grouped_vid_range);
		ALWAYS_ASSERT (vid_range == grouped_vid_range);
	}

	SimpleContainer GetContainer() {
		SimpleContainer cont;
		cont.data = vpage_.GetData();
		cont.size_used = vpage_.GetSize();
		cont.capacity = vpage_.GetCapacity();
		return cont;
	}

	bool IsEmpty() {
		return (vpage_.GetData() == nullptr || vpage_.GetSize() == 0 || vpage_.GetCapacity() == 0);
	}

	Range<node_t> GetSrcVidRange() {
		Range<node_t> vid_range (GetSlotVid(0), GetSlotVid(GetNumAdjacencyLists() - 1));
		return vid_range;
	}


	int32_t GetSlotIdxBySrcVid(node_t src_vid) {
		for (int i = 0; i < GetNumAdjacencyLists(); i++) {
			if (GetSlot(i)->src_vid == src_vid) {
				return i;
			}
		}
		return -1;
	}
	
    inline node_t* GetEndPtrOfDeltaInsert(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffsetOfDeltaInsert(slot_index) + GetNumEntriesOfDeltaInsert(slot_index); }
    inline node_t* GetEndPtrOfDeltaDelete(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffsetOfDeltaDelete(slot_index) + GetNumEntriesOfDeltaDelete(slot_index); }
	inline node_t* GetEndPtrOfCurrent(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffset(slot_index) + GetNumEntriesOfCurrent(slot_index); }
	inline node_t* GetEndPtr(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffset(slot_index) + GetNumEntries(slot_index); }
    inline node_t* GetBeginPtrOfDeltaInsert(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffsetOfDeltaInsert(slot_index); }
    inline node_t* GetBeginPtrOfDeltaDelete(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffsetOfDeltaDelete(slot_index); }
	inline node_t* GetBeginPtr(int64_t slot_index) { return ((node_t*) Data()) + vpage_.GetBeginOffset(slot_index); }

	inline int64_t GetNumEntriesOfDeltaInsert(int64_t slot_index) { return vpage_.GetNumEntriesOfDeltaInsert(slot_index); }
	inline int64_t GetNumEntriesOfDeltaDelete(int64_t slot_index) { return vpage_.GetNumEntriesOfDeltaDelete(slot_index); }
	inline int64_t GetNumEntriesOfCurrent(int64_t slot_index) { return vpage_.GetNumEntriesOfCurrent(slot_index); }
	inline int64_t GetNumEntries(int64_t slot_index) { return vpage_.GetNumEntries(slot_index); }

    void ConvertVidToDegreeOrderAndSortOfCachedMatAdj(BitMap<node_t>* fulllist_latest_flags, node_t offset_vid, bool current, bool delta);
    void ConvertVidToDegreeOrderAndSort();
    bool SanityCheckDegreeOrdered(int cached);
	bool SanityCheck(bool cur = true, bool ins = true, bool del = true);
    bool SanityCheckOrderdSrcVids();
	bool SanityCheckUsingVidRange(Range<node_t> vid_range);
	bool SanityCheckUsingDegree(bool deg_order);
	bool SanityCheckUniqueness();
	bool SanityCheckUsingOrderedSrcVids();

    void PrintNSlots(int64_t num_slots);
    void PrintContents(bool cur, bool ins, bool del);

    static void MarkVerticesIntoBitMap(UdfSendRequest req, BitMap<node_t>* vids, node_t offset_vid);
    static void ClearVerticesIntoBitMap(UdfSendRequest req, BitMap<node_t>* vids, node_t offset_vid);

	Slot32* GetSlot(int64_t slot_index) {
		return (Slot32*)(Data() + Size() - sizeof(node_t) - sizeof(Slot32) * (slot_index + 1));
	}
	node_t& NumEntry() {
		return vpage_.NumEntry();
	}

    static void Copy(MaterializedAdjacencyLists* l, int32_t l_slot, char* copy_form, MaterializedAdjacencyLists* r, int32_t r_slot, char* copy_to) {
        LOG_ASSERT(false);
    }

    static atom temp_lock_;

  private:

	bool deleted_;
	int64_t current_slot_idx_;
	VariableSizedPage<Slot32> vpage_;
	VariableSizedPage<Slot32> vpage_insert_;
	VariableSizedPage<Slot32> vpage_delete_;
};

#endif
