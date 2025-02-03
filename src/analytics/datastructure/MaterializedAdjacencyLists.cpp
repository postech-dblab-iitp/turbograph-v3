#include "analytics/datastructure/MaterializedAdjacencyLists.hpp"
#include "analytics/core/TurboDB.hpp"

VidToAdjacencyListMappingTable VidToAdjacencyListMappingTable::VidToAdjTable;
VidToAdjacencyListMappingTable& MaterializedAdjacencyLists::VidToAdjTable = VidToAdjacencyListMappingTable::VidToAdjTable;

PageID MaterializedAdjacencyLists::TotalNumAvailableFrames = -1;
PageID MaterializedAdjacencyLists::TotalNumUsedFrames = -1;
atom MaterializedAdjacencyLists::BufferLock;
atom MaterializedAdjacencyLists::temp_lock_;


template<int LV> 
void VidToAdjacencyListMappingTable::Get(node_t vid, node_t*& data_ptr, int64_t& data_sz) {
    AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get<LV>(vid);
    MaterializedAdjacencyLists* adj_ptr = ptr.ptr;
    data_ptr = adj_ptr->GetBeginPtr(ptr.slot_id);
    data_sz = adj_ptr->GetNumEntries(ptr.slot_id);
    return;
}

bool VidToAdjacencyListMappingTablePerWindow::Has(node_t src_vid) {
	ALWAYS_ASSERT (vid_to_adj_ != NULL);
    if (!vid_range_.contains(src_vid)) {
		ALWAYS_ASSERT(vid_to_adj_[src_vid - vid_range_.GetBegin()].slot_id == -1);
		return false;
	} else if (vid_to_adj_[src_vid - vid_range_.GetBegin()].ptr == nullptr || vid_to_adj_[src_vid - vid_range_.GetBegin()].slot_id == -1) {
		//INVARIANT (vid_to_adj_[src_vid - vid_range_.GetBegin()].slot_id == -1);
		return false;
	} else {
		return true;
	}
}

// XXX - Assume a single threaded operation
void VidToAdjacencyListMappingTablePerWindow::Put(void* void_mat_adj) {
	MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) void_mat_adj;
	ALWAYS_ASSERT (mat_adj->GetNumAdjacencyLists() != 0);
	ALWAYS_ASSERT (mat_adj->GetNumEdges() != 0);

    ContainerLock();
	mat_adj_container_.push_back(void_mat_adj);

	// Fill in 'Vid-To-AdjacencyList' Table
	node_t cur_src_vid = -1;
	node_t cur_src_vid_idx;
	node_t prev_src_vid = -1;
	for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
		cur_src_vid = mat_adj->GetSlotVid(adj_idx);
		cur_src_vid_idx = cur_src_vid - vid_range_.GetBegin();

		ALWAYS_ASSERT (vid_range_.contains(cur_src_vid));
		ALWAYS_ASSERT (cur_src_vid > prev_src_vid);
		
        vid_to_adj_[cur_src_vid_idx].ptr = mat_adj;
		vid_to_adj_[cur_src_vid_idx].slot_id = adj_idx;

		ALWAYS_ASSERT (vid_to_adj_[cur_src_vid_idx].ptr != nullptr);
		ALWAYS_ASSERT (vid_to_adj_[cur_src_vid_idx].slot_id != -1);
	}
    ContainerUnlock();
}

void VidToAdjacencyListMappingTablePerWindow::Put(std::vector<void*>& vec_void_mat_adjs) {
    ContainerLock();
    for (int64_t k = 0; k < vec_void_mat_adjs.size(); k++) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) vec_void_mat_adjs[k];
        mat_adj_container_.push_back((void*) mat_adj);
    }

#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
    for (int64_t k = 0; k < vec_void_mat_adjs.size(); k++) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) vec_void_mat_adjs[k];
        ALWAYS_ASSERT (mat_adj->GetNumAdjacencyLists() != 0);
        ALWAYS_ASSERT (mat_adj->GetNumEdges() != 0);

        // Fill in 'Vid-To-AdjacencyList' Table
        node_t cur_src_vid = -1;
        node_t cur_src_vid_idx;
        node_t prev_src_vid = -1;
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            cur_src_vid = mat_adj->GetSlotVid(adj_idx);
            cur_src_vid_idx = cur_src_vid - vid_range_.GetBegin();

            ALWAYS_ASSERT (vid_range_.contains(cur_src_vid));
            ALWAYS_ASSERT (cur_src_vid > prev_src_vid);

            vid_to_adj_[cur_src_vid_idx].ptr = mat_adj;
            vid_to_adj_[cur_src_vid_idx].slot_id = adj_idx;

            ALWAYS_ASSERT (vid_to_adj_[cur_src_vid_idx].ptr != nullptr);
            ALWAYS_ASSERT (vid_to_adj_[cur_src_vid_idx].slot_id != -1);
        }
    }
    ContainerUnlock();
}

// Used for Caching for full-list requests
void VidToAdjacencyListMappingTablePerWindow::PutWithCopy(void* void_mat_adj, Range<node_t> mat_adj_vid_range) {
    MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) void_mat_adj;
    ALWAYS_ASSERT (mat_adj->GetNumAdjacencyLists() != 0);
    ALWAYS_ASSERT (mat_adj->GetNumEdges() != 0);
   
    if (!TryAcquireReference()) return;
    ContainerLock();

    // Check what to copy
    INVARIANT(mat_adj->GetNumAdjacencyLists() * sizeof(bool) < 4 * 1024 * 1024L);
    SimpleContainer container_to_cache = RequestRespond::GetTempDataBuffer(mat_adj->GetNumAdjacencyLists() * sizeof(bool));
    FixedSizeVector<int64_t> to_cache(container_to_cache);
    to_cache.recursive_resize(container_to_cache.data, mat_adj->GetNumAdjacencyLists());
    to_cache.set(false);

    int64_t total_src_vertices_cnt = 0;
    int64_t total_edges_cnt = 0;
    node_t cur_src_vid, cur_src_vid_idx;
    int64_t remaining_bytes = VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(0).GetRemainingBytes();
    int64_t size_to_request = 0;
    for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
        cur_src_vid = mat_adj->GetSlot(adj_idx)->src_vid;   // mat_adj is not converted to deg_order yet
        cur_src_vid_idx = cur_src_vid - vid_range_.GetBegin();
        
        ALWAYS_ASSERT (cur_src_vid >= PartitionStatistics::my_first_node_id() && cur_src_vid <= PartitionStatistics::my_last_node_id());
        if (TurboDB::OutDegree(cur_src_vid) != mat_adj->GetNumEntries(adj_idx)) {
            fprintf(stdout, "[%ld] Fuck vid %ld of %ld != %ld\n", PartitionStatistics::my_machine_id(), cur_src_vid, TurboDB::OutDegree(cur_src_vid), mat_adj->GetNumEntries(adj_idx));
        }
        ALWAYS_ASSERT (TurboDB::OutDegree(cur_src_vid) == mat_adj->GetNumEntries(adj_idx));
        ALWAYS_ASSERT (vid_to_adj_[cur_src_vid_idx].slot_id != -2);

        if (!vid_range_.contains(cur_src_vid)) continue;
        if (Has(cur_src_vid)) continue;
        if (vid_to_adj_[cur_src_vid_idx].slot_id == -2) continue;       // XXX
        ALWAYS_ASSERT(vid_to_adj_[cur_src_vid_idx].ptr == nullptr);  // XXX ??
        to_cache[adj_idx] = (vid_to_adj_[cur_src_vid_idx].ptr == nullptr);  // XXX ??
        if (!to_cache[adj_idx]) continue;
        
        ALWAYS_ASSERT(vid_to_adj_[cur_src_vid_idx].slot_id == -1);
        ALWAYS_ASSERT(vid_to_adj_[cur_src_vid_idx].ptr == nullptr);
        ALWAYS_ASSERT(mat_adj->GetNumEntries(adj_idx) > 0);
        
        vid_to_adj_[cur_src_vid_idx].slot_id = -2;      // reserve the slot

        total_src_vertices_cnt++;
        total_edges_cnt += mat_adj->GetNumEntries(adj_idx);
    
        int64_t adjlist_data_size = sizeof(node_t) * total_edges_cnt;
        int64_t slot_data_size = sizeof(Slot32) * total_src_vertices_cnt;
        int64_t page_metadata_size = sizeof(node_t);
        size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;
    
        if (remaining_bytes < size_to_request) {
            total_src_vertices_cnt--;
            total_edges_cnt -= mat_adj->GetNumEntries(adj_idx);
            vid_to_adj_[cur_src_vid_idx].slot_id = -1;
            vid_to_adj_[cur_src_vid_idx].ptr = nullptr;

            adjlist_data_size = sizeof(node_t) * total_edges_cnt;
            slot_data_size = sizeof(Slot32) * total_src_vertices_cnt;
            page_metadata_size = sizeof(node_t);
            size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;

            ALWAYS_ASSERT(total_src_vertices_cnt >= 0);
            ALWAYS_ASSERT(total_edges_cnt >= 0);
            ALWAYS_ASSERT(size_to_request == 0 || total_src_vertices_cnt == 0 || remaining_bytes >= size_to_request);
            to_cache[adj_idx] = false;
            break;
        }
    }
    
    if (total_edges_cnt == 0 || VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(0).GetRemainingBytes() < size_to_request) {
        /*
        if (VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(0).GetRemainingBytes() < size_to_request) {
            fprintf(stdout, "[PutWithCopy] Failed due to a lack of memory space; need %ld, only have %ld\n", size_to_request, VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(0).GetRemainingBytes());
        }
        */
        ReleaseReference();
        ContainerUnlock();
        RequestRespond::ReturnTempDataBuffer(container_to_cache);
        return;
    }
    
    int64_t adjlist_data_size = sizeof(node_t) * total_edges_cnt;
    int64_t slot_data_size = sizeof(Slot32) * total_src_vertices_cnt;
    int64_t page_metadata_size = sizeof(node_t);
    size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;
    
    SimpleContainer cont;
    cont.data = VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(0).Allocate(size_to_request);
    cont.capacity = size_to_request;
    
    ContainerUnlock();

    MaterializedAdjacencyLists* mat_adj_to_cache = new MaterializedAdjacencyLists();
    mat_adj_to_cache->Initialize(size_to_request, cont.capacity, cont.data);

    // Fill in
    char*	tmp_container = cont.data;
    int64_t current_slot_idx = 0;
    int64_t current_data_offset = 0;
    mat_adj_to_cache->NumEntry() = total_src_vertices_cnt;
    
    for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
        if (!to_cache[adj_idx]) continue;
        
        cur_src_vid = mat_adj->GetSlot(adj_idx)->src_vid;
        cur_src_vid_idx = cur_src_vid - vid_range_.GetBegin();

        MaterializedAdjacencyLists* adj_ptr = (MaterializedAdjacencyLists*) mat_adj;
        node_t src_vid = adj_ptr->GetSlot(adj_idx)->src_vid;
        node_t* data = adj_ptr->GetBeginPtr(adj_idx);
        int64_t sz = adj_ptr->GetNumEntries(adj_idx);

        mat_adj_to_cache->GetSlot(current_slot_idx)->src_vid = cur_src_vid;
        mat_adj_to_cache->GetSlot(current_slot_idx)->end_offset = current_data_offset + adj_ptr->GetNumEntriesOfCurrent(adj_idx);
        mat_adj_to_cache->GetSlot(current_slot_idx)->delta_end_offset[0] = mat_adj_to_cache->GetSlot(current_slot_idx)->end_offset + adj_ptr->GetNumEntriesOfDeltaInsert(adj_idx);
        mat_adj_to_cache->GetSlot(current_slot_idx)->delta_end_offset[1] = mat_adj_to_cache->GetSlot(current_slot_idx)->delta_end_offset[0] + adj_ptr->GetNumEntriesOfDeltaDelete(adj_idx);
				
        ALWAYS_ASSERT (current_data_offset * sizeof(node_t) + sz * sizeof(node_t) <= adjlist_data_size);
        ALWAYS_ASSERT (mat_adj_to_cache->GetNumEntries(current_slot_idx) > 0);
        ALWAYS_ASSERT (mat_adj_to_cache->GetNumEntries(current_slot_idx) == adj_ptr->GetNumEntries(adj_idx));

        char* copy_to = tmp_container + current_data_offset * sizeof(node_t);
        char* copy_from = (char*) data;

        ALWAYS_ASSERT(copy_to >= tmp_container && copy_to < tmp_container + size_to_request);
        ALWAYS_ASSERT(copy_from >= adj_ptr->Data() && copy_from < adj_ptr->Data() + adj_ptr->Size());

        memcpy ((void*) copy_to, (void*) copy_from, sz * sizeof(node_t));
        
        ALWAYS_ASSERT(vid_range_.contains(cur_src_vid));
        ALWAYS_ASSERT (mat_adj_to_cache->GetSlot(current_slot_idx)->src_vid >= PartitionStatistics::my_first_node_id() && mat_adj_to_cache->GetSlot(current_slot_idx)->src_vid <= PartitionStatistics::my_last_node_id());

        current_slot_idx++;
        current_data_offset += sz;
    }

    ALWAYS_ASSERT(current_slot_idx == mat_adj_to_cache->GetNumAdjacencyLists());
    ALWAYS_ASSERT(current_slot_idx == total_src_vertices_cnt);
    ALWAYS_ASSERT(current_data_offset == total_edges_cnt);
    ALWAYS_ASSERT(mat_adj_to_cache->SanityCheckUsingVidRange(vid_range_));
    
    mat_adj_to_cache->ConvertVidToDegreeOrderAndSort();
    
    ContainerLock();
    mat_adj_container_.push_back((void*) mat_adj_to_cache);
    AddCurrentBlockSizeInByte(mat_adj_to_cache->Size());    // PutWithCopy
    ContainerUnlock();

    for(int64_t adj_idx = 0; adj_idx < mat_adj_to_cache->GetNumAdjacencyLists() ; adj_idx++) {
        cur_src_vid = mat_adj_to_cache->GetSlotVid(adj_idx);
        cur_src_vid_idx = cur_src_vid - vid_range_.GetBegin();
            
		ALWAYS_ASSERT(vid_range_.contains(cur_src_vid));
		ALWAYS_ASSERT(mat_adj_vid_range.contains(cur_src_vid));
        ALWAYS_ASSERT(vid_to_adj_[cur_src_vid_idx].slot_id == -2);
        ALWAYS_ASSERT(vid_to_adj_[cur_src_vid_idx].ptr == nullptr);
        
        vid_to_adj_[cur_src_vid_idx].ptr = mat_adj_to_cache;
        vid_to_adj_[cur_src_vid_idx].slot_id = adj_idx;
           
        ALWAYS_ASSERT (cur_src_vid >= PartitionStatistics::my_first_node_id() && cur_src_vid <= PartitionStatistics::my_last_node_id());
        ALWAYS_ASSERT (TurboDB::OutDegree(cur_src_vid) == mat_adj_to_cache->GetNumEntries(adj_idx));
    }

    ReleaseReference();
    RequestRespond::ReturnTempDataBuffer(container_to_cache);
}

void VidToAdjacencyListMappingTablePerWindow::InitializeContainer(Range<node_t> vid_range) {
	ALWAYS_ASSERT (mat_adj_container_.size() == 0);
	ALWAYS_ASSERT (vid_to_adj_ == nullptr);
	
    //fprintf(stdout, "[%ld] InitializeContainer([%ld, %ld]) %ld %ld %ld\n", PartitionStatistics::my_machine_id(), vid_range.GetBegin(), vid_range.GetEnd(), GetBlockMemoryStackAllocator().GetSize(), GetBlockMemoryStackAllocator().GetRemainingBytes(), sizeof(AdjacencyListMemoryPointer) * vid_range.length());
    
    vid_range_ = vid_range;
	vid_to_adj_ = (AdjacencyListMemoryPointer*) GetBlockMemoryStackAllocator().Allocate(sizeof(AdjacencyListMemoryPointer) * vid_range.length());

	#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
	for (int64_t i = 0; i < vid_range_.length(); i++) {
		vid_to_adj_[i].ptr = nullptr;
		vid_to_adj_[i].slot_id = -1;
	}
	mat_adj_container_.clear();
}

void VidToAdjacencyListMappingTablePerWindow::DeleteVidToAdjTable() {
	if (vid_to_adj_ != NULL) {
		INVARIANT (vid_range_.length() != 0);
		//GetBlockMemoryStackAllocator().Free((char*) vid_to_adj_);
		GetBlockMemoryStackAllocator().FreeBytes(sizeof(AdjacencyListMemoryPointer) * vid_range_.length());
		vid_to_adj_ = NULL;
	}
}

void VidToAdjacencyListMappingTablePerWindow::DeleteAll() {
	ALWAYS_ASSERT (!IsValid());
	ALWAYS_ASSERT (!BeingReferenced());
	int64_t total_size_bytes = 0;
	for (int i = 0; i < mat_adj_container_.size(); i++) {
		ALWAYS_ASSERT (mat_adj_container_[i] != nullptr);
		ALWAYS_ASSERT (((MaterializedAdjacencyLists*) mat_adj_container_[i])->Data() != nullptr);
		size_t buf_size = (size_t) ((MaterializedAdjacencyLists*) mat_adj_container_[i])->Size();
		total_size_bytes += buf_size;
        //fprintf(stdout, "[%ld] [DeleteAll::AddCurrentBlockSizeInByte] %ld\n", PartitionStatistics::my_machine_id(), - buf_size);
	    AddCurrentBlockSizeInByte(-buf_size);
	}
	GetBlockMemoryStackAllocator().FreeBytes(total_size_bytes);
	DeleteVidToAdjTable();
	INVARIANT (GetBlockMemoryStackAllocator().GetStackPointer() == 0);
	INVARIANT (GetCurrentBlockSizeInByte() == 0);
	mat_adj_container_.clear();
}

void MaterializedAdjacencyLists::EvictAsNeededAndPrepareForMergeDelta(BitMap<node_t>* fulllist_latest_flags, Range<node_t> bitmap_vid_range, BitMap<node_t>* requested_vertices, Range<node_t> vid_range, int lv, int64_t BytesOfNextBlock, std::vector<bool>* ptr_to_keep, std::function<void(void*)> cb) {
    ALWAYS_ASSERT(lv == 0);
    ALWAYS_ASSERT(MaterializedAdjacencyLists::VidToAdjTable.DoesBlockExist(lv));
    turbo_timer temp_timer;
    temp_timer.start_timer(6);
    
    fprintf(stdout, "[%ld] %ld EvictAsNeededAndPrepareForMergeDelta([%ld, %ld], ..., [%ld, %ld], %ld, %ld, ...)\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, bitmap_vid_range.GetBegin(), bitmap_vid_range.GetEnd(), vid_range.GetBegin(), vid_range.GetEnd(), (int64_t) lv, (int64_t) BytesOfNextBlock);

    if (!MaterializedAdjacencyLists::VidToAdjTable.DoesBlockExist(lv)) {
        LOG_ASSERT(false);
        return;
    }
    
    VidToAdjTable.Invalidate(lv, true);
    
    // XXX - dynamic memory allocation
    std::vector<bool>& to_keep = *ptr_to_keep;
    std::vector<bool> kept(to_keep.size(), false);
    std::vector<void*>& mat_adj_container = VidToAdjTable.GetMatAdjContainer(lv);
    BitMap<node_t>& vids = *requested_vertices;
    Range<node_t> vid_range_of_current_window = MaterializedAdjacencyLists::VidToAdjTable.GetMaterializedVidRange(lv);
    int64_t remaining_memory_budget = VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - sizeof(AdjacencyListMemoryPointer) * vid_range_of_current_window.length();

    ALWAYS_ASSERT(vid_range_of_current_window == bitmap_vid_range);

    /*
    // XXX - syko DEBUG
    TG_Vector<TG_Vector<node_t>> graph[3];
    graph[0].resize(PartitionStatistics::num_total_nodes());
    graph[1].resize(PartitionStatistics::num_total_nodes());
    graph[2].resize(PartitionStatistics::num_total_nodes());
    int64_t checked_cnt[2] = {0};
    for (int64_t i = 0; i < mat_adj_container.size(); ++i) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            node_t cur_src_vid = mat_adj->GetSlotVid(adj_idx);

            node_t* nbr_vids[3] = {nullptr};
            int64_t nbr_sz[3] = {0};
            mat_adj->GetAdjList(adj_idx, nbr_vids, nbr_sz);
            for (int k = 0; k < 3; k++) {
                for (int64_t idx = 0; idx < nbr_sz[k]; idx++) {
                    graph[k][cur_src_vid].push_back(nbr_vids[k][idx]);
                    if (k == 1 && !fulllist_latest_flags->Get(cur_src_vid - bitmap_vid_range.GetBegin())) {
                        graph[0][cur_src_vid].push_back(nbr_vids[k][idx]);
                    }
                }
            }
            checked_cnt[0]++;
        }
    }
    */

    temp_timer.start_timer(0);
    std::sort(mat_adj_container.begin(), mat_adj_container.end(),
        [](const void* a, const void* b) -> bool {
        return ((MaterializedAdjacencyLists*) a)->Data() < ((MaterializedAdjacencyLists*) b)->Data();
    });
    temp_timer.stop_timer(0);

    int64_t bytes_to_keep = 0;
    int64_t bytes_to_evict = BytesOfNextBlock;
    int64_t num_max_slots = 0.95 * (DEFAULT_NIO_BUFFER_SIZE) / sizeof(Slot32);

    // X: Old Ones, Y: New Ones
    // blocks_sz[0] = |X ^ Y|
    // blocks_sz[1] = |~X ^ Y| = BytesOfNextBlock - blocks_sz[0]
    // blocks_sz[2] = |X ^ ~Y|
    int64_t blocks_sz[3] = {0};
    int64_t blocks_cnt[3] = {0};

    ALWAYS_ASSERT(!VidToAdjacencyListMappingTable::VidToAdjTable.IsValid(lv));

    temp_timer.start_timer(1);
    // Cache (X ^ Y)
    // XXX - Optimize
    // How to avoid |V| scan?
    int64_t keep_cnts = 0;
    for (node_t v = vid_range_of_current_window.GetBegin(); v <= vid_range_of_current_window.GetEnd(); v++) {
        break;
        node_t src_vid_idx = v - bitmap_vid_range.GetBegin();
        if (!(vids.Get(src_vid_idx) && VidToAdjacencyListMappingTable::VidToAdjTable.Has(v, lv))) continue;
#if USE_DEGREE_THRESHOLD
        node_t prev_degree = TurboDB::OutDegree(v) - TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v);
        node_t cur_degree = TurboDB::OutDegree(v);
				if (!((TurboDB::OutDegree(v) + TurboDB::OutDeleteDegree(v)) > UserArguments::DEGREE_THRESHOLD)) continue;
        //if (!(cur_degree > UserArguments::DEGREE_THRESHOLD || prev_degree > UserArguments::DEGREE_THRESHOLD)) continue;
#endif
        
        ALWAYS_ASSERT (fulllist_latest_flags->Get_Atomic(src_vid_idx) == false);
        
        AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(v, lv);
        MaterializedAdjacencyLists* adj_ptr = (MaterializedAdjacencyLists*) ptr.ptr;
        node_t src_vid = adj_ptr->GetSlotVid(ptr.slot_id);
        node_t* data = adj_ptr->GetBeginPtr(ptr.slot_id);
        int64_t sz = TurboDB::OutDegree(v) - TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v);
        sz += TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v);

        if (remaining_memory_budget < bytes_to_keep + sz * sizeof(node_t) + sizeof(node_t)) continue;

        // we need 'v' and have it in the current window
        to_keep[v - vid_range_of_current_window.GetBegin()] = true;

        bytes_to_keep += sz * sizeof(node_t) + sizeof(Slot32);
        ALWAYS_ASSERT (sz > 0);

        blocks_cnt[0]++;
        keep_cnts++;
        if (keep_cnts == num_max_slots) {
            bytes_to_keep += sizeof(node_t);
            keep_cnts = 0;
        }
    }
    temp_timer.stop_timer(1);
    
    keep_cnts = 0;
    blocks_sz[0] = bytes_to_keep + sizeof(node_t);
    blocks_sz[1] = BytesOfNextBlock - blocks_sz[0];
    remaining_memory_budget -= blocks_sz[0] + blocks_sz[1];
    INVARIANT(remaining_memory_budget >= 0);

    temp_timer.start_timer(2);
    // Cache (X ^ ~Y) if there are remaining space in buffer
    // XXX - Optimize
    // How to avoid |V| scan?
    for (node_t v = vid_range_of_current_window.GetBegin(); v <= vid_range_of_current_window.GetEnd(); v++) {
        break;
        node_t src_vid_idx = v - bitmap_vid_range.GetBegin();
        if (!(VidToAdjacencyListMappingTable::VidToAdjTable.Has(v, lv) && !vids.Get(src_vid_idx))) continue;
        ALWAYS_ASSERT (to_keep[src_vid_idx] == false);

        AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(v, lv);
        MaterializedAdjacencyLists* adj_ptr = (MaterializedAdjacencyLists*) ptr.ptr;
        ALWAYS_ASSERT(v == adj_ptr->GetSlotVid(ptr.slot_id));

#if USE_DEGREE_THRESHOLD
        node_t prev_degree = TurboDB::OutDegree(v) - TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v);
        node_t cur_degree = TurboDB::OutDegree(v);
        if (!(TurboDB::OutDegree(v) + TurboDB::OutDeleteDegree(v)) > UserArguments::DEGREE_THRESHOLD) continue;
        //if (!(cur_degree > UserArguments::DEGREE_THRESHOLD || prev_degree > UserArguments::DEGREE_THRESHOLD)) continue;
#endif

        int64_t adjlist_sz = TurboDB::OutDegree(v) - TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v) + TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v);
        int64_t sz = adjlist_sz * sizeof(node_t) + sizeof(Slot32);
        ALWAYS_ASSERT (adjlist_sz > 0);
        if (remaining_memory_budget < blocks_sz[2] + sz + sizeof(node_t)) continue;

        to_keep[src_vid_idx] = true;
        bytes_to_keep += sz;
        blocks_sz[2] += sz;

        blocks_cnt[2]++;   
        keep_cnts++;
        
        ALWAYS_ASSERT (remaining_memory_budget >= blocks_sz[2] + sizeof(node_t));
        
        if (keep_cnts == num_max_slots) {
            bytes_to_keep += sizeof(node_t);
            blocks_sz[2] += sizeof(node_t);
            keep_cnts = 0;
        }
    }
    fprintf(stdout, "[%ld][Syko Evict] %ld %ld blocks_sz = (%ld %ld %ld) %ld, num_max_slots = %ld\n", PartitionStatistics::my_machine_id(), keep_cnts, remaining_memory_budget, blocks_sz[0], blocks_sz[1], blocks_sz[2], sizeof(node_t), num_max_slots);

    INVARIANT(keep_cnts == 0 || (keep_cnts > 0 && remaining_memory_budget >= blocks_sz[2] + sizeof(node_t)));
    if (keep_cnts > 0 && remaining_memory_budget >= blocks_sz[2] + sizeof(node_t)) {
        bytes_to_keep += sizeof(node_t);
        blocks_sz[2] += sizeof(node_t);
        keep_cnts = 0;
    }
    remaining_memory_budget -= blocks_sz[2];
    INVARIANT(remaining_memory_budget >= 0);
    temp_timer.stop_timer(2);


    //fprintf(stdout, "END [%ld] %ld EvictAsNeededAndPrepareForMergeDelta. blocks_cnt[0] = %ld, .[1] = %ld, .[2] = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, blocks_cnt[0], requested_vertices->GetTotalSingleThread() - blocks_cnt[0], blocks_cnt[2]);
    
    // XXX - dynamic memory allocation
    std::vector<Slot32> slots;
    std::vector<void*> new_mat_adj_container;
    slots.resize(num_max_slots);
    VidToAdjTable.SetCurrentBlockSizeInByte(0, lv);
    VidToAdjTable.GetBlockMemoryStackAllocator(lv).MoveStackPointer(0);
	
    node_t num_hit_vertices = 0;
    int64_t cur_vpage_size = 0;
    int64_t slot_idx = 0;
    node_t accum_data_offset_in_bytes = 0;
    node_t cur_data_offset = 0;
    char* target_buf_ptr;
    temp_timer.start_timer(3);
    for (int64_t i = 0; i < mat_adj_container.size(); i++) {
        break;
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
        ALWAYS_ASSERT (mat_adj->SanityCheckUniqueness());
		ALWAYS_ASSERT (mat_adj->SanityCheck(true, true, true));
        for (int64_t adj_idx = 0; adj_idx < mat_adj->GetNumAdjacencyLists(); adj_idx++) {
            node_t src_vid = mat_adj->GetSlotVid(adj_idx);
            node_t src_vid_idx = src_vid - vid_range_of_current_window.GetBegin();
            if (!to_keep[src_vid_idx]) continue;
            if (kept[src_vid_idx]) continue;
            
            kept[src_vid_idx] = true;

            ALWAYS_ASSERT(src_vid == PartitionStatistics::DegreeOrderToVid(mat_adj->GetSlot(adj_idx)->src_vid));

            ALWAYS_ASSERT((uintptr_t)VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetData(accum_data_offset_in_bytes) <= (uintptr_t)mat_adj->GetBeginPtr(adj_idx));
            ALWAYS_ASSERT((uintptr_t)mat_adj->Data() <= (uintptr_t)mat_adj->GetBeginPtr(adj_idx));
     
            bool marked_in_fulllist_latest_flags = fulllist_latest_flags->Get_Atomic(src_vid_idx);

#ifndef PERFORMANCE
            if (marked_in_fulllist_latest_flags) {
                ALWAYS_ASSERT(mat_adj->GetNumEntriesOfDeltaInsert(adj_idx) == TurboDB::OutInsertDegree(src_vid));
                ALWAYS_ASSERT(mat_adj->GetNumEntriesOfDeltaDelete(adj_idx) == TurboDB::OutDeleteDegree(src_vid));
                ALWAYS_ASSERT(mat_adj->GetNumEntriesOfCurrent(adj_idx) + mat_adj->GetNumEntriesOfDeltaInsert(adj_idx) - mat_adj->GetNumEntriesOfDeltaDelete(adj_idx) == TurboDB::OutDegree(src_vid));
            } else {
                ALWAYS_ASSERT(mat_adj->GetNumEntriesOfCurrent(adj_idx) + mat_adj->GetNumEntriesOfDeltaInsert(adj_idx) - mat_adj->GetNumEntriesOfDeltaDelete(adj_idx) == TurboDB::OutDegree(src_vid) - TurboDB::OutInsertDegree(src_vid) + TurboDB::OutDeleteDegree(src_vid));
            }
#endif

            node_t* data = mat_adj->GetBeginPtr(adj_idx);
            int64_t deltas_sz, sz;

            deltas_sz = TurboDB::OutInsertDegree(src_vid) + TurboDB::OutDeleteDegree(src_vid);
            if (marked_in_fulllist_latest_flags) {
                sz = mat_adj->GetNumEntries(adj_idx);
            } else {
                sz = mat_adj->GetNumEntriesOfCurrent(adj_idx) + mat_adj->GetNumEntriesOfDeltaInsert(adj_idx) - mat_adj->GetNumEntriesOfDeltaDelete(adj_idx) + deltas_sz;
            }
            ALWAYS_ASSERT (sz > 0);

            target_buf_ptr = VidToAdjTable.GetBlockMemoryStackAllocator(lv).TryAllocate(sz * sizeof(node_t));

            // The (current, deltas) will be 'Current' at this update snapshot
            // We also need additional space for 'Delta'
            slots[slot_idx].src_vid = mat_adj->GetSlot(adj_idx)->src_vid;
            slots[slot_idx].end_offset = cur_data_offset + sz - deltas_sz;
            slots[slot_idx].delta_end_offset[0] = slots[slot_idx].end_offset + TurboDB::OutInsertDegree(src_vid);
            slots[slot_idx].delta_end_offset[1] = slots[slot_idx].delta_end_offset[0] + TurboDB::OutDeleteDegree(src_vid);

#ifndef PERFORMANCE
            if (slot_idx == 0) {
                ALWAYS_ASSERT(slots[slot_idx].end_offset == TurboDB::OutDegree(src_vid) - TurboDB::OutInsertDegree(src_vid) + TurboDB::OutDeleteDegree(src_vid));
            } else {
                ALWAYS_ASSERT(slots[slot_idx].end_offset - slots[slot_idx - 1].delta_end_offset[1] == TurboDB::OutDegree(src_vid) - TurboDB::OutInsertDegree(src_vid) + TurboDB::OutDeleteDegree(src_vid));
            }
#endif
            ALWAYS_ASSERT(slots[slot_idx].delta_end_offset[0] - slots[slot_idx].end_offset == TurboDB::OutInsertDegree(src_vid));
            ALWAYS_ASSERT(slots[slot_idx].delta_end_offset[1] - slots[slot_idx].delta_end_offset[0] == TurboDB::OutDeleteDegree(src_vid));

            temp_timer.start_timer(4);
            node_t* dst = (node_t*) &target_buf_ptr[0];
            if (marked_in_fulllist_latest_flags) {
                memmove(&dst[0], data, mat_adj->GetNumEntries(adj_idx) * sizeof(node_t));
            } else {
                // XXX - Optimize
                int64_t c = 0;
                if (mat_adj->GetNumEntriesOfDeltaDelete(adj_idx) > 0) {
                    node_t* begin1 = (node_t*) mat_adj->GetBeginPtr(adj_idx);
                    node_t* begin2 = (node_t*) mat_adj->GetBeginPtrOfDeltaDelete(adj_idx);
                    int64_t b = 0;
                    int64_t a = 0;
                    while (a < mat_adj->GetNumEntriesOfCurrent(adj_idx)) {
                        if (begin1[a] == begin2[b]) {
                            b++;
                            a++;
                            if (b == mat_adj->GetNumEntriesOfDeltaDelete(adj_idx)) break;
                        } else {
                            dst[c] = begin1[a];
                            c++;
                            a++;
                        }
                    }
                    ALWAYS_ASSERT (b == mat_adj->GetNumEntriesOfDeltaDelete(adj_idx));
                    memmove(&dst[c], &begin1[a], (mat_adj->GetNumEntriesOfCurrent(adj_idx) + mat_adj->GetNumEntriesOfDeltaInsert(adj_idx) - a) * sizeof(node_t));
                    c += (mat_adj->GetNumEntriesOfCurrent(adj_idx) + mat_adj->GetNumEntriesOfDeltaInsert(adj_idx) - a);
                } else {
                    memmove(&dst[c], data, mat_adj->GetNumEntries(adj_idx) * sizeof(node_t));
                    c += mat_adj->GetNumEntries(adj_idx);
                }
                memset(&dst[c], -1, deltas_sz * sizeof(node_t));
            }
            temp_timer.stop_timer(4);

            ALWAYS_ASSERT(slots[slot_idx].src_vid == mat_adj->GetSlot(adj_idx)->src_vid);
            ALWAYS_ASSERT(slots[slot_idx].end_offset == cur_data_offset + sz - deltas_sz);
            ALWAYS_ASSERT(slots[slot_idx].delta_end_offset[0] == slots[slot_idx].end_offset + TurboDB::OutInsertDegree(src_vid));
            ALWAYS_ASSERT(slots[slot_idx].delta_end_offset[1] == slots[slot_idx].delta_end_offset[0] + TurboDB::OutDeleteDegree(src_vid));
            ALWAYS_ASSERT(src_vid == PartitionStatistics::DegreeOrderToVid(mat_adj->GetSlot(adj_idx)->src_vid));

            cur_data_offset += sz;
            cur_vpage_size += sz * sizeof(node_t);
            slot_idx++;
            num_hit_vertices++;
            
            
            if (slot_idx == num_max_slots) {
                target_buf_ptr = VidToAdjTable.GetBlockMemoryStackAllocator(lv).Allocate(slot_idx * sizeof(Slot32) + sizeof(node_t));
                if (target_buf_ptr == NULL) break;
                
                target_buf_ptr -= cur_vpage_size;
                cur_vpage_size += slot_idx * sizeof(Slot32) + sizeof(node_t);
                accum_data_offset_in_bytes += cur_data_offset * sizeof(node_t) + slot_idx * sizeof(Slot32) + sizeof(node_t);

                MaterializedAdjacencyLists* new_mat_adj = new MaterializedAdjacencyLists() ;
                new_mat_adj->Initialize(cur_vpage_size, cur_vpage_size, target_buf_ptr);
                new_mat_adj_container.push_back((void*) new_mat_adj);

                new_mat_adj->NumEntry() = slot_idx;
                for (int64_t idx = 0; idx < slot_idx; idx++) {
                    *new_mat_adj->GetSlot(idx) = slots[idx];
                }

		        ALWAYS_ASSERT (new_mat_adj->SanityCheckUsingDegree(true));
                
                slot_idx = 0;
                cur_vpage_size = 0;
                cur_data_offset = 0;
            }
        }            
		if (target_buf_ptr == NULL) break;
    }
    temp_timer.stop_timer(3);
        
    if (slot_idx > 0) {
        target_buf_ptr = VidToAdjTable.GetBlockMemoryStackAllocator(lv).Allocate(slot_idx * sizeof(Slot32) + sizeof(node_t));
		if (target_buf_ptr != NULL) {
        
            target_buf_ptr -= cur_vpage_size;
            cur_vpage_size += slot_idx * sizeof(Slot32) + sizeof(node_t);
            accum_data_offset_in_bytes += cur_data_offset * sizeof(node_t) + slot_idx * sizeof(Slot32) + sizeof(node_t);

            MaterializedAdjacencyLists* new_mat_adj = new MaterializedAdjacencyLists() ;
            new_mat_adj->Initialize(cur_vpage_size, cur_vpage_size, target_buf_ptr);
            new_mat_adj_container.push_back((void*) new_mat_adj);
            new_mat_adj->NumEntry() = slot_idx;
            for (int64_t idx = 0; idx < slot_idx; idx++) {
                *new_mat_adj->GetSlot(idx) = slots[idx];
            }
            ALWAYS_ASSERT (new_mat_adj->SanityCheckUsingDegree(true));
        }
    }

	INVARIANT (accum_data_offset_in_bytes == VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetStackPointer());
    
    for (int64_t i = 0; i < mat_adj_container.size(); i++) {
        ALWAYS_ASSERT (mat_adj_container[i] != nullptr);
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
        delete mat_adj;
        mat_adj_container[i] = nullptr;
    }
    mat_adj_container.clear();
   
    VidToAdjTable.GetBlockMemoryStackAllocator(lv).Allocate(sizeof(AdjacencyListMemoryPointer) * vid_range_of_current_window.length());
   
    for (int64_t i = 0; i < new_mat_adj_container.size(); i++) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) new_mat_adj_container[i];
	    ALWAYS_ASSERT (mat_adj->GetNumAdjacencyLists() != 0);
        mat_adj->Initialize(mat_adj->Size(), mat_adj->Size(), ((char*) mat_adj->Data()) + sizeof(AdjacencyListMemoryPointer) * vid_range_of_current_window.length());
    }
    
    AdjacencyListMemoryPointer* temp_vid_to_adj = (AdjacencyListMemoryPointer*) VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetData(0);
    memmove(((char*) temp_vid_to_adj) + sizeof(AdjacencyListMemoryPointer) * vid_range_of_current_window.length(), (char*) temp_vid_to_adj, accum_data_offset_in_bytes);

#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
	for (int64_t i = 0; i < vid_range_of_current_window.length(); i++) {
		temp_vid_to_adj[i].ptr = nullptr;
		temp_vid_to_adj[i].slot_id = -1;
	}
    VidToAdjTable.SetMaterializedVidRange(vid_range_of_current_window, lv);
	VidToAdjTable.SetAdjacencyListMemoryPointer(temp_vid_to_adj, lv);
    VidToAdjTable.SetCurrentBlockSizeInByte(accum_data_offset_in_bytes, lv);

    INVARIANT(VidToAdjTable.GetCurrentBlockSizeInByte(lv) + sizeof(AdjacencyListMemoryPointer) * vid_range_of_current_window.length() == VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes());

#ifndef PERFORMANCE
    for (int64_t i = 0; i < new_mat_adj_container.size(); i++) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) new_mat_adj_container[i];
	    ALWAYS_ASSERT (mat_adj->GetNumAdjacencyLists() != 0);
        node_t cur_src_vid = -1;
        node_t cur_src_vid_idx = -1;
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            cur_src_vid = mat_adj->GetSlotVid(adj_idx);
            cur_src_vid_idx = cur_src_vid - vid_range_of_current_window.GetBegin();
            ALWAYS_ASSERT(to_keep[cur_src_vid_idx]);
            ALWAYS_ASSERT(vid_range_of_current_window.contains(cur_src_vid));
            ALWAYS_ASSERT(!VidToAdjacencyListMappingTable::VidToAdjTable.Has(cur_src_vid, lv));
            for (int64_t delta_idx = 0; delta_idx < TurboDB::OutInsertDegree(cur_src_vid) + TurboDB::OutDeleteDegree(cur_src_vid); delta_idx++) {
                if (!fulllist_latest_flags->Get_Atomic(cur_src_vid_idx)) {
                    if(mat_adj->GetBeginPtrOfDeltaInsert(adj_idx)[delta_idx] != -1){
                        fprintf(stdout, "[EvictAsNeededAndPrepareForMergeDelta::Check] src = %ld, dst_deltas[%ld] (%p) = %ld != -1 // |DeltaInsert| = %ld, |DeltaDelete| = %ld\n", cur_src_vid, delta_idx, &mat_adj->GetBeginPtrOfDeltaInsert(adj_idx)[delta_idx], mat_adj->GetBeginPtrOfDeltaInsert(adj_idx)[delta_idx], (int64_t) TurboDB::OutInsertDegree(cur_src_vid), (int64_t) TurboDB::OutDeleteDegree(cur_src_vid));
                    }
                    ALWAYS_ASSERT(mat_adj->GetBeginPtrOfDeltaInsert(adj_idx)[delta_idx] == -1);
                } else {
                    ALWAYS_ASSERT(mat_adj->GetBeginPtrOfDeltaInsert(adj_idx)[delta_idx] != -1);
                }
            } 
        }
    }
#endif

    //int64_t req_cnts = vids.GetTotalSingleThread();
    //fprintf(stdout, "Cached %ld among %ld (%.2f %)\n", num_hit_vertices, req_cnts, 100 * (double) num_hit_vertices / req_cnts); 

    temp_timer.start_timer(5);
    // XXX - Optimize
    VidToAdjacencyListMappingTable::VidToAdjTable.ContainerLock(lv);
    //VidToAdjacencyListMappingTable::VidToAdjTable.Put(new_mat_adj_container, lv); // at EvictAsNeededAndPrepareForMergeDelta
    for (int64_t i = 0; i < new_mat_adj_container.size(); i++) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) new_mat_adj_container[i];
        mat_adj_container.push_back((void*) mat_adj);
    }
#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
    for (int64_t i = 0; i < new_mat_adj_container.size(); i++) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) new_mat_adj_container[i];
	
        node_t cur_src_vid = -1;
        node_t cur_src_vid_idx;
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            cur_src_vid = mat_adj->GetSlotVid(adj_idx);
            cur_src_vid_idx = cur_src_vid - bitmap_vid_range.GetBegin();
            ALWAYS_ASSERT(to_keep[cur_src_vid_idx]);
            ALWAYS_ASSERT(vid_range_of_current_window.contains(cur_src_vid));
            ALWAYS_ASSERT(!VidToAdjacencyListMappingTable::VidToAdjTable.Has(cur_src_vid, lv));
            
            if(vids.Get_Atomic(cur_src_vid_idx)) {
                vids.Clear_Atomic(cur_src_vid_idx);
            }
            
            temp_vid_to_adj[cur_src_vid_idx].ptr = mat_adj;
            temp_vid_to_adj[cur_src_vid_idx].slot_id = adj_idx;
        }
    }
    VidToAdjacencyListMappingTable::VidToAdjTable.ContainerUnlock(lv);
    for (int64_t i = 0; i < new_mat_adj_container.size(); i++) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) new_mat_adj_container[i];
        cb((void*) mat_adj);
    }
    temp_timer.stop_timer(5);
    ALWAYS_ASSERT(!VidToAdjacencyListMappingTable::VidToAdjTable.IsValid(lv));

    /*
    // XXX - syko DEBUG
    for (int64_t i = 0; i < mat_adj_container.size(); ++i) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            node_t cur_src_vid = mat_adj->GetSlotVid(adj_idx);

            node_t* nbr_vids[3] = {nullptr};
            int64_t nbr_sz[3] = {0};
            mat_adj->GetAdjList(adj_idx, nbr_vids, nbr_sz);
            for (int k = 0; k <= 1; k++) {
                if (k == 0) {
                    if (cur_src_vid == 1149900 || graph[k][cur_src_vid].size() != nbr_sz[k]) {
                        fprintf(stdout, "\t[%ld] %ld EvictAsNeededAndPrepareForMergeDelta. src_vid = %ld, k = %ld, %ld != %ld, cache_updated = %ld, %ld %ld %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, cur_src_vid, k, graph[k][cur_src_vid].size(), nbr_sz[k], fulllist_latest_flags->Get(cur_src_vid - bitmap_vid_range.GetBegin()) ? 1L : 0L, TurboDB::OutDegree(cur_src_vid), TurboDB::OutInsertDegree(cur_src_vid), TurboDB::OutDeleteDegree(cur_src_vid));
                    }
                    ALWAYS_ASSERT(graph[k][cur_src_vid].size() == nbr_sz[k]);
                }
                for (int64_t idx = 0; idx < nbr_sz[k]; idx++) {
                    ALWAYS_ASSERT(std::find(std::begin(graph[0][cur_src_vid]), std::end(graph[0][cur_src_vid]), nbr_vids[k][idx]) != std::end(graph[k][cur_src_vid]));
                    //ALWAYS_ASSERT(graph[k][cur_src_vid][idx] == nbr_vids[k][idx]);
                }
            }
            checked_cnt[1]++;
        }
    }
    */
    

    ALWAYS_ASSERT(new_mat_adj_container.size() == mat_adj_container.size());

    // 'VidToAdjTable' will become validaded after 'MergeDeltaWithCachedCurrent'
    // at TG_AdjWindow.hpp
    //VidToAdjTable.Validate(lv);
    temp_timer.stop_timer(6);
    fprintf(stdout, "[%ld] EvictAsNeededAndPrepareForMergeDelta elapsed %.3f %.3f %.3f %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), temp_timer.get_timer(6), temp_timer.get_timer(0), temp_timer.get_timer(1), temp_timer.get_timer(2), temp_timer.get_timer(3), temp_timer.get_timer(4), temp_timer.get_timer(5)); 
}

void MaterializedAdjacencyLists::MergeDeltaWithCachedCurrent(BitMap<node_t>* fulllist_latest_flags, Range<node_t> bitmap_vid_range, BitMap<node_t>* requested_vertices, Range<node_t> vid_range, int lv, Range<int64_t> dst_edge_partitions, std::vector<bool>* ptr_to_keep, std::function<void(void*)> cb) {
    turbo_timer temp_timer;
    temp_timer.start_timer(0);
    
    std::vector<bool>& to_keep = *ptr_to_keep;
    std::vector<void*>& mat_adj_container = VidToAdjTable.GetMatAdjContainer(lv);

    RequestRespond::InitializeIoInterface();
    diskaio::DiskAioInterface** my_io = RequestRespond::GetMyDiskIoInterface();
    
    BitMap<node_t>& vids = *requested_vertices;
    Range<node_t> vid_range_of_current_window = MaterializedAdjacencyLists::VidToAdjTable.GetMaterializedVidRange(lv);
    ALWAYS_ASSERT(vid_range_of_current_window.GetBegin() != -1 && vid_range_of_current_window.GetEnd() != -1);


    EdgeType e_type = OUTEDGE; // XXX
    int version_id = UserArguments::UPDATE_VERSION;
    PageID pin_counts = 0;
    PageID pending_page_counts = 0;
    std::queue<PageID> pending_pages;
    Page* page_buffer = nullptr;
    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;
    
    temp_timer.start_timer(1);
    // XXX - Optimize
    // How to parallelize it?
    for (int dbtype = 0; dbtype < 2; dbtype++) {
        if (!TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType) dbtype, UserArguments::UPDATE_VERSION)) continue;

        for (PartID i = dst_edge_partitions.GetBegin(); i <= dst_edge_partitions.GetEnd(); i++) {
            VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version_id, (DynamicDBType)dbtype);

            for (PageID pid = TurboDB::GetTurboDB(e_type)->GetFirstPageId(i, version_id, (DynamicDBType)dbtype); pid <= TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version_id, (DynamicDBType)dbtype); pid++) {
                PageID table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version_id, pid, e_type, (DynamicDBType)dbtype);
                AioRequest req;
                req.user_info.db_info.page_id = pid;
                req.user_info.db_info.version_id = version_id;
                req.user_info.db_info.e_type = (int8_t)e_type;
                req.user_info.db_info.d_type = (int8_t)dbtype;
                req.user_info.do_user_cb = false;
                req.user_info.func = (void*) &InvokeUserCallback;
                req.buf = nullptr;
                
                TurboDB::GetBufMgr()->PinPageCallbackLikelyCached(PartitionStatistics::my_machine_id(), req, my_io);
                //TurboDB::GetBufMgr()->PinPageCallback(PartitionStatistics::my_machine_id(), req, my_io);

                pin_counts++;
                pending_page_counts++;
                pending_pages.push(table_page_id);

                if (pid == TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version_id, (DynamicDBType)dbtype) || pending_page_counts >= 1024) {
                    RequestRespond::WaitMyPendingDiskIO(my_io);

                    while(!pending_pages.empty()) {
                        PageID cur_pid = pending_pages.front();
                        pending_pages.pop();
                        pending_page_counts--;

                        ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(cur_pid));
                        TurboDB::GetBufMgr()->GetPagePtr_Unsafe(cur_pid, page_buffer);
                        adjlist_iter.SetCurrentPage(cur_pid, page_buffer);
                        while (adjlist_iter.GetNextAdjList(nbrlist_iter) == ReturnStatus::OK) {
                            int64_t sz = nbrlist_iter.GetNumEntries();
                            node_t vid = nbrlist_iter.GetSrcVid();
                            node_t* data = nbrlist_iter.GetData();

                            node_t src_vid_idx = vid - vid_range_of_current_window.GetBegin();
                            if (!to_keep[src_vid_idx]) continue;
                            if (fulllist_latest_flags->Get_Atomic(src_vid_idx)) continue;

                            //fprintf(stdout, "%ld [MergeDeltaWithCachedCurrent::Merging] src_vid = %ld, |deltas| = %ld\n", UserArguments::UPDATE_VERSION, vid, sz);

                            AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(vid, lv);
                            MaterializedAdjacencyLists* adj_ptr = ptr.ptr;

                            int64_t delta_idx = 0;
                            int64_t delta_degrees = (dbtype == INSERT) ? TurboDB::OutInsertDegree(vid) : TurboDB::OutDeleteDegree(vid);
                            // XXX - Optimize
                            for (; delta_idx < delta_degrees; delta_idx++) {
                                if (dbtype == INSERT && adj_ptr->GetBeginPtrOfDeltaInsert(ptr.slot_id)[delta_idx] == -1) break;
                                if (dbtype == DELETE && adj_ptr->GetBeginPtrOfDeltaDelete(ptr.slot_id)[delta_idx] == -1) break;
                            }             
                            if (delta_idx != delta_degrees) {
                                // XXX - Optimize
                                if (dbtype == INSERT) {
                                    for (int m = 0; m < sz; m++) {
                                        node_t tmp_vid = PartitionStatistics::GetPrevVisibleDstVid(data[m]);
                                        ALWAYS_ASSERT(tmp_vid != -1);
                                        ALWAYS_ASSERT(adj_ptr->GetBeginPtrOfDeltaInsert(ptr.slot_id)[delta_idx + m] == -1);
                                        adj_ptr->GetBeginPtrOfDeltaInsert(ptr.slot_id)[delta_idx + m] = tmp_vid;
                                    }
                                } else {
                                    for (int m = 0; m < sz; m++) {
                                        ALWAYS_ASSERT(data[m] < PartitionStatistics::num_total_nodes());
                                        ALWAYS_ASSERT(adj_ptr->GetBeginPtrOfDeltaDelete(ptr.slot_id)[delta_idx + m] == -1);
                                        adj_ptr->GetBeginPtrOfDeltaDelete(ptr.slot_id)[delta_idx + m] = data[m];
                                    }
                                }
                            }
                        }
                        TurboDB::GetBufMgr()->UnpinPage(PartitionStatistics::my_machine_id(), cur_pid);
                    }
                }
            }
        }
    }
    temp_timer.stop_timer(1);

    ALWAYS_ASSERT(pending_page_counts == 0);
    ALWAYS_ASSERT(pending_pages.empty());

    temp_timer.start_timer(2);
#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
    for (int64_t i = 0; i < mat_adj_container.size(); i++) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
#ifndef PERFORMANCE
        for (int64_t adj_idx = 0; adj_idx < mat_adj->GetNumAdjacencyLists(); adj_idx++) {
            node_t src_vid = mat_adj->GetSlotVid(adj_idx);
            ALWAYS_ASSERT(to_keep[src_vid - vid_range_of_current_window.GetBegin()]);
            node_t* nbr_vids[3] = {nullptr};
            int64_t nbr_sz[3] = {0};
            mat_adj->GetAdjList(adj_idx, nbr_vids, nbr_sz);
            for (int k = 0; k < 3; k++) {
                for (int64_t idx = 0; idx < nbr_sz[k]; idx++) {
                    ALWAYS_ASSERT(nbr_vids[k][idx] != -1);
                }
            }
        }
#endif
        cb((void*) mat_adj);
        ALWAYS_ASSERT(mat_adj->SanityCheckUsingDegree(true));
        ALWAYS_ASSERT(mat_adj->SanityCheck());
    }
    temp_timer.stop_timer(2);
    temp_timer.stop_timer(0);
    fprintf(stdout, "[%ld] MergeDeltaWithCachedCurrent %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), temp_timer.get_timer(0), temp_timer.get_timer(1), temp_timer.get_timer(2)); 
}

void MaterializedAdjacencyLists::SlideBlockMemoryWindow (Range<node_t> bitmap_vid_range, BitMap<node_t>* requested_vertices, Range<node_t> target_vid_range, int lv, int64_t BytesOfNextBlock) {
    fprintf(stdout, "[%ld] SlideBlockMemoryWindow(bitmap_vid_range[%ld, %ld], lv = %ld, target_vid_range[%ld, %ld], lv %ld, BytesOfNextBlock %ld) %ld %ld %ld\n", PartitionStatistics::my_machine_id(), (int64_t) bitmap_vid_range.GetBegin(), (int64_t) bitmap_vid_range.GetEnd(), lv, (int64_t) target_vid_range.GetBegin(), (int64_t) target_vid_range.GetEnd(), (int64_t) lv, (int64_t) BytesOfNextBlock, (int64_t) VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize(), (int64_t) VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes(), (int64_t) VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - (int64_t) VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes());

    INVARIANT(VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() >= sizeof(AdjacencyListMemoryPointer) * bitmap_vid_range.length() + BytesOfNextBlock);
    INVARIANT(BytesOfNextBlock != 0);

	BitMap<node_t>& vids = *requested_vertices;
	std::vector<void*>& mat_adj_container = VidToAdjTable.GetMatAdjContainer(lv);
	Range<node_t> cur_vid_range = VidToAdjTable.GetMaterializedVidRange(lv);

    if (!MaterializedAdjacencyLists::VidToAdjTable.DoesBlockExist(lv)) {
        //fprintf(stdout, "[%ld] SlideBlockMemoryWindow::Initialize (PrevBlock does not exist)\n", PartitionStatistics::my_machine_id());
	    ALWAYS_ASSERT(VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() == VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes());
	    VidToAdjTable.GetBlockMemoryStackAllocator(lv).FreeAll();
		VidToAdjTable.InitializeContainer(target_vid_range, lv);
		return;
	}
	VidToAdjTable.Invalidate(lv, true);

    // XXX - syko: for debugging later...
    {
	    int64_t BytesOfCurBlock = VidToAdjTable.GetCurrentBlockSizeInByte(lv);
        if (BytesOfCurBlock + sizeof(AdjacencyListMemoryPointer) * bitmap_vid_range.length() != VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes()) {
            fprintf(stdout, "[%ld] MAYBE WRONG! (GetCurrentBlockSizeInByte) %ld + (Index) %ld * %ld = %ld != %ld - %ld = %ld\n", PartitionStatistics::my_machine_id(), BytesOfCurBlock, sizeof(AdjacencyListMemoryPointer), bitmap_vid_range.length(), BytesOfCurBlock + sizeof(AdjacencyListMemoryPointer) * bitmap_vid_range.length(), VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize(), VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes(), VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes());
        }
    }

	std::queue<std::future<void>> reqs_to_wait;
    int64_t BytesOfCurBlock = VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes() - sizeof(AdjacencyListMemoryPointer) * bitmap_vid_range.length();
	int64_t BytesToReclaim = (BytesOfCurBlock <= BytesOfNextBlock) ? BytesOfCurBlock : BytesOfNextBlock;
	int64_t BytesOfTempBlock = BytesOfCurBlock;
	
	VidToAdjTable.Lock(lv);
	VidToAdjTable.ContainerLock(lv);

    /*
    // XXX - syko DEBUG
    std::vector<std::vector<node_t>> graph[3];
    graph[0].resize(PartitionStatistics::num_total_nodes());
    graph[1].resize(PartitionStatistics::num_total_nodes());
    graph[2].resize(PartitionStatistics::num_total_nodes());
    int64_t checked_cnt[2] = {0};
    for (int64_t i = 0; i < mat_adj_container.size(); ++i) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            node_t cur_src_vid = mat_adj->GetSlotVid(adj_idx);
            
            node_t* nbr_vids[3] = {nullptr};
            int64_t nbr_sz[3] = {0};
            mat_adj->GetAdjList(adj_idx, nbr_vids, nbr_sz);
            for (int k = 0; k < 3; k++) {
                for (int64_t idx = 0; idx < nbr_sz[k]; idx++) {
                    graph[k][cur_src_vid].push_back(nbr_vids[k][idx]);
                }
            }
            checked_cnt[0]++;
        }
    }
    */


#ifndef PERFORMANCE
	for (int64_t i = 0; i < mat_adj_container.size(); ++i) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
		ALWAYS_ASSERT (mat_adj->SanityCheck());
    }
#endif
    std::sort(mat_adj_container.begin(), mat_adj_container.end(),
	[](const void* a, const void* b) -> bool {
		return ((MaterializedAdjacencyLists*) a)->GetSrcVidRange().GetBegin() < ((MaterializedAdjacencyLists*) b)->GetSrcVidRange().GetBegin();
	});

    VidToAdjTable.ContainerUnlock(lv);

    
    int64_t num_to_keep = 0;
	int64_t num_to_free = 0;
	// How many Mat.Adjlists are going to be freed?
	for (int64_t i = 0; i < mat_adj_container.size(); ++i) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
		Range<node_t> mat_adj_src_vid_range = mat_adj->GetSrcVidRange();
        ALWAYS_ASSERT (bitmap_vid_range.contains(mat_adj_src_vid_range));

		//fprintf(stdout, "[%ld][SlideBlockMemoryWindow] [%ld, %ld] in [%ld, %ld]\n", PartitionStatistics::my_machine_id(), mat_adj_src_vid_range.GetBegin(), mat_adj_src_vid_range.GetEnd(), bitmap_vid_range.GetBegin(), bitmap_vid_range.GetEnd());

		//if ((std::rand() % 10 <= 5) && BytesOfCurBlock - BytesOfTempBlock >= BytesToReclaim) break;
		if (BytesOfCurBlock - BytesOfTempBlock >= BytesToReclaim) break;
		BytesOfTempBlock -= mat_adj->Size();
		num_to_free++;
	}
	num_to_keep = (mat_adj_container.size() - num_to_free);
	
    INVARIANT (BytesOfCurBlock - BytesOfTempBlock >= BytesToReclaim);
	INVARIANT (num_to_keep + num_to_free == mat_adj_container.size());
	INVARIANT (!VidToAdjTable.IsValid(lv));


	VidToAdjTable.GetBlockMemoryStackAllocator(lv).MoveStackPointer(0);
	AdjacencyListMemoryPointer* temp_vid_to_adj = (AdjacencyListMemoryPointer*) VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(lv).Allocate(sizeof(AdjacencyListMemoryPointer) * bitmap_vid_range.length());    // Alloc VidToAdjTable
	#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
	for (int64_t i = 0; i < bitmap_vid_range.length(); i++) {
		temp_vid_to_adj[i].ptr = nullptr;
		temp_vid_to_adj[i].slot_id = -1;
	}
	
    std::vector<void*> mat_adj_container_to_free;
	std::vector<void*> mat_adj_container_to_keep;
	// [0, num_to_free)
	for (int64_t i = 0; i < num_to_free; i++) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
		mat_adj_container_to_free.push_back((void*) mat_adj);
	}
	// [num_to_free, END)
    for (int64_t i = num_to_free; i < mat_adj_container.size(); i++) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
		ALWAYS_ASSERT (bitmap_vid_range.contains(mat_adj->GetSrcVidRange()));
		mat_adj_container_to_keep.push_back((void*) mat_adj);
    }


    Range<node_t> prev_block_vid_range(-1, -1);
	Range<node_t> next_block_vid_range(-1, bitmap_vid_range.GetEnd());
    if (mat_adj_container.size() > 0) {
		MaterializedAdjacencyLists* first_mat_adj = (MaterializedAdjacencyLists*) mat_adj_container.front();
		MaterializedAdjacencyLists* end_mat_adj = (MaterializedAdjacencyLists*) mat_adj_container.back();
        prev_block_vid_range.Set(first_mat_adj->GetSlotVid(0), end_mat_adj->GetSlotVid(end_mat_adj->GetNumAdjacencyLists() - 1));
        next_block_vid_range.SetBegin(end_mat_adj->GetSlotVid(end_mat_adj->GetNumAdjacencyLists() - 1) + 1);
    }
    if (mat_adj_container_to_keep.size() > 0) {
        MaterializedAdjacencyLists* first_mat_adj = (MaterializedAdjacencyLists*) mat_adj_container_to_keep.front();
        next_block_vid_range.SetBegin(first_mat_adj->GetSlotVid(0));
    }
	fprintf(stdout, "[%ld] prev_block_vid_range = [%lld, %lld], next_target_block_vid_range = [%lld, %lld], num = %ld, num_to_free = %lld, num_to_keep = %ld\n", PartitionStatistics::my_machine_id(), prev_block_vid_range.GetBegin(), prev_block_vid_range.GetEnd(), next_block_vid_range.GetBegin(), next_block_vid_range.GetEnd(), mat_adj_container.size(), num_to_free, mat_adj_container_to_keep.size());


    // Keep Mat.Adj of [num_to_free, END]
	// Insert those into the new index table
	#pragma omp parallel for num_threads(UserArguments::NUM_THREADS)
    for (int64_t i = num_to_free; i < mat_adj_container.size(); i++) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
		for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
			node_t cur_src_vid = mat_adj->GetSlotVid(adj_idx);
			int64_t cur_src_vid_idx = cur_src_vid - bitmap_vid_range.GetBegin();
			ALWAYS_ASSERT (bitmap_vid_range.contains(cur_src_vid));
			temp_vid_to_adj[cur_src_vid_idx].ptr = mat_adj;
			temp_vid_to_adj[cur_src_vid_idx].slot_id = adj_idx;
		}
	}

	VidToAdjTable.ContainerLock(lv);
	mat_adj_container.erase(mat_adj_container.begin(), mat_adj_container.begin() + num_to_free);
	VidToAdjTable.SetMaterializedVidRange(bitmap_vid_range, lv);
	VidToAdjTable.SetAdjacencyListMemoryPointer(temp_vid_to_adj, lv);
	VidToAdjTable.ContainerUnlock(lv);

	INVARIANT (!VidToAdjTable.IsValid(lv));

	VidToAdjTable.ContainerLock(lv);
	VidToAdjTable.SetCurrentBlockSizeInByte(0, lv);
	std::sort(mat_adj_container.begin(), mat_adj_container.end(),
	[](const void* a, const void* b) -> bool {
		return ((MaterializedAdjacencyLists*) a)->Data() < ((MaterializedAdjacencyLists*) b)->Data();
	});
	VidToAdjTable.ContainerUnlock(lv);

	int64_t current_block_memory_offset = sizeof(AdjacencyListMemoryPointer) * bitmap_vid_range.length();
	char* prev_src_buf_ptr = VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetData(0) + current_block_memory_offset;
	char* vid_to_adj_tbl_offset = prev_src_buf_ptr;
	INVARIANT (current_block_memory_offset == VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetSize() - VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes());
	for (int64_t i = 0; i < num_to_keep; i++) {
		MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
		ALWAYS_ASSERT (mat_adj != nullptr);
		ALWAYS_ASSERT (mat_adj->Data() != nullptr);
		char* buf_ptr = mat_adj->Data();
		int64_t buf_size = mat_adj->Size();
		char* target_buf_ptr = VidToAdjTable.GetBlockMemoryStackAllocator(lv).Allocate(buf_size);   // Alloc memory space for 'mat_adj'

		ALWAYS_ASSERT (buf_ptr >= prev_src_buf_ptr);
		ALWAYS_ASSERT (target_buf_ptr >= vid_to_adj_tbl_offset);
		ALWAYS_ASSERT (mat_adj->SanityCheck());

		memmove (target_buf_ptr, buf_ptr, buf_size);
		mat_adj->Initialize(buf_size, buf_size, target_buf_ptr);
		current_block_memory_offset += buf_size;
		prev_src_buf_ptr = buf_ptr;

	    //fprintf(stdout, "[%ld] [SlideBlockMemoryWindow::AddCurrentBlockSizeInByte] %ld\n", PartitionStatistics::my_machine_id(), buf_size);
		VidToAdjTable.AddCurrentBlockSizeInByte(buf_size, lv);  // SlideBlockMemoryWindow
		ALWAYS_ASSERT (VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes() >= BytesOfNextBlock);
		ALWAYS_ASSERT (mat_adj->SanityCheck());
	}
	INVARIANT (current_block_memory_offset == VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetStackPointer());
	INVARIANT (VidToAdjTable.GetBlockMemoryStackAllocator(lv).GetRemainingBytes() >= BytesOfNextBlock);

    for (int64_t i = 0; i < mat_adj_container_to_free.size(); i++) {
        ALWAYS_ASSERT (mat_adj_container_to_free[i] != nullptr);
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container_to_free[i];
        delete mat_adj;
        mat_adj_container_to_free[i] = nullptr;
    }


    /*
    // XXX - syko DEBUG
    for (int64_t i = 0; i < mat_adj_container.size(); ++i) {
        MaterializedAdjacencyLists* mat_adj = (MaterializedAdjacencyLists*) mat_adj_container[i];
        for(int64_t adj_idx = 0 ; adj_idx < mat_adj->GetNumAdjacencyLists() ; adj_idx++) {
            node_t cur_src_vid = mat_adj->GetSlotVid(adj_idx);

            node_t* nbr_vids[3] = {nullptr};
            int64_t nbr_sz[3] = {0};
            mat_adj->GetAdjList(adj_idx, nbr_vids, nbr_sz);
            for (int k = 0; k < 3; k++) {
                ALWAYS_ASSERT(graph[k][cur_src_vid].size() == nbr_sz[k]);
                for (int64_t idx = 0; idx < nbr_sz[k]; idx++) {
                    ALWAYS_ASSERT(graph[k][cur_src_vid][idx] == nbr_vids[k][idx]);
                }
            }
            checked_cnt[1]++;
        }
    }
    
    fprintf(stdout, "[%ld] %ld [SlideBlockMemoryWindow] %ld, %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, checked_cnt[0], checked_cnt[1]);
*/
    
}


void MaterializedAdjacencyLists::DropMaterializedViewInMemory (int lv) {
	VidToAdjTable.Invalidate(lv, true);
	VidToAdjTable.DeleteAll(lv);
}

void MaterializedAdjacencyLists::SendMaterializedAdjListWithUDF(MaterializedAdjacencyLists mat_adj, int64_t to) {
	if (mat_adj.IsEmpty()) return;
	UserCallbackRequest udf_req;
	udf_req.rt = UserCallback;
	udf_req.from = PartitionStatistics::my_machine_id();
	udf_req.to = to;
	udf_req.parm1 = -1;

	RequestRespond::SendRequestReturnContainer(udf_req, (char*) mat_adj.Data(), mat_adj.Size(), mat_adj.GetContainer());
	return;
}

// TODO - 'CurPid' is being passed by value, which may need to be avoided
void MaterializedAdjacencyLists::MaterializeAndEnqueue(tbb::concurrent_queue<UdfSendRequest>* req_buffer_queue, Range<node_t> bitmap_vid_range, TwoLevelBitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, int64_t to, int64_t lv, int64_t* stop_flag, bool is_full_list, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>> CurPid) {
	if (std::atomic_load((std::atomic<int64_t>*) stop_flag) == 1) {
        RequestRespond::ReturnTempDataBuffer(CurPid.get_container());
        return;
    }

    turbo_timer timer;
    timer.start_timer(0);
	ALWAYS_ASSERT(req_buffer_queue != NULL);
    ALWAYS_ASSERT(version_range.length() <= CurPid[0].size());
    INVARIANT(vid_range.length() * sizeof(node_t) < 4 * 1024 * 1024);

    timer.start_timer(1);
    bool use_fulllist_db = (TurboDB::GetTurboDB(e_type)->is_full_list_db_exist()) && UserArguments::USE_FULLIST_DB;
    MaterializedAdjacencyLists mat_adj;
    if (is_full_list) {
        if (UserArguments::USE_DELTA_NWSM) {
            mat_adj.MaterializeFullListAsCurrentAndDelta(vids, vid_range, dst_edge_partitions, false, lv, version_range, e_type, d_type, CurPid, use_fulllist_db);
        } else {
            if (d_type == ALL) {
                mat_adj.MaterializeFullList(vids, vid_range, dst_edge_partitions, false, lv, version_range, e_type, INSERT, use_fulllist_db);
            } else {
                mat_adj.MaterializeFullList(vids, vid_range, dst_edge_partitions, false, lv, version_range, e_type, d_type, use_fulllist_db);
            }

        }
    } else {
        LOG_ASSERT(false);  // by syko @ 2019.11.26
    }
    timer.stop_timer(1);
    
	if (mat_adj.IsEmpty()) {
        RequestRespond::ReturnTempDataBuffer(CurPid.get_container());
        return;
    }
    ALWAYS_ASSERT(mat_adj.SanityCheckUsingVidRange(vid_range));
    
    //mat_adj.PrintContents(true, true, true);

	UdfSendRequest udf_req;
	udf_req.req.rt = UserCallback;
	udf_req.req.from = PartitionStatistics::my_machine_id();
	udf_req.req.to = to;
	//udf_req.req.parm1 = 1;  // Sorted; //XXX for materialize partial list
	udf_req.req.parm1 = 0;  // Not sorted; delegated //XXX for full list
	udf_req.req.data_size = mat_adj.Size();
	udf_req.req.vid_range = vid_range;
	udf_req.cont = mat_adj.GetContainer();
    

    timer.start_timer(2);
	int backoff = 1;
	while (req_buffer_queue->unsafe_size() > MAT_ADJ_MAX_PENDING_REQS) {
		if (backoff < 128) { backoff = 2 * backoff; }
		usleep(backoff * 8);
	}
    timer.stop_timer(2);
	
    timer.start_timer(3);
    // XXX - syko
    // Caching the requested MatAdj for (LV>=1) into Window at LV=0
    // before responding the request from the local/remote machines
    // (Only if the LV-1 window is finalized)
    if (lv > 0) {
    //if (lv > 0 && udf_req.req.to != PartitionStatistics::my_machine_id()) {
        VidToAdjacencyListMappingTable::VidToAdjTable.AcquireReference(0);
        timer.start_timer(4);
        if (VidToAdjacencyListMappingTable::VidToAdjTable.IsValid(0) && VidToAdjacencyListMappingTable::VidToAdjTable.IsWindowFinalized(0)) {
            VidToAdjacencyListMappingTable::VidToAdjTable.PutWithCopy((void*) &mat_adj, vid_range, 0);  // at MaterializeAndEnqueue
        }
        timer.stop_timer(4);
        VidToAdjacencyListMappingTable::VidToAdjTable.ReleaseReference(0);
    }
    timer.stop_timer(3);
   
    timer.start_timer(5);
    //MaterializedAdjacencyLists::ClearVerticesIntoBitMap(udf_req, vids, bitmap_vid_range.GetBegin());
    timer.stop_timer(5);
	
    timer.stop_timer(0);

#ifdef REPORT_PROFILING_TIMERS
    fprintf(stdout, "[%lld] %lld [MaterializeAndEnqueue] %lld->%lld // %.3f %.3f %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, PartitionStatistics::my_machine_id(), to, timer.get_timer(0), timer.get_timer(1), timer.get_timer(2), timer.get_timer(3), timer.get_timer(4), timer.get_timer(4));
#endif

    req_buffer_queue->push(udf_req);
    RequestRespond::ReturnTempDataBuffer(CurPid.get_container());
}

void VidToAdjacencyListMappingTable::Get(int lv, node_t vid, node_t*& data_ptr, int64_t& data_sz) {
	AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(vid, lv);
	MaterializedAdjacencyLists* adj_ptr = ptr.ptr;
	data_ptr = adj_ptr->GetBeginPtr(ptr.slot_id);
	data_sz = adj_ptr->GetNumEntries(ptr.slot_id);
}

void MaterializedAdjacencyLists::CopyMaterializedAdjListsFromCacheAndEnqueue(tbb::concurrent_queue<UdfSendRequest>* req_buffer_queue, BitMap<node_t>* vids, Range<node_t> vid_range, std::vector<node_t> vertices, int64_t size_to_request, int64_t adjlist_data_size, int64_t slot_data_size, Range<int64_t> dst_edge_partitions, int64_t to, int64_t lv, int64_t* stop_flag, bool is_full_list, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
    LOG_ASSERT(false);  // by SYKO at 2019/10/07: The function seems to be no longer used
}


ReturnStatus MaterializedAdjacencyLists::MaterializeAndMergePartialList(BitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
    LOG_ASSERT(false); // cancer is removed by syko, 2019/10/11
}

ReturnStatus MaterializedAdjacencyLists::MaterializeFullListAsCurrentAndDeltaForFullListDB(BitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid) {
    LOG_ASSERT(false); // codes are deleted by syko, 2019/09/02
    INVARIANT(false);
}

ReturnStatus MaterializedAdjacencyLists::MaterializeFullListAsCurrentAndDelta(TwoLevelBitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, bool use_fulllist_db) {
    //fprintf(stdout, "[%ld](%ld) MaterializeFullListAsCurrentAndDelta vid_range[%ld, %ld], dst_edge_partitions[%ld, %ld], lv %d, version_range[%d, %d], e_type %d, d_type %d\n", PartitionStatistics::my_machine_id(), (int64_t) pthread_self(), vid_range.GetBegin(), vid_range.GetEnd(), dst_edge_partitions.GetBegin(), dst_edge_partitions.GetEnd(), lv, version_range.GetBegin(), version_range.GetEnd(), (int)e_type, (int)d_type);

    ALWAYS_ASSERT(lv >= 0);
    ALWAYS_ASSERT(version_range.GetBegin() == 0);
    ALWAYS_ASSERT(dst_edge_partitions.length() == PartitionStatistics::my_total_num_subchunks());
    ALWAYS_ASSERT(e_type == OUTEDGE || e_type == INEDGE);

    turbo_timer timer;
    timer.start_timer(0);
    
    // Variables
    current_slot_idx_ = -1;
    int version_begin = version_range.GetBegin(), version_end = version_range.GetEnd();
    EdgeType db_e_type = e_type;
    if (use_fulllist_db) e_type = (e_type == OUTEDGE) ? OUTEDGEFULLLIST : INEDGEFULLLIST;

    timer.start_timer(1);
    INVARIANT(vid_range.length() < std::numeric_limits<int32_t>::max());
    int64_t size_to_alloc = FixedSizeVector<FixedSizeVector<int32_t>>::recursive_size_in_bytes(2, vid_range.length());
    INVARIANT(size_to_alloc < 4 * 1024 * 1024L);
    SimpleContainer container_offset_to_fill_from = RequestRespond::GetTempDataBuffer(size_to_alloc);
    SimpleContainer container_delta_offset_to_fill_from = RequestRespond::GetTempDataBuffer(size_to_alloc);
    SimpleContainer container_degrees = RequestRespond::GetTempDataBuffer(size_to_alloc);
    SimpleContainer container_delta_degrees = RequestRespond::GetTempDataBuffer(size_to_alloc);
    
    FixedSizeVector<FixedSizeVector<int32_t>> offset_to_fill_from(container_offset_to_fill_from);
    FixedSizeVector<FixedSizeVector<int32_t>> delta_offset_to_fill_from(container_delta_offset_to_fill_from);
    FixedSizeVector<FixedSizeVector<int32_t>> degrees(container_degrees);
    FixedSizeVector<FixedSizeVector<int32_t>> delta_degrees(container_delta_degrees);
   
    offset_to_fill_from.recursive_resize(container_offset_to_fill_from.data, 2, vid_range.length());
    delta_offset_to_fill_from.recursive_resize(container_delta_offset_to_fill_from.data, 2, vid_range.length());
    degrees.recursive_resize(container_degrees.data, 2, vid_range.length());
    delta_degrees.recursive_resize(container_delta_degrees.data, 2, vid_range.length());

    for (int i = 0; i < 2; i++) {
        offset_to_fill_from[i].set(0);
        delta_offset_to_fill_from[i].set(0);
        degrees[i].set(0);
        delta_degrees[i].set(0);
    }
    
    PageID pin_counts = 0;
    PageID unpin_counts = 0;
    int64_t page_io_cnts = 0, page_pin_wo_io_cnts = 0;
    std::queue<PageID> pending_pages;
    VECTOR<std::queue<PageID>> pages_read(2);
    VECTOR<std::queue<PageID>> delta_pages_read(2);
    VECTOR<node_t> total_dummy_src_vertices_cnts(2, 0);
    VECTOR<node_t> total_src_vertices_cnt(2, 0);
    VECTOR<node_t> total_edges_cnt(2, 0);
    int64_t deleted_src_vertex = 0;
    node_t first_vid_of_bitmap = PartitionStatistics::my_first_node_id();   // XXX - assumed
    
    timer.stop_timer(1);


    timer.start_timer(2);
    ComputeSizeByDegreeArraysAndFilterByDegree(total_dummy_src_vertices_cnts, total_src_vertices_cnt, total_edges_cnt, vids, vid_range, version_range, first_vid_of_bitmap, degrees, delta_degrees, version_begin, version_end, dst_edge_partitions, d_type);
    timer.stop_timer(2);

    if (total_src_vertices_cnt[0] == 0) {
        vpage_.Initialize(0, 0, nullptr);
        return ReturnStatus::FAIL;
    }

    // Allocate container
    VariableSizedPage<Slot32>* vpage = &vpage_;
    SimpleContainer cont;
    
    int64_t adjlist_data_size = sizeof(node_t) * (total_edges_cnt[0] + total_edges_cnt[1]);
    int64_t slot_data_size = sizeof(Slot32) * (total_src_vertices_cnt[0]);
    int64_t page_metadata_size = sizeof(node_t);
    int64_t size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;
    //if (UserArguments::UPDATE_VERSION >= 1 && PartitionStatistics::my_machine_id() == 0)
    //    fprintf(stdout, "[%ld](%ld) MaterializeFullListAsCurrentAndDelta size_to_request = %ld (adj_list %ld slot %ld meta %ld), vid_range[%ld, %ld], dst_edge_partitions[%ld, %ld], lv %d, version_range[%d, %d], e_type %d, d_type %d\n", PartitionStatistics::my_machine_id(), (int64_t) pthread_self(), size_to_request, adjlist_data_size, slot_data_size, page_metadata_size, vid_range.GetBegin(), vid_range.GetEnd(), dst_edge_partitions.GetBegin(), dst_edge_partitions.GetEnd(), lv, version_range.GetBegin(), version_range.GetEnd(), (int)e_type, (int)d_type);

    // - Allocate a buffer
    ALWAYS_ASSERT(!UsePermanentMemory);
    cont = RequestRespond::GetDataSendBuffer(size_to_request);
    char*	tmp_container = cont.data;
    int64_t tmp_container_capacity = cont.capacity;
    vpage->Initialize(size_to_request, tmp_container_capacity, tmp_container);

    RequestRespond::InitializeIoInterface();
    diskaio::DiskAioInterface** my_io = RequestRespond::GetMyDiskIoInterface();
    
    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    timer.start_timer(3);
    FormatContainer(total_dummy_src_vertices_cnts, total_src_vertices_cnt, total_edges_cnt, vpage, vids, vid_range, degrees, delta_degrees, offset_to_fill_from, delta_offset_to_fill_from, first_vid_of_bitmap, size_to_request, adjlist_data_size);
    timer.stop_timer(3);

    // XXX - Optimize
    // How to parallelize it?
    for (int dbtype = 0; dbtype < 2; dbtype++) {
        if (d_type == INSERT && dbtype == 1) continue;
        
        if (dbtype == 0 && version_range.GetBegin() == 0) version_begin = 0;
        else if (dbtype == 1 && version_range.GetBegin() == 0) version_begin = 1;
        
        timer.start_timer(4);
        for (int version_id = version_begin; version_id <= version_end; version_id++) {
            if (dbtype == 1 && (version_id != version_end)) {
                continue;
            }
            Range<int64_t> dst_edge_partitions_to_process = dst_edge_partitions;
            if (use_fulllist_db) dst_edge_partitions_to_process.Set(0, 0);
            
            for (PartID i = dst_edge_partitions_to_process.GetBegin(); i <= dst_edge_partitions_to_process.GetEnd(); i++) {
                VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(db_e_type)->GetVidRangePerPage(version_id, (DynamicDBType)dbtype, use_fulllist_db);
                PageID last_pid = TurboDB::GetTurboDB(db_e_type)->GetLastPageId(i, version_id, (DynamicDBType)dbtype, use_fulllist_db);
                
                PageID pending_page_counts = 0;
                
                ALWAYS_ASSERT(dbtype < CurPid.size());
                ALWAYS_ASSERT(version_id < CurPid[dbtype].size());
                ALWAYS_ASSERT(i < CurPid[dbtype][version_id].size());
                
                while (CurPid[dbtype][version_id][i] != -1 && CurPid[dbtype][version_id][i] <= last_pid && vid_range.Overlapped(vidrangeperpage.Get(CurPid[dbtype][version_id][i]))) {
                    Range<node_t> cur_page_vid_range = vidrangeperpage.Get(CurPid[dbtype][version_id][i]);
                    PageID pid = CurPid[dbtype][version_id][i]++;

                    timer.start_timer(6);
                    if (false) {
                    //if (cur_page_vid_range.length() > 4 * 4096) {
                        if (!vids->IsSetInRange(cur_page_vid_range.GetBegin() - first_vid_of_bitmap, cur_page_vid_range.GetEnd() - first_vid_of_bitmap)) {
                            timer.stop_timer(6);
                            continue;
                        }
                    } else {
                        if (!vids->BitMap<node_t>::IsSetInRange(cur_page_vid_range.GetBegin() - first_vid_of_bitmap, cur_page_vid_range.GetEnd() - first_vid_of_bitmap)) {
                            timer.stop_timer(6);
                            continue;
                        }
                    }
                    timer.stop_timer(6);

                    timer.start_timer(8);
                    PageID table_page_id = TurboDB::GetTurboDB(db_e_type)->ConvertToDirectTablePageID(version_id, pid, (EdgeType) e_type, (DynamicDBType)dbtype);
                    AioRequest req;
                    req.user_info.db_info.page_id = pid;
                    req.user_info.db_info.version_id = version_id;
                    req.user_info.db_info.e_type = (int8_t)e_type;
                    req.user_info.db_info.d_type = (int8_t)dbtype;
                    req.user_info.do_user_cb = false;
				    req.user_info.func = (void*) &InvokeUserCallback;
                    req.buf = nullptr;
                    ReturnStatus rs;
                    if (version_id == version_end) {
                        rs = TurboDB::GetBufMgr()->PinPageCallbackLikelyCached(PartitionStatistics::my_machine_id(), req, my_io);
                    } else {
                        rs = TurboDB::GetBufMgr()->PinPageCallback(PartitionStatistics::my_machine_id(), req, my_io);
                    }
                    timer.stop_timer(8);

                    if (rs == ReturnStatus::ON_GOING) page_io_cnts++;
                    else if (rs == ReturnStatus::DONE) page_pin_wo_io_cnts++;
                    pin_counts++;
                    pending_page_counts++;

                    if (dbtype == 0) {
                        if (version_id != version_begin && version_id == version_end) {
                            delta_pages_read[dbtype].push(table_page_id);
                        } else {
                            pages_read[dbtype].push(table_page_id);
                        }
                    } else {
                        delta_pages_read[dbtype].push(table_page_id);
                    }
                    
                    if (pid == last_pid || pending_page_counts >= 1024) {
                        timer.start_timer(7);
                        RequestRespond::WaitMyPendingDiskIO(my_io);
                        timer.stop_timer(7);

                        timer.start_timer(5);
                        CopyAdjListsInPagesToContainer(pages_read, delta_pages_read, unpin_counts, e_type, dbtype, vids, vid_range, first_vid_of_bitmap, offset_to_fill_from, delta_offset_to_fill_from, tmp_container, adjlist_data_size, degrees);
                        timer.stop_timer(5);
                        pending_page_counts = 0;
                    }
                }
                
                if (pending_page_counts > 0) {
                    timer.start_timer(7);
                    RequestRespond::WaitMyPendingDiskIO(my_io);
                    timer.stop_timer(7);

                    timer.start_timer(5);
                    CopyAdjListsInPagesToContainer(pages_read, delta_pages_read, unpin_counts, e_type, dbtype, vids, vid_range, first_vid_of_bitmap, offset_to_fill_from, delta_offset_to_fill_from, tmp_container, adjlist_data_size, degrees);
                    timer.stop_timer(5);
                    pending_page_counts = 0;
                }
            }
        }
        timer.stop_timer(4);
        ALWAYS_ASSERT (pages_read[dbtype].empty());
    }
    INVARIANT (pin_counts == unpin_counts);
    ALWAYS_ASSERT (RequestRespond::WaitMyPendingDiskIO(my_io) == 0);
    
    timer.stop_timer(0);

    ALWAYS_ASSERT(SanityCheckUsingDegree(false));
    ALWAYS_ASSERT(SanityCheckUniqueness());

#ifdef REPORT_PROFILING_TIMERS
    /*
    fprintf(stdout, "[%ld] %ld [MatFullAsCurAndDel] [%ld, %ld] npages = %d (w/ i/o = %ld, w/o i/o = %ld), used page portion = %.2f, total [sec] = %.3f // (1) %.1f (2) %.1f (3) %.1f (4-5) %.1f (5) %.1f (6) %.1f (7) %.1f (8) %.1f # Avrg.Deg = %.1f Total Size = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, vid_range.GetBegin(), vid_range.GetEnd(), pin_counts, page_io_cnts, page_pin_wo_io_cnts, (double) 100 * ((double) size_to_request / ((double)TBGPP_PAGE_SIZE * pin_counts)), 
            timer.get_timer(0), (double) 100 * timer.get_timer(1)/timer.get_timer(0), (double) 100 * timer.get_timer(2)/timer.get_timer(0), (double) 100 * timer.get_timer(3)/timer.get_timer(0), (double) 100 * (timer.get_timer(4) - timer.get_timer(5))/timer.get_timer(0), (double) 100 * timer.get_timer(5)/timer.get_timer(0), (double) 100 * timer.get_timer(6)/timer.get_timer(0), (double) 100 * timer.get_timer(7)/timer.get_timer(0), (double) 100 * timer.get_timer(8)/timer.get_timer(0), 
            (double) (total_edges_cnt[0] + total_edges_cnt[1]) / total_src_vertices_cnt[0], size_to_request);
    */
#endif

    RequestRespond::ReturnTempDataBuffer(container_offset_to_fill_from);
    RequestRespond::ReturnTempDataBuffer(container_delta_offset_to_fill_from);
    RequestRespond::ReturnTempDataBuffer(container_degrees);
    RequestRespond::ReturnTempDataBuffer(container_delta_degrees);

    return ReturnStatus::OK;
}

void MaterializedAdjacencyLists::FormatContainer(std::vector<node_t>& total_dummy_src_vertices_cnts, std::vector<node_t>& total_src_vertices_cnt, std::vector<node_t>& total_edges_cnt, VariableSizedPage<Slot32>* vpage, BitMap<node_t>* vids, Range<node_t> vid_range, int dbtype, std::vector<std::vector<int64_t>>& degrees, std::vector<std::vector<int64_t>>& offset_to_fill_from, node_t first_vid_of_bitmap, int64_t size_to_request, int64_t adjlist_data_size) {
    //fprintf(stdout, "FormatContainer [%ld, %ld]\n", vid_range.GetBegin(), vid_range.GetEnd());
    int64_t current_slot_idx = 0;
    int64_t current_data_offset = 0;
    vpage->NumEntry() = total_src_vertices_cnt[dbtype];
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        if (!vids->Get(vid - first_vid_of_bitmap)) continue;
        if (degrees[dbtype][vid - vid_range.GetBegin()] == 0) continue;
        offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] = current_data_offset;

        vpage->GetSlot(current_slot_idx)->src_vid = vid;
        vpage->GetSlot(current_slot_idx)->end_offset = (current_data_offset + degrees[dbtype][vid - vid_range.GetBegin()]);

        ALWAYS_ASSERT (vpage->GetSlot(current_slot_idx)->end_offset >= 0 && sizeof(node_t) * vpage->GetSlot(current_slot_idx)->end_offset <= size_to_request);
        ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] <= adjlist_data_size);

        current_slot_idx++;
        current_data_offset += degrees[dbtype][vid - vid_range.GetBegin()];

        ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] + sizeof(node_t) * degrees[dbtype][vid - vid_range.GetBegin()] <= adjlist_data_size);
    }
}

// used in MaterializeFullListAsCurrentAndDelta
void MaterializedAdjacencyLists::FormatContainer(VECTOR<node_t>& total_dummy_src_vertices_cnts, VECTOR<node_t>& total_src_vertices_cnt, VECTOR<node_t>& total_edges_cnt, VariableSizedPage<Slot32>* vpage, BitMap<node_t>* vids, Range<node_t> vid_range, FixedSizeVector<FixedSizeVector<int32_t>>& degrees, FixedSizeVector<FixedSizeVector<int32_t>>& delta_degrees, FixedSizeVector<FixedSizeVector<int32_t>>& offset_to_fill_from, FixedSizeVector<FixedSizeVector<int32_t>>& delta_offset_to_fill_from, node_t first_vid_of_bitmap, int64_t size_to_request, int64_t adjlist_data_size) {
    //fprintf(stdout, "FormatContainer [%ld, %ld]\n", vid_range.GetBegin(), vid_range.GetEnd());
    
    ALWAYS_ASSERT(vpage == &vpage_);

    int64_t current_slot_idx = 0;
    int64_t current_data_offset = 0;
    vpage->NumEntry() = total_src_vertices_cnt[0];
    int64_t cnts = 0;
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        if (!vids->Get(vid - first_vid_of_bitmap)) continue;
        node_t vid_idx = vid - vid_range.GetBegin();
        cnts++;
       
        vpage->GetSlot(current_slot_idx)->src_vid = vid;
        vpage->GetSlot(current_slot_idx)->end_offset = (current_data_offset + degrees[0][vid - vid_range.GetBegin()]);
        vpage->GetSlot(current_slot_idx)->delta_end_offset[INSERT] = vpage->GetSlot(current_slot_idx)->end_offset + delta_degrees[INSERT][vid - vid_range.GetBegin()];
        vpage->GetSlot(current_slot_idx)->delta_end_offset[DELETE] = vpage->GetSlot(current_slot_idx)->delta_end_offset[INSERT] + delta_degrees[DELETE][vid - vid_range.GetBegin()];
        
        
        offset_to_fill_from[0][vid - vid_range.GetBegin()] = current_data_offset;
        delta_offset_to_fill_from[INSERT][vid - vid_range.GetBegin()] = vpage->GetSlot(current_slot_idx)->end_offset;
        delta_offset_to_fill_from[DELETE][vid - vid_range.GetBegin()] = vpage->GetSlot(current_slot_idx)->delta_end_offset[INSERT];
        
        current_data_offset = vpage->GetSlot(current_slot_idx)->delta_end_offset[DELETE];
        
        ALWAYS_ASSERT (current_data_offset <= size_to_request);
        ALWAYS_ASSERT (degrees[0][vid - vid_range.GetBegin()] + delta_degrees[INSERT][vid - vid_range.GetBegin()] + delta_degrees[DELETE][vid - vid_range.GetBegin()] == vpage->GetNumEntries(current_slot_idx));
        ALWAYS_ASSERT (delta_degrees[INSERT][vid - vid_range.GetBegin()] == TurboDB::OutInsertDegree(vid));
        ALWAYS_ASSERT (delta_degrees[DELETE][vid - vid_range.GetBegin()] == TurboDB::OutDeleteDegree(vid));
        
        current_slot_idx++;
    }

    ALWAYS_ASSERT(vpage->NumEntry() == cnts);
#ifndef PERFORMANCE
    ALWAYS_ASSERT(vpage->NumEntry() == vpage_.NumEntry());
    for(int64_t current_slot_idx = 0 ; current_slot_idx < cnts ; current_slot_idx++) {
        ALWAYS_ASSERT (vid_range.contains(vpage->GetSlot(current_slot_idx)->src_vid));
    }
#endif
    ALWAYS_ASSERT(SanityCheckUsingDegree(false));
}


// Materialize (Without Deltas)
void MaterializedAdjacencyLists::MaterializeFullList(TwoLevelBitMap<node_t>* vids, Range<node_t> vid_range, Range<int64_t> dst_edge_partitions, bool UsePermanentMemory, int lv, Range<int> version_range, EdgeType e_type, DynamicDBType d_type, bool use_fulllist_db) {
    //fprintf(stdout, "[%ld] MaterializeFullList vid_range[%ld, %ld], dst_edge_partitions[%ld, %ld], lv %d, version_range[%d, %d], e_type %d, d_type %d\n", PartitionStatistics::my_machine_id(), vid_range.GetBegin(), vid_range.GetEnd(), dst_edge_partitions.GetBegin(), dst_edge_partitions.GetEnd(), lv, version_range.GetBegin(), version_range.GetEnd(), (int)e_type, (int)d_type);
    ALWAYS_ASSERT (version_range.GetBegin() == 0);
    ALWAYS_ASSERT (d_type == INSERT);

    turbo_timer timer;
    timer.start_timer(0);
    
    RequestRespond::InitializeIoInterface();
    diskaio::DiskAioInterface** my_io = RequestRespond::GetMyDiskIoInterface();
    
    EdgeType db_e_type = e_type;
    Range<int64_t> dst_edge_partitions_to_process = dst_edge_partitions;
    if (use_fulllist_db) e_type = (e_type == OUTEDGE) ? OUTEDGEFULLLIST : INEDGEFULLLIST;
    if (use_fulllist_db) dst_edge_partitions_to_process.Set(0, 0);
    
    int64_t alloc_size_for_CurPid = FixedSizeVector<FixedSizeVector<PageID>>::recursive_size_in_bytes(version_range.length(), PartitionStatistics::my_total_num_subchunks());
    INVARIANT(alloc_size_for_CurPid < 4 * 1024 * 1024L);
    SimpleContainer container_CurPid = RequestRespond::GetTempDataBuffer(alloc_size_for_CurPid);
    FixedSizeVector<FixedSizeVector<PageID>> CurPid(container_CurPid);
    CurPid.recursive_resize(container_CurPid.data, version_range.length(), PartitionStatistics::my_total_num_subchunks());
    for (int64_t i = version_range.GetBegin(); i <= version_range.GetEnd(); i++) {
        CurPid[i].set(-1);
    }

    INVARIANT(vid_range.length() * sizeof(int32_t) < 4 * 1024 * 1024L);
    SimpleContainer container_offset_to_fill_from = RequestRespond::GetTempDataBuffer(vid_range.length() * sizeof(int32_t));
    FixedSizeVector<int32_t> offset_to_fill_from(container_offset_to_fill_from); 
    offset_to_fill_from.recursive_resize(container_offset_to_fill_from.data, vid_range.length());
    offset_to_fill_from.set(0);
   
    node_t first_vid_of_bitmap = PartitionStatistics::my_first_node_id();
    Page* page_buffer = nullptr;
    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;
    current_slot_idx_ = -1;

    // 1. Count Degrees
    node_t total_dummy_src_vertices_cnts = 0;
    node_t total_src_vertices_cnt = 0;
    node_t total_edges_cnt = 0;
    timer.start_timer(1);
    FilterByDegreeAndComputeSizeUsingDegreeArray(total_dummy_src_vertices_cnts, total_src_vertices_cnt, total_edges_cnt, vids, vid_range, version_range, first_vid_of_bitmap);
    timer.stop_timer(1);

    // Finish, if empty
    if (total_edges_cnt == 0 || total_src_vertices_cnt == 0) {
        vpage_.Initialize(0, 0, nullptr);
        return;
    }

    // 2. Request a container and init
    timer.start_timer(2);
    int64_t adjlist_data_size = sizeof(node_t) * (total_edges_cnt);
    int64_t slot_data_size = sizeof(Slot32) * (total_src_vertices_cnt);
    int64_t page_metadata_size = sizeof(node_t);
    int64_t size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;

    SimpleContainer cont;
    if (UsePermanentMemory) {
        cont.data = VidToAdjacencyListMappingTable::VidToAdjTable.GetBlockMemoryStackAllocator(lv).Allocate(size_to_request);
        cont.capacity = size_to_request;
    } else {
        cont = RequestRespond::GetDataSendBuffer(size_to_request);
    }
    char*	tmp_container = cont.data;
    int64_t tmp_container_capacity = cont.capacity;

    ALWAYS_ASSERT (tmp_container != nullptr);
    ALWAYS_ASSERT (tmp_container_capacity > 0 && tmp_container_capacity >= size_to_request);
    vpage_.Initialize(size_to_request, tmp_container_capacity, tmp_container);
    timer.stop_timer(2);

    timer.start_timer(3);
    // 3. Formatting Container
    int64_t current_slot_idx = 0;
    int64_t current_data_offset = 0;
    vpage_.NumEntry() = total_src_vertices_cnt;
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        if (!vids->BitMap<node_t>::Get(vid - first_vid_of_bitmap)) continue;
        ALWAYS_ASSERT (TurboDB::OutDegree(vid) > 0);
#if USE_DEGREE_THRESHOLD
        ALWAYS_ASSERT (TurboDB::OutDegree(vid) > UserArguments::DEGREE_THRESHOLD);
#endif
        offset_to_fill_from[vid - vid_range.GetBegin()] = current_data_offset;

        vpage_.GetSlot(current_slot_idx)->src_vid = vid;
        vpage_.GetSlot(current_slot_idx)->end_offset = (current_data_offset + TurboDB::OutDegree(vid));
        vpage_.GetSlot(current_slot_idx)->delta_end_offset[0] = vpage_.GetSlot(current_slot_idx)->end_offset;
        vpage_.GetSlot(current_slot_idx)->delta_end_offset[1] = vpage_.GetSlot(current_slot_idx)->end_offset;

        ALWAYS_ASSERT (vpage_.GetSlot(current_slot_idx)->end_offset >= 0 && sizeof(node_t) * vpage_.GetSlot(current_slot_idx)->end_offset <= size_to_request);
        ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[vid - vid_range.GetBegin()] + sizeof(node_t) * TurboDB::OutDegree(vid) <= adjlist_data_size);

        current_slot_idx++;
        current_data_offset += TurboDB::OutDegree(vid);
    }
    timer.stop_timer(3);

    timer.start_timer(4);
    FindPageIdsToReadFrom(vids, vid_range, version_range, CurPid, dst_edge_partitions_to_process, db_e_type, e_type, d_type);
    timer.stop_timer(4);

    int64_t page_io_cnts = 0, page_pin_wo_io_cnts = 0;
    PageID pin_counts = 0;
    PageID unpin_counts = 0;
    PageID pending_page_counts = 0;
    std::queue<PageID> pending_pages;

    timer.start_timer(5);
    for (int version_id = version_range.GetBegin(); version_id <= version_range.GetEnd(); version_id++) {
        for (PartID i = dst_edge_partitions_to_process.GetBegin(); i <= dst_edge_partitions.GetEnd(); i++) {
            VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(db_e_type)->GetVidRangePerPage(version_id, d_type, use_fulllist_db);
            while (CurPid[version_id][i] != -1 && CurPid[version_id][i] <= TurboDB::GetTurboDB(db_e_type)->GetLastPageId(i, version_id, d_type, use_fulllist_db) && vid_range.Overlapped(vidrangeperpage.Get(CurPid[version_id][i]))) {
                Range<node_t> cur_page_vid_range = vidrangeperpage.Get(CurPid[version_id][i]);
                PageID pid = CurPid[version_id][i]++;

                if (false) {
                //if (cur_page_vid_range.length() > 4 * 4096) {
                    if (!vids->IsSetInRange(cur_page_vid_range.GetBegin() - first_vid_of_bitmap, cur_page_vid_range.GetEnd() - first_vid_of_bitmap)) {
                        continue;
                    }
                } else {
                    if (!vids->BitMap<node_t>::IsSetInRange(cur_page_vid_range.GetBegin() - first_vid_of_bitmap, cur_page_vid_range.GetEnd() - first_vid_of_bitmap)) {
                        continue;
                    }
                }


                PageID table_page_id = TurboDB::GetTurboDB(db_e_type)->ConvertToDirectTablePageID(version_id, pid, e_type, d_type);

                AioRequest req;
                req.user_info.db_info.page_id = pid;
                req.user_info.db_info.version_id = version_id;
                req.user_info.do_user_cb = false;
				req.user_info.func = (void*) &InvokeUserCallback;
                req.user_info.db_info.e_type = (int8_t)e_type;
                req.user_info.db_info.d_type = (int8_t)d_type;
                req.buf = nullptr;

                ReturnStatus rs;
                if (version_id == version_range.GetEnd()) {
                    rs = TurboDB::GetBufMgr()->PinPageCallbackLikelyCached(PartitionStatistics::my_machine_id(), req, my_io);
                } else {
                    rs = TurboDB::GetBufMgr()->PinPageCallback(PartitionStatistics::my_machine_id(), req, my_io);
                }
                ALWAYS_ASSERT(req.buf != NULL);
                ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(table_page_id));
                if (rs == ReturnStatus::ON_GOING) page_io_cnts++;
                else if (rs == ReturnStatus::DONE) page_pin_wo_io_cnts++;

                pin_counts++;
                pending_page_counts++;
                pending_pages.push(table_page_id);

                ALWAYS_ASSERT(pending_pages.size() == pending_page_counts);
                if (pid == TurboDB::GetTurboDB(db_e_type)->GetLastPageId(i, version_id, d_type, use_fulllist_db) || pending_page_counts >= 1024) {
                    timer.start_timer(7);
                    int64_t num_pages_processed = RequestRespond::WaitMyPendingDiskIO();
                    INVARIANT (RequestRespond::WaitMyPendingDiskIO() == 0);
                    timer.stop_timer(7);
                    CopyAdjListsInPagesToContainer(pending_pages, pending_page_counts, unpin_counts, e_type, d_type, vids, vid_range, first_vid_of_bitmap, offset_to_fill_from, tmp_container, adjlist_data_size);
                }
            }
        }
    }
    timer.stop_timer(5);

    timer.start_timer(6);
    if (pending_page_counts > 0) {
        timer.start_timer(7);
        int64_t num_pages_processed = RequestRespond::WaitMyPendingDiskIO();
        INVARIANT (RequestRespond::WaitMyPendingDiskIO() == 0);
        timer.stop_timer(7);
        CopyAdjListsInPagesToContainer(pending_pages, pending_page_counts, unpin_counts, e_type, d_type, vids, vid_range, first_vid_of_bitmap, offset_to_fill_from, tmp_container, adjlist_data_size);
    }
    INVARIANT (RequestRespond::WaitMyPendingDiskIO() == 0);
    INVARIANT (pending_page_counts == 0);
    INVARIANT (pin_counts == unpin_counts);

    timer.stop_timer(6);

    INVARIANT (total_src_vertices_cnt == current_slot_idx);
    INVARIANT (current_data_offset == total_edges_cnt);
    INVARIANT (GetNumAdjacencyLists() == total_src_vertices_cnt);
    INVARIANT (GetNumEdges() == total_edges_cnt);

    RequestRespond::ReturnTempDataBuffer(container_CurPid);
    RequestRespond::ReturnTempDataBuffer(container_offset_to_fill_from);
    timer.stop_timer(0);

 #ifdef REPORT_PROFILING_TIMERS
    /*
    fprintf(stdout, "[%ld] %ld [MatFull] [%ld, %ld] npages = %d (w/ i/o = %ld, w/o i/o = %ld), used page portion = %.2f, total [sec] = %.3f // (1) %.1f (2) %.1f (3) %.1f (4) %.1f (5) %.1f (6) %.1f (7) %.1f Total Size = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, vid_range.GetBegin(), vid_range.GetEnd(), pin_counts, page_io_cnts, page_pin_wo_io_cnts, (double) 100 * ((double) size_to_request / ((double)TBGPP_PAGE_SIZE * pin_counts)), 
            timer.get_timer(0), (double) 100 * timer.get_timer(1)/timer.get_timer(0), (double) 100 * timer.get_timer(2)/timer.get_timer(0), (double) 100 * timer.get_timer(3)/timer.get_timer(0), (double) 100 * timer.get_timer(4)/timer.get_timer(0), (double) 100 * timer.get_timer(5)/timer.get_timer(0), (double) 100 * timer.get_timer(6)/timer.get_timer(0), (double) 100 * timer.get_timer(7)/timer.get_timer(0), size_to_request);
    */
#endif

   
}


// called by 'MaterializeFullListAsCurrentAndDelta' (With Deltas)
void MaterializedAdjacencyLists::ComputeSizeByDegreeArraysAndFilterByDegree(VECTOR<node_t>& total_dummy_src_vertices_cnts, VECTOR<node_t>& total_src_vertices_cnt, VECTOR<node_t>& total_edges_cnt, BitMap<node_t>* vids, Range<node_t>& vid_range, Range<int>& version_range, node_t first_vid_of_bitmap, FixedSizeVector<FixedSizeVector<int32_t>>& degrees, FixedSizeVector<FixedSizeVector<int32_t>>& delta_degrees, int version_begin, int version_end, Range<int64_t> dst_edge_partitions, DynamicDBType db_type) {
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        node_t lid = LocalStatistics::Vid2Lid(vid);
        if (!vids->Get(vid - first_vid_of_bitmap)) continue;    // need 'InvokeIfMarked'

        degrees[0][vid - vid_range.GetBegin()] = TurboDB::OutDegree(vid) - TurboDB::OutInsertDegree(vid) + TurboDB::OutDeleteDegree(vid);   // previous
        degrees[1][vid - vid_range.GetBegin()] = TurboDB::OutDegree(vid);   // current
    
        if (db_type == INSERT || db_type == ALL)
            delta_degrees[INSERT][vid - vid_range.GetBegin()] = TurboDB::OutInsertDegree(vid);
        if (db_type == DELETE || db_type == ALL)
            delta_degrees[DELETE][vid - vid_range.GetBegin()] = TurboDB::OutDeleteDegree(vid);

        node_t prev_degree = degrees[0][vid - vid_range.GetBegin()];
        node_t cur_degree = degrees[1][vid - vid_range.GetBegin()];

        //fprintf(stdout, "[ComputeSizeByDegreeArraysAndFilterByDegree] %ld, %ld %ld %ld %ld\n", vid, degrees[0][vid - vid_range.GetBegin()], degrees[1][vid - vid_range.GetBegin()], delta_degrees[INSERT][vid - vid_range.GetBegin()], delta_degrees[DELETE][vid - vid_range.GetBegin()]);

        // TODO need to check the below code block furher (for correctness)
#if USE_DEGREE_THRESHOLD
        //if (cur_degree > UserArguments::DEGREE_THRESHOLD || prev_degree > UserArguments::DEGREE_THRESHOLD) {
        if ((TurboDB::OutDegree(vid) + TurboDB::OutDeleteDegree(vid)) > UserArguments::DEGREE_THRESHOLD) {
#else
        if (cur_degree > 0 || prev_degree > 0) {
#endif
            total_src_vertices_cnt[0]++;
            total_edges_cnt[0] += prev_degree;
            total_edges_cnt[1] += delta_degrees[INSERT][vid - vid_range.GetBegin()] + delta_degrees[DELETE][vid - vid_range.GetBegin()];
        } else {
            vids->Clear_Atomic(lid);
        }

    }

}

// called by 'MaterializeFullList' (Without Deltas)
void MaterializedAdjacencyLists::FilterByDegreeAndComputeSizeUsingDegreeArray(node_t& total_dummy_src_vertices_cnts, node_t& total_src_vertices_cnt, node_t& total_edges_cnt, BitMap<node_t>* vids, Range<node_t>& vid_range, Range<int>& version_range, node_t first_vid_of_bitmap) {
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        node_t lid = LocalStatistics::Vid2Lid(vid);
        if (!vids->Get(vid - first_vid_of_bitmap)) continue;

#if USE_DEGREE_THRESHOLD
        if (TurboDB::OutDegree(vid) > UserArguments::DEGREE_THRESHOLD) {
            total_src_vertices_cnt++;
            total_edges_cnt += TurboDB::OutDegree(vid);
        } else {
            total_dummy_src_vertices_cnts++;
            vids->Clear_Atomic(lid);
        }
#else
        if (TurboDB::OutDegree(vid) > 0) {
            total_src_vertices_cnt++;
            total_edges_cnt += TurboDB::OutDegree(vid);
        } else {
            total_dummy_src_vertices_cnts++;
            vids->Clear_Atomic(lid);
        }
#endif
    }
}

void MaterializedAdjacencyLists::FindPageIdsToReadFrom(BitMap<node_t>* vids, Range<node_t>& vid_range, Range<int>& version_range, FixedSizeVector<FixedSizeVector<PageID>>& CurPid, Range<int64_t> dst_edge_partitions, EdgeType db_e_type, EdgeType e_type, DynamicDBType dbtype) {
    bool all_found = true;
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        node_t lvid = LocalStatistics::Vid2Lid(vid);
        if (!vids->Get(lvid)) continue;

        all_found = true;
        for (int version_id = version_range.GetBegin(); version_id <= version_range.GetEnd(); version_id++) {
            for (PartID i = dst_edge_partitions.GetBegin(); i <= dst_edge_partitions.GetEnd(); i++) {
                if (CurPid[version_id][i] == -1) {
                    CurPid[version_id][i] = TurboDB::GetTurboDB(db_e_type)->GetPageRangeByVid(i, vid, version_id, e_type, dbtype).GetBegin();
                    if (CurPid[version_id][i] == -1) {
                        all_found = false;
                    }
                }
            }
        }
        if (all_found) break;
    }
}

// used in MaterializeFullListAsCurrentAndDelta
void MaterializedAdjacencyLists::CopyAdjListsInPagesToContainer(VECTOR<std::queue<PageID>>& pages_read, VECTOR<std::queue<PageID>>& delta_pages_read, PageID& unpin_counts, EdgeType& e_type, int& dbtype, BitMap<node_t>* vids, Range<node_t> vid_range, node_t first_vid_of_bitmap, FixedSizeVector<FixedSizeVector<int32_t>>& offset_to_fill_from, FixedSizeVector<FixedSizeVector<int32_t>>& delta_offset_to_fill_from, char*	tmp_container, int64_t& adjlist_data_size, FixedSizeVector<FixedSizeVector<int32_t>>& degrees) {
    ALWAYS_ASSERT(dbtype == INSERT || dbtype == DELETE);
    
    //fprintf(stdout, "[CopyAdjListsInPagesToContainer] %p [%ld, %ld], dbtype = %ld\n", this, GetSlot(0)->src_vid, GetSlot(GetNumAdjacencyLists() - 1)->src_vid, dbtype);
    
    Page* page_buffer = nullptr;
    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    while (!pages_read[dbtype].empty()) {

        PageID cur_pid = pages_read[dbtype].front();
        pages_read[dbtype].pop();
   
        if (UserArguments::UPDATE_VERSION >= 11) {
            //fprintf(stdout, "[CopyAdjListsInPagesToContainer::CUR] %p [%ld, %ld], pid = %ld\n", this, GetSlot(0)->src_vid, GetSlot(GetNumAdjacencyLists() - 1)->src_vid, cur_pid);
        }

        ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(cur_pid));
        TurboDB::GetBufMgr()->GetPagePtr_Unsafe(cur_pid, page_buffer);

        adjlist_iter.SetCurrentPage(cur_pid, page_buffer);
        while (adjlist_iter.GetNextAdjList(*vids, nbrlist_iter) == ReturnStatus::OK) {
            if (!vid_range.contains(nbrlist_iter.GetSrcVid())) continue;

            ALWAYS_ASSERT (nbrlist_iter.GetSrcVid() >= PartitionStatistics::my_first_node_id()
                    && nbrlist_iter.GetSrcVid() <= PartitionStatistics::my_last_node_id());
            ALWAYS_ASSERT (vids->Get(nbrlist_iter.GetSrcVid() - first_vid_of_bitmap));

            node_t* data = nbrlist_iter.GetData();
            int64_t sz = nbrlist_iter.GetNumEntries();
            node_t vid = nbrlist_iter.GetSrcVid();
            

            ALWAYS_ASSERT (vid_range.contains(vid));
            ALWAYS_ASSERT (sz > 0);

            char* copy_to = (char*) tmp_container + sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()];
            char* copy_from = (char*) data;

            node_t* vid_copy_to = (node_t*) copy_to;
            node_t* vid_copy_from = (node_t*) copy_from;

            int64_t deleted_cnts = 0;
            for (int64_t idx = 0; idx < sz; idx++) {
                node_t tmp_vid = PartitionStatistics::GetPrevVisibleDstVid(vid_copy_from[idx]);
                if (tmp_vid == -1) {
                    deleted_cnts++;
                    //fprintf(stdout, "\tDELETED! [CopyAdjListsInPagesToContainer::CUR] %p [%ld, %ld], pid = %ld, src = %ld dst = %ld at %p (%ld at %p)\n", this, GetSlot(0)->src_vid, GetSlot(GetNumAdjacencyLists() - 1)->src_vid, cur_pid, vid, tmp_vid, vid_copy_to, *(vid_copy_to+1), (vid_copy_to+1));
                    continue;
                }
                *vid_copy_to = tmp_vid;
               
                /*
                if (UserArguments::UPDATE_VERSION >= 11) {
                    fprintf(stdout, "[CopyAdjListsInPagesToContainer::CUR] %p [%ld, %ld], pid = %ld, src = %ld dst = %ld at %p (%ld at %p)\n", this, GetSlot(0)->src_vid, GetSlot(GetNumAdjacencyLists() - 1)->src_vid, cur_pid, vid, tmp_vid, vid_copy_to, *(vid_copy_to+1), (vid_copy_to+1));
                }
                */
                vid_copy_to++;
            }
            ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(cur_pid));
            
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] >= 0);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] <= adjlist_data_size);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] + sizeof(node_t) * (sz - deleted_cnts) <= adjlist_data_size);
            //fprintf(stdout, "[CopyAdjListsInPagesToContainer][%ld] vid = %ld, cur_pid = %d, sz = %ld, offset_to_fill_from = %ld, adjlist_data_size = %ld\n", PartitionStatistics::my_machine_id(), vid, cur_pid, sz - deleted_cnts, offset_to_fill_from[dbtype][vid - vid_range.GetBegin()], adjlist_data_size);
            
            offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] += sz - deleted_cnts;
        }
        unpin_counts++;
        TurboDB::GetBufMgr()->UnpinPage(PartitionStatistics::my_machine_id(), cur_pid);
    }

    while (!delta_pages_read[dbtype].empty()) {
        PageID cur_pid = delta_pages_read[dbtype].front();
        delta_pages_read[dbtype].pop();
        
        if (UserArguments::UPDATE_VERSION >= 11) {
            //fprintf(stdout, "[CopyAdjListsInPagesToContainer::DELTA] %p [%ld, %ld], pid = %ld\n", this, GetSlot(0)->src_vid, GetSlot(GetNumAdjacencyLists() - 1)->src_vid, cur_pid);
        }

        ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(cur_pid));
        TurboDB::GetBufMgr()->GetPagePtr_Unsafe(cur_pid, page_buffer);

        adjlist_iter.SetCurrentPage(cur_pid, page_buffer);
        while (adjlist_iter.GetNextAdjList(*vids, nbrlist_iter) == ReturnStatus::OK) {
            if (!vid_range.contains(nbrlist_iter.GetSrcVid())) continue;

            ALWAYS_ASSERT (nbrlist_iter.GetSrcVid() >= PartitionStatistics::my_first_node_id()
                    && nbrlist_iter.GetSrcVid() <= PartitionStatistics::my_last_node_id());
            ALWAYS_ASSERT (vids->Get(nbrlist_iter.GetSrcVid() - first_vid_of_bitmap));

            node_t* data = nbrlist_iter.GetData();
            int64_t sz = nbrlist_iter.GetNumEntries();
            node_t vid = nbrlist_iter.GetSrcVid();

            //fprintf(stdout, "[DELTA][%ld] vid = %ld, cur_pid = %d, sz = %ld, offset_to_fill_from = %ld\n", PartitionStatistics::my_machine_id(), vid, cur_pid, sz, offset_to_fill_from[dbtype][vid - vid_range.GetBegin()]);

            ALWAYS_ASSERT (vid_range.contains(vid));
            ALWAYS_ASSERT (sz > 0);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] >= 0);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] <= adjlist_data_size);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] + sizeof(node_t) * sz <= adjlist_data_size);

            char* copy_to = (char*) tmp_container + sizeof(node_t) * delta_offset_to_fill_from[dbtype][vid - vid_range.GetBegin()];
            char* copy_from = (char*) data;
            int64_t copy_size = sz * sizeof(node_t);

            ALWAYS_ASSERT (copy_to >= tmp_container && copy_to < tmp_container + adjlist_data_size);

            node_t* vid_copy_to = (node_t*) copy_to;
            node_t* vid_copy_from = (node_t*) copy_from;

            int64_t deleted_cnts = 0;
            for (int64_t idx = 0; idx < sz; idx++) {
                node_t tmp_vid = PartitionStatistics::GetPrevVisibleDstVid(vid_copy_from[idx]);
                if (tmp_vid == -1) {
                    deleted_cnts++;
                    continue;
                }
                *vid_copy_to = tmp_vid;
                if (UserArguments::UPDATE_VERSION >= 1) {
                    //fprintf(stdout, "[CopyAdjListsInPagesToContainer::DELTA] %p [%ld, %ld], pid = %ld, src_vid = %ld dst_vid = %ld at %p\n", this, GetSlot(0)->src_vid, GetSlot(GetNumAdjacencyLists() - 1)->src_vid, cur_pid, vid, tmp_vid, vid_copy_to);
                }
                vid_copy_to++;
            }
            delta_offset_to_fill_from[dbtype][vid - vid_range.GetBegin()] += sz - deleted_cnts;
            ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(cur_pid));
        }
        unpin_counts++;
        TurboDB::GetBufMgr()->UnpinPage(PartitionStatistics::my_machine_id(), cur_pid);
    }
    
    
    INVARIANT (pages_read[dbtype].empty());
    INVARIANT (delta_pages_read[dbtype].empty());
}

// used in MaterializeFullList
void MaterializedAdjacencyLists::CopyAdjListsInPagesToContainer(std::queue<PageID>& pending_pages, PageID& pending_page_counts, PageID& unpin_counts, EdgeType& e_type, DynamicDBType& d_type, BitMap<node_t>* vids, const Range<node_t> vid_range, const node_t first_vid_of_bitmap, FixedSizeVector<int32_t>& offset_to_fill_from, const char* tmp_container, int64_t& adjlist_data_size) {
    Page* page_buffer = nullptr;
    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    ALWAYS_ASSERT(d_type == INSERT);
    ALWAYS_ASSERT(RequestRespond::WaitMyPendingDiskIO() == 0);

    INVARIANT(offset_to_fill_from.get_container().capacity < 4 * 1024 * 1024L);
    SimpleContainer container_offset_to_fill_from = RequestRespond::GetTempDataBuffer(offset_to_fill_from.get_container().capacity);
    FixedSizeVector<int32_t> tmp_offset_to_fill_from(container_offset_to_fill_from); 
    tmp_offset_to_fill_from.copy_contents_from(offset_to_fill_from); 

    while (!pending_pages.empty()) {
        PageID cur_table_page_id = pending_pages.front();
        pending_pages.pop();
        pending_page_counts--;

        ALWAYS_ASSERT(TurboDB::GetTurboDB(e_type)->GetBufMgr()->IsPinnedPage(cur_table_page_id));
        TurboDB::GetTurboDB(e_type)->GetBufMgr()->GetPagePtr_Unsafe(cur_table_page_id, page_buffer);

        adjlist_iter.SetCurrentPage(cur_table_page_id, page_buffer);
        while (adjlist_iter.GetNextAdjList(*vids, nbrlist_iter) == ReturnStatus::OK) {
            if (!vid_range.contains(nbrlist_iter.GetSrcVid())) continue;
            ALWAYS_ASSERT (nbrlist_iter.GetSrcVid() >= PartitionStatistics::my_first_node_id() && nbrlist_iter.GetSrcVid() <= PartitionStatistics::my_last_node_id());
            ALWAYS_ASSERT (vids->Get(nbrlist_iter.GetSrcVid() - first_vid_of_bitmap));

            node_t* data = nbrlist_iter.GetData();
            int64_t sz = nbrlist_iter.GetNumEntries();
            node_t vid = nbrlist_iter.GetSrcVid();

            ALWAYS_ASSERT (vid_range.contains(vid));
            ALWAYS_ASSERT (sz > 0);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[vid - vid_range.GetBegin()] >= 0);
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[vid - vid_range.GetBegin()] <= adjlist_data_size);

            char* copy_to = (char*) tmp_container + sizeof(node_t) * offset_to_fill_from[vid - vid_range.GetBegin()];
            char* copy_from = (char*) data;
            ALWAYS_ASSERT (copy_to >= tmp_container && copy_to <= tmp_container + adjlist_data_size);

            node_t* vid_copy_to = (node_t*) copy_to;
            node_t* vid_copy_from = (node_t*) copy_from;
            int64_t deleted_sz = 0;
            for (int64_t idx = 0; idx < sz; idx++) {
                node_t tmp_vid = PartitionStatistics::GetVisibleDstVid(vid_copy_from[idx]);
                if (tmp_vid == -1) {
                    deleted_sz++;
                    continue;
                }
                *vid_copy_to = tmp_vid;
                vid_copy_to++;
            }
            ALWAYS_ASSERT(TurboDB::GetTurboDB(e_type)->GetBufMgr()->IsPinnedPage(cur_table_page_id));
            ALWAYS_ASSERT (sizeof(node_t) * offset_to_fill_from[vid - vid_range.GetBegin()] + sizeof(node_t) * (sz - deleted_sz) <= adjlist_data_size);
            ALWAYS_ASSERT (sz - deleted_sz <= TurboDB::OutDegree(vid));
            
            offset_to_fill_from[vid - vid_range.GetBegin()] += sz - deleted_sz;
        
            ALWAYS_ASSERT(offset_to_fill_from[vid - vid_range.GetBegin()] - tmp_offset_to_fill_from[vid - vid_range.GetBegin()] <= TurboDB::OutDegree(vid));
        }

        unpin_counts++;
        TurboDB::GetTurboDB(e_type)->GetBufMgr()->UnpinPage(PartitionStatistics::my_machine_id(), cur_table_page_id);
    }
    
    RequestRespond::ReturnTempDataBuffer(container_offset_to_fill_from);
}

void MaterializedAdjacencyLists::ConvertVidToDegreeOrderAndSortOfCachedMatAdj(BitMap<node_t>* fulllist_latest_flags, node_t offset_vid, bool current, bool delta) {
    ALWAYS_ASSERT (UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
    ALWAYS_ASSERT (Data() != NULL);
    ALWAYS_ASSERT (GetNumAdjacencyLists() > 0);
    ALWAYS_ASSERT (!IsEmpty());
    //ALWAYS_ASSERT (GetNumEdges() > 0);    // syko

    int64_t sz = Size() / sizeof(node_t);
    node_t* begin = (node_t*) Data();
    int64_t idx = 0;
    
    //fprintf(stdout, "[%ld] %ld [ConvertVidToDegreeOrderAndSortOfCachedMatAdj] %p [%ld, %ld]\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, this, GetSlotVid(0), GetSlotVid(GetNumAdjacencyLists() - 1));
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        node_t src_vid = GetSlotVid(adj_idx);
        node_t src_vid_idx = src_vid - offset_vid;

        if (fulllist_latest_flags->Get(src_vid_idx)) continue;

        if (current) {
            std::sort (GetBeginPtr(adj_idx), &begin[GetSlot(adj_idx)->end_offset], std::less<node_t>());
        }
        //fprintf(stdout, "\t[%ld] %ld [ConvertVidToDegreeOrderAndSortOfCachedMatAdj] %p, src_vid = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, this, src_vid);

        if (delta) {
            for (int64_t i = GetSlot(adj_idx)->end_offset; i < GetSlot(adj_idx)->delta_end_offset[0]; i++) {
                node_t dst_vid = begin[i];
                node_t dst_degree_order = PartitionStatistics::VidToDegreeOrder(dst_vid);
                begin[i] = dst_degree_order;
                //fprintf(stdout, "[%ld] %ld [ConvertVidToDegreeOrderAndSortOfCachedMatAdj::INSERT] src_vid = %ld, dst_vid = %ld, dst_degree_order = %ld at %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, GetSlotVid(adj_idx), dst_vid, dst_degree_order, &begin[i]);
            }
            std::sort (&begin[GetSlot(adj_idx)->end_offset], &begin[GetSlot(adj_idx)->delta_end_offset[0]], std::less<node_t>());
            for (int64_t i = GetSlot(adj_idx)->delta_end_offset[0]; i < GetSlot(adj_idx)->delta_end_offset[1]; i++) {
                node_t dst_vid = begin[i];
                node_t dst_degree_order = PartitionStatistics::VidToDegreeOrder(dst_vid);
                begin[i] = dst_degree_order;
                //fprintf(stdout, "[%ld] %ld [ConvertVidToDegreeOrderAndSortOfCachedMatAdj::DELETE] src_vid = %ld, dst_vid = %ld, dst_degree_order = %ld at %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, GetSlotVid(adj_idx), dst_vid, dst_degree_order, &begin[i]);
            }
            std::sort (&begin[GetSlot(adj_idx)->delta_end_offset[0]], &begin[GetSlot(adj_idx)->delta_end_offset[1]], std::less<node_t>());
        }
    }
}


void MaterializedAdjacencyLists::ConvertVidToDegreeOrderAndSort() {
    ALWAYS_ASSERT (UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
    ALWAYS_ASSERT (Data() != NULL);
    ALWAYS_ASSERT (GetNumAdjacencyLists() > 0);
    ALWAYS_ASSERT (GetNumEdges() > 0);
    ALWAYS_ASSERT (!IsEmpty());
    
    //fprintf(stdout, "[ConvertVidToDegreeOrderAndSort] %p [%ld, %ld]\n", this, GetSlotVid(0), GetSlotVid(GetNumAdjacencyLists() - 1));

    int64_t sz = Size() / sizeof(node_t);
    node_t* begin = (node_t*) Data();
    int64_t idx = 0;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        ALWAYS_ASSERT (GetBeginPtr(adj_idx) <= GetBeginPtrOfDeltaInsert(adj_idx));
        ALWAYS_ASSERT (GetBeginPtrOfDeltaInsert(adj_idx) <= GetBeginPtrOfDeltaDelete(adj_idx));
        
        node_t src_vid = GetSlot(adj_idx)->src_vid;
        node_t src_degree_order = PartitionStatistics::VidToDegreeOrder(src_vid);
        GetSlot(adj_idx)->src_vid = src_degree_order;
    
        if (UserArguments::UPDATE_VERSION >= 11) {
            //fprintf(stdout, "[ConvertVidToDegreeOrderAndSort] %p %ld, %p %p %p\n", this, GetSlotVid(adj_idx), GetBeginPtr(adj_idx), GetBeginPtrOfDeltaInsert(adj_idx), GetBeginPtrOfDeltaDelete(adj_idx));
        }

        int64_t from = -1, to = -1;
        from = idx;
        for (int64_t i = idx; i < GetSlot(adj_idx)->end_offset; i++) {
            node_t dst_vid = begin[i];
            node_t dst_degree_order = PartitionStatistics::VidToDegreeOrder(dst_vid);
            begin[i] = dst_degree_order;
            idx++;
        }
        to = idx;
        std::sort (&begin[from], &begin[to], std::less<node_t>());
        ALWAYS_ASSERT(std::is_sorted(&begin[from], &begin[to], std::less<node_t>())); 

        for (int dbtype = 0; dbtype < 2; dbtype++) {
            from = idx;
            for (int64_t i = idx; i < GetSlot(adj_idx)->delta_end_offset[dbtype]; i++) {
                node_t dst_vid = begin[i];
                node_t dst_degree_order = PartitionStatistics::VidToDegreeOrder(dst_vid);
                begin[i] = dst_degree_order;
                idx++;
            }
            to = idx;
            std::sort (&begin[from], &begin[to], std::less<node_t>());
            ALWAYS_ASSERT(std::is_sorted(&begin[from], &begin[to], std::less<node_t>())); 
        }
    }

}

// Used only before ConvertVidToDegreeOrder
bool MaterializedAdjacencyLists::SanityCheckUsingVidRange(Range<node_t> vid_range) {
    ALWAYS_ASSERT (GetNumAdjacencyLists() != 0);
    ALWAYS_ASSERT (GetNumEdges() != 0);

    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    node_t* begin = (node_t*) Data();
    int64_t idx = 0;
    node_t prev_src_vid = -1;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        if(!vid_range.contains(GetSlot(adj_idx)->src_vid)) {
            fprintf(stdout, "%ld (among %ld) not in [%ld, %ld]\n", GetSlot(adj_idx)->src_vid, GetNumAdjacencyLists(), vid_range.GetBegin(), vid_range.GetEnd());
            return false;
        }
        ALWAYS_ASSERT (GetSlot(adj_idx)->src_vid > prev_src_vid);
        ALWAYS_ASSERT (GetNumEntries(adj_idx) > 0);
        prev_src_vid = GetSlot(adj_idx)->src_vid;
        node_t prev_vid = -1;
    }

    //fprintf(stdout, "Checked %ld [%ld, %ld]\n", GetNumAdjacencyLists(), vid_range.GetBegin(), vid_range.GetEnd());
    return true;
}

bool MaterializedAdjacencyLists::SanityCheckDegreeOrdered(int cached) {
    ALWAYS_ASSERT (GetNumAdjacencyLists() != 0);
    ALWAYS_ASSERT (GetNumEdges() != 0);

    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    node_t* begin = (node_t*) Data();
    int64_t idx = 0;
    node_t prev_src_vid = -1;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        ALWAYS_ASSERT (GetSlotVid(adj_idx) > prev_src_vid);
        ALWAYS_ASSERT (GetNumEntries(adj_idx) > 0);
        prev_src_vid = GetSlotVid(adj_idx);
        node_t prev_vid = -1;
        for (int64_t i = idx; i < GetSlot(adj_idx)->end_offset; i++) {
            if (begin[i] <= prev_vid) {
                for (int64_t j = idx; j <= i; j++) {
                    fprintf(stdout, "cached = %ld, vid = %ld, i = %lld, idx = %lld, SlotIdx: %ld, SlotNumEntries: %ld, begin[%lld]: %lld, begin[%lld-1] = %lld, GetSlot(adj_idx)->end_offset = %ld\n", cached, prev_src_vid, i, GetNumEntries(adj_idx),idx, adj_idx, j, (int64_t) begin[j], j, (int64_t) prev_vid, GetSlot(adj_idx)->end_offset);
                }
            }
            ALWAYS_ASSERT (begin[i] > prev_vid);
            ALWAYS_ASSERT (begin[i] >= 0 && begin[i] < PartitionStatistics::num_total_nodes());
            prev_vid = begin[i];
            idx++;
        }
    }
    return true;
}

bool MaterializedAdjacencyLists::SanityCheckOrderdSrcVids() {
    node_t prev_src_vid = -1;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        if (GetSlotVid(adj_idx) <= prev_src_vid) return false;
        prev_src_vid = GetSlotVid(adj_idx);
    }
    return true;
}

bool MaterializedAdjacencyLists::SanityCheckUsingDegree(bool convert_deg_order) {
    if (PartitionStatistics::num_machines() > 1 && UserArguments::CURRENT_LEVEL > 0) return true;

    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    node_t* begin = (node_t*) Data();
    node_t* nbrs = NULL;
    int64_t idx = 0;
    node_t prev_src_vid = -1;
    
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists(); adj_idx++) {
		node_t vid = GetSlot(adj_idx)->src_vid;
        if (convert_deg_order) {
            vid = PartitionStatistics::DegreeOrderToVid(vid);
        }

        
        ALWAYS_ASSERT(vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id());
        if (!(vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id())) return false;

        if (UserArguments::USE_DELTA_NWSM) {
            if (!(GetNumEntriesOfCurrent(adj_idx) == TurboDB::OutDegree(vid) - TurboDB::OutInsertDegree(vid) + TurboDB::OutDeleteDegree(vid))) return false;
            ALWAYS_ASSERT (GetNumEntriesOfCurrent(adj_idx) == TurboDB::OutDegree(vid) - TurboDB::OutInsertDegree(vid) + TurboDB::OutDeleteDegree(vid));
            ALWAYS_ASSERT (GetNumEntriesOfDeltaInsert(adj_idx) == TurboDB::OutInsertDegree(vid));
            ALWAYS_ASSERT (GetNumEntriesOfDeltaDelete(adj_idx) == TurboDB::OutDeleteDegree(vid));
            ALWAYS_ASSERT (GetNumEntries(adj_idx) == TurboDB::OutDegree(vid) + 2 * TurboDB::OutDeleteDegree(vid));
        } else {
            ALWAYS_ASSERT (GetNumEntriesOfCurrent(adj_idx) == TurboDB::OutDegree(vid));
        }
    }

    return true;
}

bool MaterializedAdjacencyLists::SanityCheckUniqueness() {
    //fprintf(stdout, "[SanityCheckUniqueness] %p [%ld, %ld]\n", this, GetSlotVid(0), GetSlotVid(GetNumAdjacencyLists() - 1));

    int64_t sz = Size() / sizeof(node_t);
    node_t* begin = (node_t*) Data();
    int64_t idx = 0;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        node_t src_vid = GetSlot(adj_idx)->src_vid;

        int64_t from = -1, to = -1;
        from = idx;
        for (int64_t i = idx; i < GetSlot(adj_idx)->end_offset; i++) {
            idx++;
        }
        to = idx;
        std::sort (&begin[from], &begin[to], std::less<node_t>());
        /*
        if (std::adjacent_find(&begin[from], &begin[to]) != &begin[to]) {
            node_t* pos = std::adjacent_find(&begin[from], &begin[to]);
            fprintf(stdout, "\t[SanityCheckUniqueness] %p [%ld, %ld], src_vid = %ld has %ld %ld at %p\n", this, GetSlotVid(0), GetSlotVid(GetNumAdjacencyLists() - 1), src_vid, *pos, *(pos+1), pos);
        }
        */
        ALWAYS_ASSERT(std::adjacent_find(&begin[from], &begin[to]) == &begin[to]); 

        for (int dbtype = 0; dbtype < 2; dbtype++) {
            from = idx;
            for (int64_t i = idx; i < GetSlot(adj_idx)->delta_end_offset[dbtype]; i++) {
                idx++;
            }
            to = idx;
            std::sort (&begin[from], &begin[to], std::less<node_t>());
            ALWAYS_ASSERT(std::adjacent_find(&begin[from], &begin[to]) == &begin[to]); 
        }
    }
    return true;
}

bool MaterializedAdjacencyLists::SanityCheckUsingOrderedSrcVids() {
    node_t prev_src_vid = -1;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists(); adj_idx++) {
        if(prev_src_vid >= GetSlotVid(adj_idx)) return false;
        ALWAYS_ASSERT(prev_src_vid < GetSlotVid(adj_idx));
        prev_src_vid = GetSlotVid(adj_idx);
    }
    return true;
}

bool MaterializedAdjacencyLists::SanityCheck(bool cur, bool ins, bool del) {
    ALWAYS_ASSERT (GetNumAdjacencyLists() != 0);
    ALWAYS_ASSERT (GetNumEdges() != 0);

    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    node_t* begin = (node_t*) Data();
    node_t* nbrs = NULL;
    int64_t idx = 0;
                        
    //fprintf(stdout, "[SanityCheck] %p [%ld, %ld]\n", this, GetSlotVid(0), GetSlotVid(GetNumAdjacencyLists() - 1));
   
    node_t prev_src_vid = -1;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists(); adj_idx++) {
        if (GetNumEntries(adj_idx) <= 0) return false;
        ALWAYS_ASSERT (GetNumEntries(adj_idx) > 0);
        //fprintf(stdout, "[SanityCheck] src_vid = %ld\n", GetSlotVid(adj_idx));

        ALWAYS_ASSERT (GetBeginPtr(adj_idx) <= GetBeginPtrOfDeltaInsert(adj_idx));
        ALWAYS_ASSERT (GetBeginPtrOfDeltaInsert(adj_idx) <= GetBeginPtrOfDeltaDelete(adj_idx));
        ALWAYS_ASSERT (GetBeginPtr(adj_idx) + GetNumEntriesOfCurrent(adj_idx) == GetBeginPtrOfDeltaInsert(adj_idx));
        ALWAYS_ASSERT (GetBeginPtrOfDeltaInsert(adj_idx) + GetNumEntriesOfDeltaInsert(adj_idx) == GetBeginPtrOfDeltaDelete(adj_idx));
        ALWAYS_ASSERT (std::is_sorted(GetBeginPtr(adj_idx), GetBeginPtrOfDeltaInsert(adj_idx), std::less<node_t>())); 
        ALWAYS_ASSERT (std::is_sorted(GetBeginPtrOfDeltaInsert(adj_idx), GetBeginPtrOfDeltaDelete(adj_idx), std::less<node_t>())); 
        ALWAYS_ASSERT (std::is_sorted(GetBeginPtrOfDeltaDelete(adj_idx), GetBeginPtrOfDeltaDelete(adj_idx) + GetNumEntriesOfDeltaDelete(adj_idx), std::less<node_t>())); 
       
        if (cur) {
            nbrs = &begin[idx];
            node_t prev_vid = -1;
            for (int64_t i = 0; i < GetNumEntriesOfCurrent(adj_idx); i++) {
                if (nbrs[i] <= prev_vid) {
                    fprintf(stdout, "[%ld][SanityCheck::CUR] src_vid = %ld, nbrs[%lld]: %lld, prev_vid = %lld, |prev_nbrs| = %ld, |ins| = %ld, |del| = %ld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), i, (int64_t) PartitionStatistics::DegreeOrderToVid(nbrs[i]), (int64_t) PartitionStatistics::DegreeOrderToVid(prev_vid), (int64_t) GetNumEntriesOfCurrent(adj_idx), (int64_t) GetNumEntriesOfDeltaInsert(adj_idx), (int64_t) GetNumEntriesOfDeltaDelete(adj_idx));
                    //PrintContents(true, true, true);
                    for (int64_t j = idx; j <= i + idx; j++) {
                        //fprintf(stdout, "[%ld][SanityCheck::CUR] src_vid = %ld, i = %lld, idx = %lld, Slot: %ld, nbrs[%lld]: %lld, prev_vid = %lld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), i, idx, adj_idx, j, (int64_t) begin[j], (int64_t) (j == idx ? -1 : begin[j-1]));
                    }
                    return false;
                    LOG_ASSERT(false);
                }
                if (nbrs[i] <= prev_vid) return false;
                prev_vid = nbrs[i];
                idx++;
            }
        }
        if (ins) {
            nbrs = &begin[idx];
            node_t prev_vid = -1;
            for (int64_t i = 0; i < GetNumEntriesOfDeltaInsert(adj_idx); i++) {
                if (nbrs[i] <= prev_vid) {
                    fprintf(stdout, "[%ld][SanityCheck::INS] src_vid = %ld, nbrs[%lld]: %lld, prev_vid = %lld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), i, (int64_t) nbrs[i], (int64_t) prev_vid);
                    for (int64_t j = idx; j <= i + idx; j++) {
                        //fprintf(stdout, "[%ld][SanityCheck::INS] src_vid = %ld, i = %lld, idx = %lld, Slot: %ld, nbrs[%lld]: %lld, prev_vid = %lld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), i, idx, adj_idx, j, (int64_t) begin[j], (int64_t) prev_vid);
                    }
                    LOG_ASSERT(false);
                }
                if (nbrs[i] <= prev_vid) return false;
                prev_vid = nbrs[i];
                idx++;
            }
        }
        if (del) {
            nbrs = &begin[idx];
            node_t prev_vid = -1;
            for (int64_t i = 0; i < GetNumEntriesOfDeltaDelete(adj_idx); i++) {
                if (nbrs[i] <= prev_vid) {
                    fprintf(stdout, "[%ld][SanityCheck::DEL] src_vid = %ld, nbrs[%lld]: %lld, prev_vid = %lld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), i, (int64_t) nbrs[i], (int64_t) prev_vid);
                    for (int64_t j = idx; j <= i + idx; j++) {
                        //fprintf(stdout, "[%ld][SanityCheck::DEL] src_vid = %ld, i = %lld, idx = %lld, Slot: %ld, nbrs[%lld]: %lld, prev_vid = %lld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), i, idx, adj_idx, j, (int64_t) begin[j], (int64_t) prev_vid);
                    }
                    LOG_ASSERT(false);
                }
                if (nbrs[i] <= prev_vid) return false;
                prev_vid = nbrs[i];
                idx++;
            }
        }
    }
    return true;
}

void MaterializedAdjacencyLists::PrintNSlots(int64_t num_slots) {
    for (int64_t i = 0; i < num_slots; i++) {
        fprintf(stdout, "Slot: %ld, SrcVid: %ld, EndOffset: %ld\n", i, GetSlotVid(i), GetSlot(i)->end_offset);
    }
}

void MaterializedAdjacencyLists::MarkVerticesIntoBitMap(UdfSendRequest req, BitMap<node_t>* vids, node_t offset_vid) {
	MaterializedAdjacencyLists mat_adj;
	mat_adj.Initialize(req.cont.size_used, req.cont.capacity, req.cont.data);

    //fprintf(stdout, "[%ld] [MarkVerticesIntoBitMap] %p %ld, offset_vid = %ld\n", PartitionStatistics::my_machine_id(), req.cont.data, req.req.parm1, offset_vid);
    for(int64_t adj_idx = 0 ; adj_idx < mat_adj.GetNumAdjacencyLists() ; adj_idx++) {
        node_t vid = -1;
        if (req.req.parm1 == 0) {
            vid = mat_adj.GetSlot(adj_idx)->src_vid;
        } else {
            vid = mat_adj.GetSlotVid(adj_idx);
        }
        node_t lvid = vid - offset_vid;
        //fprintf(stdout, "\t[%ld] [MarkVerticesIntoBitMap] %p %ld, vid = %ld, lvid = %ld\n", PartitionStatistics::my_machine_id(), req.cont.data, req.req.parm1, vid, lvid);
        if(vids->Get_Atomic(lvid)) {
            fprintf(stdout, "[%ld] MarkVerticesIntoBitMap %ld already marked\n", PartitionStatistics::my_machine_id(), vid);
        }
        ALWAYS_ASSERT(!vids->Get_Atomic(lvid));
        vids->Set(lvid);   // XXX - should be atomic?
    }
}

void MaterializedAdjacencyLists::ClearVerticesIntoBitMap(UdfSendRequest req, BitMap<node_t>* vids, node_t offset_vid) {
	MaterializedAdjacencyLists mat_adj;
	mat_adj.Initialize(req.cont.size_used, req.cont.capacity, req.cont.data);
    //fprintf(stdout, "[%ld] [ClearVerticesIntoBitMap] %p %ld, offset_vid = %ld\n", PartitionStatistics::my_machine_id(), req.cont.data, req.req.parm1, offset_vid);

    for(int64_t adj_idx = 0 ; adj_idx < mat_adj.GetNumAdjacencyLists() ; adj_idx++) {
        node_t vid = -1;
        if (req.req.parm1 == 0) {
            vid = mat_adj.GetSlot(adj_idx)->src_vid;
        } else {
            vid = mat_adj.GetSlotVid(adj_idx);
        }
        node_t lvid = vid - offset_vid;
        //fprintf(stdout, "\t[%ld] [ClearVerticesIntoBitMap] %p %ld, vid = %ld, lvid = %ld\n", PartitionStatistics::my_machine_id(), req.cont.data, req.req.parm1, vid, lvid);
        //ALWAYS_ASSERT(vids->Get_Atomic(lvid));    // why not?
        vids->Clear(lvid);   // XXX - should be atomic?
    }
}

void MaterializedAdjacencyLists::PrintContents(bool cur, bool ins, bool del) {
    TG_AdjacencyMatrixPageIterator adjlist_iter;
    TG_AdjacencyListIterator nbrlist_iter;

    node_t* begin = (node_t*) Data();
    node_t* nbrs = NULL;
    int64_t idx = 0;
    node_t prev_src_vid = -1;
    for(int64_t adj_idx = 0 ; adj_idx < GetNumAdjacencyLists() ; adj_idx++) {
        ALWAYS_ASSERT (GetNumEntries(adj_idx) > 0);
        if (cur) {
            nbrs = GetBeginPtr(adj_idx);
            for (int64_t i = 0; i < GetNumEntriesOfCurrent(adj_idx); i++) {
                fprintf(stdout, "[%ld][PrintContents::CUR] src_vid = %ld, dst_vid = %ld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), nbrs[i]);
            }
        }
        if (ins) {
            nbrs = GetBeginPtrOfDeltaInsert(adj_idx);
            for (int64_t i = 0; i < GetNumEntriesOfDeltaInsert(adj_idx); i++) {
                fprintf(stdout, "[%ld][PrintContents::INS] src_vid = %ld, dst_vid = %ld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), nbrs[i]);
            }
        }
        if (del) {
            nbrs = GetBeginPtrOfDeltaDelete(adj_idx);
            for (int64_t i = 0; i < GetNumEntriesOfDeltaDelete(adj_idx); i++) {
                fprintf(stdout, "[%ld][PrintContents::DEL] src_vid = %ld, dst_vid = %ld\n", PartitionStatistics::my_machine_id(), GetSlotVid(adj_idx), nbrs[i]);
            }
        }
    }
}
