#include "analytics/util/TG_NWSM_Utility.hpp"
#include "analytics/datastructure/TwoLevelBitMap.hpp"
#include "analytics/core/TurboDB.hpp"

int64_t ComputeMemorySizeOfAdjacencyLists(BitMap<node_t>& vids, Range<node_t>& vid_range) {
	int64_t adjlist_data_size = 0;
	int64_t slot_data_size = 0;
	int64_t page_metadata_size = sizeof(node_t);
	int64_t size_to_request = 0;
	for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
		node_t lid = LocalStatistics::Vid2Lid(vid);
		if (!vids.Get(lid)) continue;
		adjlist_data_size += sizeof(node_t) * TurboDB::OutDegree(vid);
		slot_data_size += sizeof(Slot32);
	}
	size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;
	return size_to_request;
}

int64_t GroupNodeLists(int target_machine, TwoLevelBitMap<node_t>& vids, Range<node_t>& vid_range, int64_t MaxBytesToPin, Range<node_t>& ProcessedVertices) {
	if (target_machine == PartitionStatistics::my_machine_id()) {
		return GroupNodeLists(vids, vid_range, MaxBytesToPin, ProcessedVertices);
	} else {
		LOG_ASSERT (false);
	}
}

int64_t GroupNodeLists(TwoLevelBitMap<node_t>& vids, Range<node_t>& vid_range, int64_t MaxBytesToPin, Range<node_t>& ProcessedVertices) {
	INVARIANT(TurboDB::is_out_degree_vector_initialized_);
	if(!PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id()).contains(vid_range)) {
		fprintf(stdout, "[%ld, %ld] does not contain [%ld, %ld]\n"
		        ,PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id()).GetBegin()
		        ,PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id()).GetEnd()
		        ,vid_range.GetBegin()
		        ,vid_range.GetEnd());
	}
	INVARIANT(PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id()).contains(vid_range));

	ALWAYS_ASSERT (MaxBytesToPin != 0);
	fprintf(stdout, "BEGIN GroupNodeLists; vid_range [%lld, %lld]; MaxBytesToPin = %lld\n", (int64_t) vid_range.GetBegin(), (int64_t) vid_range.GetEnd(), MaxBytesToPin);

	ProcessedVertices.Set(vid_range.GetBegin(), vid_range.GetEnd());

	int64_t total_src_vertices_cnt = 0;
	int64_t total_edges_cnt = 0;

	int64_t adjlist_data_size = 0;
	int64_t slot_data_size = 0;
	int64_t page_metadata_size;
	int64_t size_to_request = 0;
	for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
		node_t lid = LocalStatistics::Vid2Lid(vid);
		if (!vids.Get(lid)) continue;

		total_src_vertices_cnt++;
		total_edges_cnt += TurboDB::OutDegree(vid);

		adjlist_data_size += sizeof(node_t) * TurboDB::OutDegree(vid);
		slot_data_size += sizeof(Slot32);
		page_metadata_size = sizeof(node_t) * ((adjlist_data_size + slot_data_size + DEFAULT_NIO_BUFFER_SIZE - 1) / DEFAULT_NIO_BUFFER_SIZE);

		size_to_request = adjlist_data_size + slot_data_size + page_metadata_size;
		ProcessedVertices.SetEnd(vid);

		// 5% margin
		if (MaxBytesToPin != -1 && size_to_request >= MaxBytesToPin * 0.95) {
			break;
		}
	}

	INVARIANT (MaxBytesToPin == -1 || size_to_request <= MaxBytesToPin);
	fprintf(stdout, "GroupNodeLists; vid_range [%lld, %lld] into %lld MB sized block; MaxBytesToPin = %lld\n", (int64_t) ProcessedVertices.GetBegin(), (int64_t) ProcessedVertices.GetEnd(), size_to_request / (1024*1024L), MaxBytesToPin);
	return size_to_request;
}

int64_t CountNumPagesToStream(TwoLevelBitMap<node_t>& active_vertices, Range<int64_t>& dst_edge_partitions, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	int version_from = version_range.GetBegin();
	int version_to = version_range.GetEnd();

    int64_t num_pages = 0;

#pragma omp parallel 
    {
        TwoLevelBitMap<node_t>* vids = NULL;
        int64_t my_num_pages = 0;
#pragma omp for collapse(2)
        for (int version = version_from; version <= version_to; version++) {
            for (int64_t i = dst_edge_partitions.begin ; i <= dst_edge_partitions.end; i++) {
                if (!TurboDB::GetTurboDB(e_type)->is_version_exist(d_type, version)) continue;
                VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version, d_type);
                PageID first_page_id = TurboDB::GetTurboDB(e_type)->GetFirstPageId(i, version, d_type);
                PageID last_page_id = TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version, d_type);
                if (first_page_id == -1 || last_page_id == -1) continue;
                for (PageID pid = first_page_id; pid <= last_page_id; pid++) {
                    Range<node_t> vid_range = vidrangeperpage.Get(pid);
                    PageID table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, pid, e_type, d_type);
                    node_t begin_lid = LocalStatistics::Vid2Lid(vid_range.GetBegin());
                    node_t end_lid = LocalStatistics::Vid2Lid(vid_range.GetEnd());
                    
                    if (active_vertices.IsSetInRange(begin_lid, end_lid)) {
                        my_num_pages++;
                    }
                }
            }
        }

        std::atomic_fetch_add((std::atomic<int64_t>*) &num_pages, my_num_pages);
    }
	return num_pages;
}

void GroupNodeListsIntoPageListsForStreaming(TwoLevelBitMap<node_t>& main_vids, TwoLevelBitMap<node_t>& delta_vids, Range<node_t>& input_vid_range, Range<int64_t>& dst_edge_partitions, TwoLevelBitMap<PageID>& pids, PageID MaxNumPagesToPin, Range<node_t>& ProcessedVertices, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
	ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);
    INVARIANT (MaxNumPagesToPin == std::numeric_limits<PageID>::max());

	int version_from = version_range.GetBegin();
	int version_to = version_range.GetEnd();
    int64_t num_pages_iterated = 0;
    int64_t num_loops_iterated = 0;
    

    turbo_timer group_timer;
    // For Main DB
    group_timer.start_timer(0);
    if (version_from == 0 && TurboDB::GetTurboDB(e_type)->is_version_exist(d_type, version_from)) {
        TwoLevelBitMap<node_t>* vids = NULL;
        if (UserArguments::USE_DELTA_NWSM && version_from == UserArguments::UPDATE_VERSION) {
            vids = &delta_vids;
        } else {
            vids = &main_vids;
        }
        group_timer.start_timer(1);
        int64_t num_target_vertices = vids->GetTotal();
        group_timer.stop_timer(1);
        if (num_target_vertices != 0) {
            //if (false) {
            if (num_target_vertices <= 1000) { //XXX Threshold value?
                group_timer.start_timer(2);
#pragma omp parallel
                {
                    VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version_from, d_type);
#pragma omp for
                    for (int64_t i = dst_edge_partitions.begin ; i <= dst_edge_partitions.end; i++) {
                        node_t vid_max = -1;
                        vids->InvokeIfMarked([&](node_t idx) {
                            node_t vid = idx + PartitionStatistics::my_first_internal_vid();
                            if (vid < vid_max) return;
                            Range<PageID> pid_range = TurboDB::GetTurboDB(e_type)->GetPageRangeByVid(i, vid, version_from, e_type, d_type);
                            if (pid_range.GetBegin() == -1 || pid_range.GetEnd() == -1) return;
                            for (PageID pid = pid_range.GetBegin(); pid <= pid_range.GetEnd(); pid++) {
                                if (pid == pid_range.GetEnd()) {
                                    Range<node_t> vid_range_in_page = vidrangeperpage.Get(pid);
                                    if (vid_max < vid_range_in_page.GetEnd())
                                        vid_max = vid_range_in_page.GetEnd();
                                }
                                PageID table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version_from, pid, e_type, d_type);
                                pids.Set_Atomic(table_page_id); 
                            }
                        }, true);
                    }
                }
                group_timer.stop_timer(2);
            } else {
                group_timer.start_timer(3);
                node_t first = vids->FindFirstMarkedEntry() + PartitionStatistics::my_first_internal_vid();
                node_t last = vids->FindLastMarkedEntry() + PartitionStatistics::my_first_internal_vid();
                Range<node_t> target_range(first, last);
                //fprintf(stdout, "[%ld] First = %ld, Last = %ld, portion %.2f\n", PartitionStatistics::my_machine_id(), first, last, (double) 100 * ((double)(last - first + 1) / (double)input_vid_range.length()));
                for (int64_t i = dst_edge_partitions.begin ; i <= dst_edge_partitions.end; i++) {
                    VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version_from, d_type);
                    PageID first_page_id = TurboDB::GetTurboDB(e_type)->GetFirstPageId(i, version_from, d_type);
                    PageID last_page_id = TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version_from, d_type);
                    if (first_page_id == -1 || last_page_id == -1) continue;
#pragma omp parallel for
                    for (PageID pid = first_page_id; pid <= last_page_id; pid++) {
                        Range<node_t> vid_range = vidrangeperpage.Get(pid);
                        if (!input_vid_range.Overlapped(vid_range)) continue;
                        //if (!target_range.Overlapped(vid_range)) continue;
                        PageID table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version_from, pid, e_type, d_type);
                        node_t begin_lid = LocalStatistics::Vid2Lid(vid_range.GetBegin());
                        node_t end_lid = LocalStatistics::Vid2Lid(vid_range.GetEnd());

                        if (vids->IsSetInRange(begin_lid, end_lid)) {
                            pids.Set_Atomic(table_page_id);
                        }
                    }
                }
                group_timer.stop_timer(3);
            }
        }
        version_from++;
    }
    group_timer.stop_timer(0);
    //fprintf(stdout, "[%ld] (%d, %d, %d) GroupNodeListsIntoPageListsForStreaming1 vid_range [%ld, %ld] version_range [%d, %d] use_delta_nwsm = %s, Main Total = %ld, Delta Total = %ld, pids Total = %ld, %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, input_vid_range.GetBegin(), input_vid_range.GetEnd(), version_range.GetBegin(), version_range.GetEnd(), UserArguments::USE_DELTA_NWSM ? "true" : "false", main_vids.GetTotal(), delta_vids.GetTotal(), pids.GetTotal(), group_timer.get_timer(0), group_timer.get_timer(1), group_timer.get_timer(2), group_timer.get_timer(3));
    fprintf(stdout, "[%ld] (%d, %d, %d) GroupNodeListsIntoPageListsForStreaming1 vid_range [%ld, %ld] version_range [%d, %d] %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, input_vid_range.GetBegin(), input_vid_range.GetEnd(), version_range.GetBegin(), version_range.GetEnd(), group_timer.get_timer(0), group_timer.get_timer(1), group_timer.get_timer(2), group_timer.get_timer(3));

    if (version_from > version_to) return;
    // For Delta DB
    group_timer.start_timer(4);
#pragma omp parallel 
    {
        TwoLevelBitMap<node_t>* vids = NULL;
#pragma omp for collapse(2)
        for (int64_t i = dst_edge_partitions.begin ; i <= dst_edge_partitions.end; i++) {
            for (int version = version_from; version <= version_to; version++) {
                if (UserArguments::USE_DELTA_NWSM && version == UserArguments::UPDATE_VERSION) {
                    vids = &delta_vids;
                } else {
                    vids = &main_vids;
                }
                if (!TurboDB::GetTurboDB(e_type)->is_version_exist(d_type, version)) continue;
                VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version, d_type);
                PageID first_page_id = TurboDB::GetTurboDB(e_type)->GetFirstPageId(i, version, d_type);
                PageID last_page_id = TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version, d_type);
                if (first_page_id == -1 || last_page_id == -1) continue;
                //std::atomic_fetch_add((std::atomic<int64_t>*) &num_loops_iterated, +1L);
                //std::atomic_fetch_add((std::atomic<int64_t>*) &num_pages_iterated, (int64_t)(last_page_id - first_page_id + 1));
//#pragma omp for
                for (PageID pid = first_page_id; pid <= last_page_id; pid++) {
                    Range<node_t> vid_range = vidrangeperpage.Get(pid);
                    if (!input_vid_range.Overlapped(vid_range)) continue;
                    PageID table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version, pid, e_type, d_type);
                    node_t begin_lid = LocalStatistics::Vid2Lid(vid_range.GetBegin());
                    node_t end_lid = LocalStatistics::Vid2Lid(vid_range.GetEnd());
                    
                    //if (version != version_from || vids->IsSetInRange(begin_lid, end_lid)) {
                    if (vids->IsSetInRange(begin_lid, end_lid)) {
                        pids.Set_Atomic(table_page_id);
                    }
                }
            }
        }
    }
    group_timer.stop_timer(4);
    //fprintf(stdout, "[%ld] GroupNodeListsIntoPageListsForStreaming num_loops_iterated = %ld, num_pages_iterated = %ld\n", PartitionStatistics::my_machine_id(), num_loops_iterated, num_pages_iterated);
    //fprintf(stdout, "[%ld] (%d, %d, %d) GroupNodeListsIntoPageListsForStreaming2 vid_range [%ld, %ld] version_range [%d, %d] use_delta_nwsm = %s, Main Total = %ld, Delta Total = %ld, pids Total = %ld, %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, input_vid_range.GetBegin(), input_vid_range.GetEnd(), version_range.GetBegin(), version_range.GetEnd(), UserArguments::USE_DELTA_NWSM ? "true" : "false", main_vids.GetTotal(), delta_vids.GetTotal(), pids.GetTotal(), group_timer.get_timer(4));
    fprintf(stdout, "[%ld] (%d, %d, %d) GroupNodeListsIntoPageListsForStreaming2 vid_range [%ld, %ld] version_range [%d, %d] use_delta_nwsm = %s %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, UserArguments::INC_STEP, input_vid_range.GetBegin(), input_vid_range.GetEnd(), version_range.GetBegin(), version_range.GetEnd(), UserArguments::USE_DELTA_NWSM ? "true" : "false", group_timer.get_timer(4));
	return;
}

//XXX - TODO tslee
void GroupNodeListsIntoPageLists(TwoLevelBitMap<node_t>& vids, Range<node_t>& vid_range, Range<int64_t>& dst_edge_partitions, TwoLevelBitMap<PageID>& pids, PageID MaxNumPagesToPin, Range<node_t>& ProcessedVertices) {
    ALWAYS_ASSERT(false);
    abort();
}

ReturnStatus TG_AdjacencyListIterator::GetNext(node_t& neighbor) {
	ALWAYS_ASSERT (mBeginPtr != NULL || (mBeginPtr == NULL && mNumEntries == 0));
	if(mCurrentOffset+1  >= (int)mNumEntries) {
		mBeginPtr = NULL;
		return DONE;
	}
	mCurrentOffset++;

	ALWAYS_ASSERT (mCurrentOffset >= 0);
	ALWAYS_ASSERT (mCurrentOffset < (int)mNumEntries);
	neighbor=mBeginPtr[mCurrentOffset];

	ALWAYS_ASSERT(neighbor != -1);
	return OK;
}

ReturnStatus TG_AdjacencyListIterator::InitCursor(node_t start_dst_vid) {
	if(mNumEntries == 0) {
		return DONE;
	}
	//binary search
	node_t* dst_pos = (node_t*)std::lower_bound(mBeginPtr, mBeginPtr + mNumEntries, start_dst_vid);
	if(*dst_pos == start_dst_vid) {
		mStartOffset = (dst_pos + mNumEntries - mBeginPtr) % mNumEntries;
		mCurrentOffset -= 1;
	} else {
		mStartOffset = dst_pos + 1 - mBeginPtr;
		if(mStartOffset >= mNumEntries)
			mStartOffset = 0;
		mCurrentOffset = -1;
		return FAIL;
	}

	return OK;
}
ReturnStatus TG_AdjacencyListIterator::MoveCursor(node_t dst_vid) {
	if (mNumEntries == 0) {
		return DONE;
	}

	//binary search
	node_t* dst_pos = (node_t*)std::lower_bound(mBeginPtr, mBeginPtr + mNumEntries, dst_vid);
	if(*dst_pos == dst_vid) {
		mCurrentOffset = (dst_pos + mNumEntries - mBeginPtr) % mNumEntries;
		mCurrentOffset -= 1;
	} else {
		mCurrentOffset = -1;
		return FAIL;
	}

	return OK;
}


ReturnStatus TG_AdjacencyMatrixPageIterator::ReadNextPage (BitMap<PageID>& ActivePages) 	{
	abort();
	return OK;
}

ReturnStatus TG_AdjacencyMatrixPageIterator::SetCurrentPage (PageID pid, Page* page) {
	mBuffer = page;
	ALWAYS_ASSERT(mBuffer != NULL);

	mCurrentPid=pid;
	mNumSlots=mBuffer->NumEntry();
    if (mNumSlots > 100) is_sparse = true;
    else is_sparse = false;
	mCurrentLid=-1;
	mCurrentSlotIdx=-1;
    PageFirstLid = LocalStatistics::Vid2Lid(mBuffer->GetSlot(0)->src_vid);
    PageLastLid = LocalStatistics::Vid2Lid(mBuffer->GetSlot(mNumSlots - 1)->src_vid);
    FirstVid = PartitionStatistics::my_first_internal_vid();
	return OK;
}
ReturnStatus TG_AdjacencyMatrixPageIterator::SetCurrentPage (Page* page) {
	mBuffer = page;
	ALWAYS_ASSERT(mBuffer != NULL);

	mCurrentPid=-1;
	mNumSlots=mBuffer->NumEntry();

	mCurrentLid=-1;
	mCurrentSlotIdx=-1;
    PageFirstLid = LocalStatistics::Vid2Lid(mBuffer->GetSlot(0)->src_vid);
    PageLastLid = LocalStatistics::Vid2Lid(mBuffer->GetSlot(mNumSlots - 1)->src_vid);
	return OK;
}
ReturnStatus TG_AdjacencyMatrixPageIterator::GetNextAdjList (TG_AdjacencyListIterator& iter) {
	if(mBuffer == NULL) {
		std::cout << "TG_AdjacencyMatrixPageIterator::GetNextAdjList() - Invalid page." << std::endl;
		return FAIL;
	}
	node_t past_lid = mCurrentLid;

	node_t vid;
	for(mCurrentSlotIdx = mCurrentSlotIdx + 1; mCurrentSlotIdx < mNumSlots ; mCurrentSlotIdx++) {
		vid = mBuffer->GetSlot(mCurrentSlotIdx)->src_vid;
		node_t lid = LocalStatistics::Vid2Lid(vid);

		if(true) {
			mCurrentLid = lid;
			break;
		}
	}
	if(mCurrentSlotIdx == mNumSlots)
		return DONE;
	if(mCurrentSlotIdx == 0)
		iter.Init(mCurrentLid, vid, (node_t*) &(*mBuffer)[0], mBuffer->GetSlot(mCurrentSlotIdx)->end_offset);
	else {
		int64_t past_offset = mBuffer->GetSlot(mCurrentSlotIdx-1)->end_offset;
		int64_t cur_offset = mBuffer->GetSlot(mCurrentSlotIdx)->end_offset;
		iter.Init(mCurrentLid, vid, (node_t*) &(*mBuffer)[past_offset], cur_offset - past_offset);
	}
	return OK;
}

ReturnStatus TG_AdjacencyMatrixPageIterator::PrintStat (BitMap<node_t>& ActiveNodes) {
    int64_t num_active_nodes = ActiveNodes.GetTotalInRangeSingleThread(PageFirstLid, PageLastLid);
    int64_t num_active_nodes_in_page = 0;
    for (int i = 0; i < mNumSlots; i++) {
        if (ActiveNodes.Get(LocalStatistics::Vid2Lid(mBuffer->GetSlot(i)->src_vid))) 
            num_active_nodes_in_page++;
    }
    fprintf(stdout, "Page %d num_active_nodes = %ld, num_active_nodes_in_page = %ld, numSlots = %ld, nan / nanip = %.3f, nanip / ns = %.3f\n", mCurrentPid, num_active_nodes, num_active_nodes_in_page, mNumSlots, (double)num_active_nodes / num_active_nodes_in_page, (double)num_active_nodes_in_page / mNumSlots);
}

ReturnStatus TG_AdjacencyMatrixPageIterator::GetNextAdjList2 (BitMap<node_t>& ActiveNodes, TG_AdjacencyListIterator& iter) {
#ifdef PERFORMANCE
	ALWAYS_ASSERT (mCurrentPid != -1 && mBuffer != NULL);
#else
	if(mCurrentPid == -1 || mBuffer == NULL) {
		std::cout << "TG_AdjacencyMatrixPageIterator::GetNextAdjList() - Invalid page." << std::endl;
		return FAIL;
	}
#endif
    while (true) {
        node_t lid = ActiveNodes.FindFirstMarkedEntry(PageFirstLid, PageLastLid);
        node_t vid_target = lid + PartitionStatistics::my_first_internal_vid();
        if (lid != -1) {
            mCurrentLid = lid;
            PageFirstLid = lid + 1;
            int64_t SlotIdx = mBuffer->FindSlotBinary(mCurrentSlotIdx + 1, mNumSlots - 1, vid_target);
            if (SlotIdx == -1) continue;
            if (SlotIdx == 0)
                iter.Init(mCurrentLid, vid_target, (node_t*) &(*mBuffer)[0], mBuffer->GetSlot(SlotIdx)->end_offset);
            else {
                int64_t past_offset = mBuffer->GetSlot(SlotIdx-1)->end_offset;
                int64_t cur_offset = mBuffer->GetSlot(SlotIdx)->end_offset;
                iter.Init(mCurrentLid, vid_target, (node_t*) &(*mBuffer)[past_offset], cur_offset - past_offset);
            }
            mCurrentSlotIdx = SlotIdx;
            return OK;
        } else {
            return DONE;
        }
    }
}

ReturnStatus TG_AdjacencyMatrixPageIterator::GetNextAdjList (BitMap<node_t>& ActiveNodes, TG_AdjacencyListIterator& iter) {
#ifdef PERFORMANCE
	ALWAYS_ASSERT (mCurrentPid != -1 && mBuffer != NULL);
#else
	if(mCurrentPid == -1 || mBuffer == NULL) {
		std::cout << "TG_AdjacencyMatrixPageIterator::GetNextAdjList() - Invalid page." << std::endl;
		return FAIL;
	}
#endif
	node_t past_lid = mCurrentLid;

	node_t vid;
	for(mCurrentSlotIdx = mCurrentSlotIdx + 1; mCurrentSlotIdx < mNumSlots ; mCurrentSlotIdx++) {
		vid = mBuffer->GetSlot(mCurrentSlotIdx)->src_vid;
		node_t lid = vid - FirstVid;

		if(ActiveNodes.Get(lid)) {
			mCurrentLid = lid;
			break;
		}
	}
	if(mCurrentSlotIdx == mNumSlots) return DONE;
	if(mCurrentSlotIdx == 0)
		iter.Init(mCurrentLid, vid, (node_t*) &(*mBuffer)[0], mBuffer->GetSlot(mCurrentSlotIdx)->end_offset);
	else {
		int64_t past_offset = mBuffer->GetSlot(mCurrentSlotIdx-1)->end_offset;
		int64_t cur_offset = mBuffer->GetSlot(mCurrentSlotIdx)->end_offset;
		iter.Init(mCurrentLid, vid, (node_t*) &(*mBuffer)[past_offset], cur_offset - past_offset);
	}
	return OK;
}

// 2-way Set Intersection with the partial order constraint
int64_t TriangleIntersection_Hybrid(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, Range<int32_t>* binary_splitting_idx, std::function<bool(node_t, node_t)> CheckPartialOrder) { //v1 - ext, v2 - int
	int32_t v1_idx=0, v2_idx=0, v1_cur_sz, v2_cur_sz;;
	int64_t triangle_cnt_delta=0;
	int binary_splitting_idx_size=0;
	Range<int32_t> v1_range (0, v1_sz), v2_range (0, v2_sz); //start - inclusive, end - exclusive

	while(1) {
		v1_cur_sz=v1_range.end - v1_range.begin;
		v2_cur_sz=v2_range.end - v2_range.begin;

		if(v1_cur_sz == 0 || v2_cur_sz == 0) {
			if(binary_splitting_idx_size == 0) break;

			binary_splitting_idx_size-=2;
			v1_range = binary_splitting_idx[binary_splitting_idx_size];
			v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
			continue;
		}

		if(v1_cur_sz < v2_cur_sz) {
			if(v2_cur_sz < BINARY_SPLITTING_THRESHOLD || v2_cur_sz < BINARY_SPLITTING_MAGNITUDE * v1_cur_sz) {
				triangle_cnt_delta+=TriangleIntersection(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, NULL, CheckPartialOrder);
				if(binary_splitting_idx_size == 0) break;

				binary_splitting_idx_size-=2;
				v1_range = binary_splitting_idx[binary_splitting_idx_size];
				v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
				continue;
			}

			v1_idx=(v1_range.begin+v1_range.end)/2;
			v2_idx=std::lower_bound(v2+v2_range.begin, v2+v2_range.end, v1[v1_idx])-v2;
			if(v2_idx == v2_range.end) {
				v1_range.end=v1_idx;
				v2_range.end=v2_idx;
				continue;
			}

		} else {
			if(v1_cur_sz < BINARY_SPLITTING_THRESHOLD || v1_cur_sz < BINARY_SPLITTING_MAGNITUDE * v2_cur_sz) {
				triangle_cnt_delta+=TriangleIntersection(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, NULL, CheckPartialOrder);
				if(binary_splitting_idx_size == 0) break;

				binary_splitting_idx_size-=2;
				v1_range = binary_splitting_idx[binary_splitting_idx_size];
				v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
				continue;
			}

			v2_idx=(v2_range.begin+v2_range.end)/2;
			v1_idx=std::lower_bound(v1+v1_range.begin, v1+v1_range.end, v2[v2_idx])-v1;
			if(v1_idx == v1_range.end) {
				v1_range.end=v1_idx;
				v2_range.end=v2_idx;
				continue;
			}
		}

		if(binary_splitting_idx_size == MAXIMUM_BINARY_SPLITTING_IDX_SIZE) {
			triangle_cnt_delta+=TriangleIntersection(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, NULL, CheckPartialOrder);

			if(binary_splitting_idx_size == 0) break;

			binary_splitting_idx_size-=2;
			v1_range = binary_splitting_idx[binary_splitting_idx_size];
			v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
			continue;
		}


		if(v1[v1_idx] == v2[v2_idx]) {
			if(CheckPartialOrder(v2_vid, v2[v2_idx])) { // nbr_vid > 3rd node vid
				triangle_cnt_delta++;

				binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx+1, v1_range.end);
				binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx+1, v2_range.end);
			}
			v1_range.end=v1_idx;
			v2_range.end=v2_idx;
		} else {
			if(CheckPartialOrder(v2_vid, v1[v1_idx])) { // nbr_vid > 3rd node vid
				// v1_idx is lower_bound so v1[v1_idx] > v2[v2_idx]

				if(v1_cur_sz < v2_cur_sz) {
					binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx+1, v1_range.end);
					binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx, v2_range.end);
				} else {
					binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx, v1_range.end);
					binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx+1, v2_range.end);
				}
			}
			v1_range.end=v1_idx;
			v2_range.end=v2_idx;
		}

	}
	return triangle_cnt_delta;
}

int64_t TriangleIntersection(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, Range<int32_t>* binary_splitting_idx, std::function<bool(node_t, node_t)> CheckPartialOrder) { //v1 - ext, v2 - int
	int64_t triangle_cnt_delta=0;
	// Intersection

	// Case 1:: MIN(n1+n2, n1log(n2), n2log(n1)) = n1+n2
	int strategy=0;

	// if a > b, alogb > bloga
	if(v1_sz+v2_sz > nlogm(v1_sz, v2_sz)) {
		if(nlogm(v1_sz, v2_sz) > nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;
		else if(v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=1;
	} else if(v1_sz+v2_sz > 10*nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;
    //if (v1_vid == 10413938 && v2_vid == 13) {
    /*if (v1_vid == 13 && v2_vid == 10413938) {
        fprintf(stdout, "v1 %ld, v2 %ld, strategy = %d\n", v1_vid, v2_vid, strategy);
        std::string to_print = "v1(" + std::to_string(v1_vid) + "): ";
        for (int64_t i = 0; i < v1_sz; i++) to_print += (std::to_string(v1[i]) + " ");
        to_print += "\n";
        to_print += "v2(" + std::to_string(v2_vid) + "): ";
        for (int64_t i = 0; i < v2_sz; i++) to_print += (std::to_string(v2[i]) + " ");
        to_print += "\n";
        fprintf(stdout, "%s", to_print.c_str());
    }*/
	
    //strategy=0;

	int32_t v2_idx=0, v1_idx=0;
	if(strategy == 0) { //v1+v2
		int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
		bool is_satisfy_po=true;
		while(v1_idx < v1_end && v2_idx < v2_end) {
			if( v1[v1_idx] == v2[v2_idx] ) {
				if( CheckPartialOrder(v2_vid, v1[v1_idx]) ) triangle_cnt_delta++;
				else {
					is_satisfy_po=false;
					break;
				}
			} else if( v1[v1_idx] == v2[v2_idx+1] ) {
				if( CheckPartialOrder(v2_vid, v2[v2_idx+1]) ) triangle_cnt_delta++;
				else {
					is_satisfy_po=false;
					break;
				}

				v2_idx+=2;
				continue;
			} else if( v1[v1_idx+1] == v2[v2_idx] ) {
				if( CheckPartialOrder(v2_vid, v1[v1_idx+1]) ) triangle_cnt_delta++;
				else {
					is_satisfy_po=false;
					break;
				}

				v1_idx+=2;
				continue;
			}

			if( v1[v1_idx+1] == v2[v2_idx+1] ) {
				if( CheckPartialOrder(v2_vid, v1[v1_idx+1]) ) triangle_cnt_delta++;
				else {
					is_satisfy_po=false;
					break;
				}

				v1_idx+=2;
				v2_idx+=2;
			} else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
				if(!CheckPartialOrder(v2_vid, v2[v2_idx+1])) {
					is_satisfy_po=false;
					break;
				}

				v2_idx+=2;
			} else {
				if(!CheckPartialOrder(v2_vid, v1[v1_idx+1])) {
					is_satisfy_po=false;
					break;
				}

				v1_idx+=2;
			}
		}
		if(is_satisfy_po) {
			while( v1_idx < v1_sz && v2_idx < v2_sz ) {
				if(v1[v1_idx] == v2[v2_idx] ) {
					if(CheckPartialOrder(v2_vid, v1[v1_idx])) // nbr_vid > 3rd node vid
						triangle_cnt_delta++;
					else break;
					v1_idx++;
					v2_idx++;
				} else if(v2[v2_idx] < v1[v1_idx] ) {
					if(!CheckPartialOrder(v2_vid, v1[v1_idx]))
						break;

					v2_idx++;
				} else {
					if(!CheckPartialOrder(v2_vid, v2[v2_idx]))
						break;

					v1_idx++;
				}
			}
		}
	} else if(strategy == 1) { //v1*log(v2)
		if(binary_splitting_idx != NULL && v2_sz > BINARY_SPLITTING_THRESHOLD && v2_sz > v1_sz * BINARY_SPLITTING_MAGNITUDE) {
			triangle_cnt_delta+=TriangleIntersection_Hybrid(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, binary_splitting_idx, CheckPartialOrder);
		} else {
			v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
			while( v2_idx < v2_sz ) {

				if(CheckPartialOrder(v2_vid, v1[v1_idx])) { // nbr_vid > 3rd node vid
					if( v2[v2_idx] == v1[v1_idx] )
						triangle_cnt_delta++;
				} else break;

				v1_idx++;
				if(v1_idx == v1_sz) break;
				v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
			}
		}
	} else { //v2*log(v1)
		if(binary_splitting_idx != NULL && v1_sz > BINARY_SPLITTING_THRESHOLD && v1_sz > v2_sz * BINARY_SPLITTING_MAGNITUDE) {
			triangle_cnt_delta+=TriangleIntersection_Hybrid(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, binary_splitting_idx, CheckPartialOrder);
		} else {
			//binary search
			v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
			while( v1_idx < v1_sz) {
				if(CheckPartialOrder(v2_vid, v1[v1_idx])) { // nbr_vid > 3rd node vid
					if( v2[v2_idx] == v2[v2_idx] )
						triangle_cnt_delta++;
				} else break;

				v2_idx++;
				if(v2_idx == v2_sz) break;

				//binary search
				v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
			}
		}
	}
	return triangle_cnt_delta;
}

int64_t TriangleIntersectionWithoutPartialOrder(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, Range<int32_t>* binary_splitting_idx, std::function<bool(node_t, node_t)> CheckPartialOrder) { //v1 - ext, v2 - int
	int64_t triangle_cnt_delta=0;
	// Intersection

	// Case 1:: MIN(n1+n2, n1log(n2), n2log(n1)) = n1+n2
	int strategy=0;

	// if a > b, alogb > bloga
	if(v1_sz+v2_sz > nlogm(v1_sz, v2_sz)) {
		if(nlogm(v1_sz, v2_sz) > nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;
		else if(v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=1;
	} else if(v1_sz+v2_sz > 10*nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;

	int32_t v2_idx=0, v1_idx=0;
	if(strategy == 0) { //v1+v2
		int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
		bool is_satisfy_po=true;
		while(v1_idx < v1_end && v2_idx < v2_end) {
			if( v1[v1_idx] == v2[v2_idx] ) {
				triangle_cnt_delta++;
			} else if( v1[v1_idx] == v2[v2_idx+1] ) {
				triangle_cnt_delta++;
				v2_idx+=2;
				continue;
			} else if( v1[v1_idx+1] == v2[v2_idx] ) {
				triangle_cnt_delta++;
				v1_idx+=2;
				continue;
			}

			if( v1[v1_idx+1] == v2[v2_idx+1] ) {
				triangle_cnt_delta++;
				v1_idx+=2;
				v2_idx+=2;
			} else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
				if(!CheckPartialOrder(v2_vid, v2[v2_idx+1])) {
					is_satisfy_po=false;
					break;
				}

				v2_idx+=2;
			} else {
				if(!CheckPartialOrder(v2_vid, v1[v1_idx+1])) {
					is_satisfy_po=false;
					break;
				}

				v1_idx+=2;
			}
		}
		if(is_satisfy_po) {
			while( v1_idx < v1_sz && v2_idx < v2_sz ) {
				if(v1[v1_idx] == v2[v2_idx] ) {
					if(CheckPartialOrder(v2_vid, v1[v1_idx])) // nbr_vid > 3rd node vid
						triangle_cnt_delta++;
					else break;
					v1_idx++;
					v2_idx++;
				} else if(v2[v2_idx] < v1[v1_idx] ) {
					if(!CheckPartialOrder(v2_vid, v1[v1_idx]))
						break;

					v2_idx++;
				} else {
					if(!CheckPartialOrder(v2_vid, v2[v2_idx]))
						break;

					v1_idx++;
				}
			}
		}
	} else if(strategy == 1) { //v1*log(v2)
		if(binary_splitting_idx != NULL && v2_sz > BINARY_SPLITTING_THRESHOLD && v2_sz > v1_sz * BINARY_SPLITTING_MAGNITUDE) {
			triangle_cnt_delta+=TriangleIntersection_Hybrid(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, binary_splitting_idx, CheckPartialOrder);
		} else {
			v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
			while( v2_idx < v2_sz ) {

				if(CheckPartialOrder(v2_vid, v1[v1_idx])) { // nbr_vid > 3rd node vid
					if( v2[v2_idx] == v1[v1_idx] )
						triangle_cnt_delta++;
				} else break;

				v1_idx++;
				if(v1_idx == v1_sz) break;
				v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
			}
		}
	} else { //v2*log(v1)
		if(binary_splitting_idx != NULL && v1_sz > BINARY_SPLITTING_THRESHOLD && v1_sz > v2_sz * BINARY_SPLITTING_MAGNITUDE) {
			triangle_cnt_delta+=TriangleIntersection_Hybrid(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, binary_splitting_idx, CheckPartialOrder);
		} else {
			//binary search
			v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
			while( v1_idx < v1_sz) {
				if(CheckPartialOrder(v2_vid, v1[v1_idx])) { // nbr_vid > 3rd node vid
					if( v2[v2_idx] == v2[v2_idx] )
						triangle_cnt_delta++;
				} else break;

				v2_idx++;
				if(v2_idx == v2_sz) break;

				//binary search
				v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
			}
		}
	}
	return triangle_cnt_delta;
}

int64_t CountNumberOfCommonNeighbors(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v2, node_t* v2_data, int64_t v2_sz, std::function<bool(node_t, node_t)> CheckPartialOrder) {
	if (v1_sz == 0 || v2_sz == 0) return 0;
    
    DummyUDFDataStructure ds;
	Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

	node_t v1_degreeorder;
	node_t v2_degreeorder;
	if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
		v1_degreeorder = v1;
		v2_degreeorder = v2;
	} else {
		v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
		v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
	}

	return TriangleIntersection(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, binary_splitting_idx, CheckPartialOrder);
}



int64_t IncrementalCountNumberOfCommonNeighbors(node_t v1, node_t* v1_data[], int64_t v1_sz[], node_t v2, node_t* v2_data[], int64_t v2_sz[], std::function<bool(node_t, node_t)> CheckPartialOrder) {
    int64_t cnts = 0;
            
    for (int delta_type = 0; delta_type <= 2; delta_type++) {

    }
}

