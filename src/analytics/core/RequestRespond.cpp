#include "analytics/core/RequestRespond.hpp"
#include "analytics/core/TurboDB.hpp"
#include "analytics/datastructure/MaterializedAdjacencyLists.hpp"

RequestRespond RequestRespond::rr_;
std::mutex RequestRespond::mutex_for_end;
bool RequestRespond::end = false;

per_thread_lazy<SimpleContainer> RequestRespond::per_thread_container_;
moodycamel::ConcurrentQueue<char*> RequestRespond::req_buffer_queue_;
moodycamel::ConcurrentQueue<SimpleContainer> RequestRespond::small_data_buffer_queue_;
moodycamel::ConcurrentQueue<SimpleContainer> RequestRespond::large_data_buffer_queue_;
moodycamel::ConcurrentQueue<TwoLevelBitMap<node_t>*> RequestRespond::temp_bitmap_queue_[2];
moodycamel::ConcurrentQueue<char*> RequestRespond::per_thread_buffer_queue_;
char* RequestRespond::per_thread_buffer_;
int64_t RequestRespond::per_thread_buffer_size_;
std::queue<std::future<void>> RequestRespond::reqs_to_wait_allreduce[2];

__thread int64_t RequestRespond::my_core_id_ = -1;
int64_t RequestRespond::core_counts_ = 0;

int64_t RequestRespond::total_request_buffer_size;
int64_t RequestRespond::max_request_size;
int64_t RequestRespond::num_request_buffer;
char* RequestRespond::request_buffer;

int64_t RequestRespond::total_data_buffer_size;
int64_t RequestRespond::small_data_size;
int64_t RequestRespond::large_data_size;
int64_t RequestRespond::num_small_data_buffer;
int64_t RequestRespond::num_large_data_buffer;
char* RequestRespond::data_buffer;

std::atomic<int64_t> RequestRespond::matadj_hit_bytes(0); // = std::atomic<int64_t>(0);
std::atomic<int64_t> RequestRespond::matadj_miss_bytes(0); // = std::atomic<int64_t>(0);

int64_t RequestRespond::max_permanent_buffer_size;
std::atomic<int64_t> RequestRespond::bytes_permanently_allocated(0);// = std::atomic<int64_t>(0);
int64_t RequestRespond::max_dynamic_buffer_size;
std::atomic<int64_t> RequestRespond::bytes_dynamically_allocated(0);// = std::atomic<int64_t>(0);

int64_t RequestRespond::max_dynamic_recv_buffer_size;
std::atomic<int64_t> RequestRespond::recv_buffer_bytes_dynamically_allocated(0); // = std::atomic<int64_t>(0);
int64_t RequestRespond::max_dynamic_send_buffer_size;
std::atomic<int64_t> RequestRespond::send_buffer_bytes_dynamically_allocated(0); // = std::atomic<int64_t>(0);

turbo_tcp RequestRespond::server_sockets;
turbo_tcp RequestRespond::client_sockets;
turbo_tcp RequestRespond::general_server_sockets[DEFAULT_NUM_GENERAL_TCP_CONNECTIONS];
turbo_tcp RequestRespond::general_client_sockets[DEFAULT_NUM_GENERAL_TCP_CONNECTIONS];
std::vector<bool> RequestRespond::general_connection_pool;

VariableSizedMemoryAllocatorWithCircularBuffer RequestRespond::circular_temp_data_buffer;
VariableSizedMemoryAllocatorWithCircularBuffer RequestRespond::circular_stream_send_buffer;
VariableSizedMemoryAllocatorWithCircularBuffer RequestRespond::circular_stream_recv_buffer;

void* TurboMalloc(int64_t sz) {
    //LOG_ASSERT(false);
	fprintf(stdout, "[TurboMalloc] Memory allocation %ld KB\n", sz / 1024);
retry:
	void* ptr = nullptr;

	int backoff = 1;
	while (RequestRespond::bytes_dynamically_allocated.fetch_add(sz) + sz > RequestRespond::GetMaxDynamicBufferSize()) {
		RequestRespond::bytes_dynamically_allocated.fetch_add(-sz);
		usleep(backoff * 1024);
		backoff= 2 * backoff;
		if (backoff >= 256) {
			backoff = 256;
		}
	}

	posix_memalign(&ptr, 64, sz);
	if (ptr == nullptr) {
		fprintf(stderr, "[TurboMalloc] Memory allocation failed\n");
	    abort();
    }
	ALWAYS_ASSERT (ptr != nullptr);
	return ptr;
}

void TurboFree(void* ptr, int64_t sz) {
	ALWAYS_ASSERT (ptr != nullptr);
	RequestRespond::bytes_dynamically_allocated.fetch_add(-sz);
	free(ptr);
}

void RequestRespond::InitializeCoreIds() {
    if (my_core_id_ == -1) {
        my_core_id_ = std::atomic_fetch_add( (std::atomic<int64_t>*) &(core_counts_), 1L);
    }
}

void RequestRespond::InitializeIoInterface() {
    InitializeCoreIds();
    if (RequestRespond::GetMyDiskIoInterface() == nullptr) {
        RequestRespond::GetMyDiskIoInterface() = new diskaio::DiskAioInterface*[2];
        int aio_tid = my_core_id_ % UserArguments::NUM_DISK_AIO_THREADS;
        //system_fprintf(0, stdout, "[%ld] [RequestRespond::InitializeIoInterface] tid = %ld, sid = %ld\n", PartitionStatistics::my_machine_id(), my_core_id_, socket_id);
        
        RequestRespond::GetMyDiskIoInterface()[READ_IO] = DiskAioFactory::GetPtr()->CreateAioInterface(PER_THREAD_MAXIMUM_ONGOING_DISK_AIO * 2, aio_tid);
        RequestRespond::GetMyDiskIoInterface()[WRITE_IO] = DiskAioFactory::GetPtr()->CreateAioInterface(PER_THREAD_MAXIMUM_ONGOING_DISK_AIO * 2, aio_tid);
    }
}

void RequestRespond::InitializeTempBitMapQueue(int64_t num_entries, int64_t num_buffers_for_sender, int64_t num_buffers_for_receiver) {
	for (int64_t i = 0; i < num_buffers_for_sender; i++) {
		TwoLevelBitMap<node_t>* tmp = new TwoLevelBitMap<node_t>();
		tmp->Init(num_entries);
		temp_bitmap_queue_[0].enqueue(tmp);
	}
    for (int64_t i = 0; i < num_buffers_for_receiver; i++) {
        TwoLevelBitMap<node_t>* tmp = new TwoLevelBitMap<node_t>();
        tmp->Init(num_entries);
        temp_bitmap_queue_[1].enqueue(tmp);
    }
}

void RequestRespond::SendSynchronously(int partition_id, char* buf, int64_t sz, int tag) {
    MPI_Ssend((void *) buf, sz, MPI_CHAR, partition_id, tag, MPI_COMM_WORLD);
    delete[] buf;
}

diskaio::DiskAioInterface**& RequestRespond::GetMyDiskIoInterface() {
    ALWAYS_ASSERT (my_core_id_ != -1);
    ALWAYS_ASSERT (my_core_id_ >= 0 && my_core_id_ < MAX_NUM_PER_THREAD_DATASTRUCTURE);
    return rr_.per_thread_aio_interface.get(my_core_id_);
}

diskaio::DiskAioInterface*& RequestRespond::GetMyDiskIoInterface(IOMode mode) {
    ALWAYS_ASSERT (my_core_id_ != -1);
    ALWAYS_ASSERT (my_core_id_ >= 0 && my_core_id_ < MAX_NUM_PER_THREAD_DATASTRUCTURE);
    return rr_.per_thread_aio_interface.get(my_core_id_)[mode];
}

char* RequestRespond::GetTempReqReceiveBuffer() {
	char* tmp_buffer = nullptr;
	int backoff = 1;
	while (!req_buffer_queue_.try_dequeue(tmp_buffer)) {
		//fprintf(stdout, "[RequestRespond] No More Receive Buffers; backoff %d\n", backoff * 32);
		usleep(backoff * 32);
		backoff= 2 * backoff;
		if (backoff >= 1024) backoff = 1024;
	}
	return tmp_buffer;
}

void RequestRespond::ReturnTempReqReceiveBuffer(char* tmp_buffer) {
	req_buffer_queue_.enqueue(tmp_buffer);
}

SimpleContainer RequestRespond::GetTempDataBuffer(int64_t req_size) {
    //if (req_size > (1024*1024)) fprintf(stdout, "[%ld][GetTempDataBuffer] alloc %ld KB\n", PartitionStatistics::my_machine_id(), req_size / 1024);
	SimpleContainer cont;
	if (req_size > max_dynamic_buffer_size) {
        void* ptr = TurboMalloc(req_size);
        cont.data = (char*) ptr;
        cont.capacity = req_size;
        ALWAYS_ASSERT (ptr != nullptr);
    } else if (req_size > large_data_size) {
        int backoff = 1;
        void* ptr = NULL;
        while ((ptr = (void*) circular_temp_data_buffer.Allocate(req_size)) == NULL) {
            usleep(backoff * 1024);
            backoff= 2 * backoff;
            if (backoff >= 256) backoff = 256;
        }
		INVARIANT (ptr != NULL);
        
        cont.data = (char*) ptr;
        cont.capacity = req_size;
        ALWAYS_ASSERT (ptr != nullptr);
	} else if (req_size > small_data_size) {
		int backoff = 1;
		while (!large_data_buffer_queue_.try_dequeue(cont)) {
			usleep(backoff * 32);
			backoff= 2 * backoff;
			if (backoff >= 4096) backoff = 4096;
			//if (backoff == 4096) fprintf(stdout, "A[%ld] Alloc TmpDataBuffer of size %ld\n", PartitionStatistics::my_machine_id(), req_size);
		}
	} else {
		int backoff = 1;
		while (!small_data_buffer_queue_.try_dequeue(cont)) {
			usleep(backoff * 32);
			backoff= 2 * backoff;
			if (backoff >= 4096) backoff = 4096;
			//if (backoff == 4096) fprintf(stdout, "B[%ld] Alloc TmpDataBuffer of size %ld\n", PartitionStatistics::my_machine_id(), req_size);
		}
	}
    //if (req_size > (1024*1024)) fprintf(stdout, "[%ld][GetTempDataBuffer] Memory allocation %ld KB; GOT IT\n", PartitionStatistics::my_machine_id(), req_size / 1024);
	ALWAYS_ASSERT (cont.capacity >= req_size);
	cont.size_used = 0;
//    fprintf(stdout, "[%ld] GetTempDataBuffer of size %ld cont.data %x\n", PartitionStatistics::my_machine_id(), req_size, cont.data);
	return cont;
}

void RequestRespond::ReturnTempDataBuffer(SimpleContainer cont) {
    //fprintf(stdout, "[%ld] Free TmpDataBuffer of size %ld\n", PartitionStatistics::my_machine_id(), cont.capacity);
	ALWAYS_ASSERT (cont.data != nullptr);
	if (cont.capacity > max_dynamic_buffer_size) {
		TurboFree( (void*) cont.data, cont.capacity);
    } else if (cont.capacity > large_data_size) {
		circular_temp_data_buffer.Free(cont.data, cont.capacity);
	} else if (cont.capacity > small_data_size) {
		ALWAYS_ASSERT (cont.capacity == large_data_size);
		large_data_buffer_queue_.enqueue(cont);
	} else {
		ALWAYS_ASSERT (cont.capacity == small_data_size);
		small_data_buffer_queue_.enqueue(cont);
	}
//    fprintf(stdout, "[%ld] ReturnTempDataBuffer of size %ld cont.data %x\n", PartitionStatistics::my_machine_id(), cont.capacity, cont.data);
	return;
}


TwoLevelBitMap<node_t>* RequestRespond::GetTempNodeBitMap(bool sender_or_receiver, int64_t backoff_limit) {
	TwoLevelBitMap<node_t>* tmp_buffer = NULL;
    int queue_idx = (sender_or_receiver == true) ? 0 : 1;
	int backoff = 1;
	int backoff_cnt = 0;
	while (!temp_bitmap_queue_[queue_idx].try_dequeue(tmp_buffer)) {
		usleep(backoff * 32);
		backoff= 2 * backoff;
		if (backoff >= 1024) {
			backoff = 1024;
			backoff_cnt++;
		}
		if (backoff_limit != -1 && backoff_cnt >= backoff_limit) {
			tmp_buffer = nullptr;
			break;
		}
	}
    //if (tmp_buffer != nullptr) fprintf(stdout, "[%ld] %ld [RequestRespond::GetTempNodeBitMap] (%ld) got %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, (int64_t) pthread_self(), tmp_buffer);
	return tmp_buffer;
}

void RequestRespond::ReturnTempNodeBitMap(bool sender_or_receiver, TwoLevelBitMap<node_t>* tmp_bitmap) {
    //fprintf(stdout, "[%ld] %ld [RequestRespond::ReturnTempNodeBitMap] (%ld) got %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, (int64_t) pthread_self(), tmp_bitmap);
    INVARIANT (tmp_bitmap != nullptr);
	tmp_bitmap->ClearAll();
    int queue_idx = (sender_or_receiver == true) ? 0 : 1;
	temp_bitmap_queue_[queue_idx].enqueue(tmp_bitmap);
}

void RequestRespond::RespondSequentialVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv) {
	TG_DistributedVectorBase::vectorID2vector(vectorID)->RespondSequentialVectorPull(chunkID, from, lv);
}
void RequestRespond::RespondInputVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv) {
	TG_DistributedVectorBase::vectorID2vector(vectorID)->RespondInputVectorPull(chunkID, from, lv);
}
void RequestRespond::RespondOutputVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv) {
	TG_DistributedVectorBase::vectorID2vector(vectorID)->RespondOutputVectorPull(chunkID, from, lv);
}

void RequestRespond::RespondOutputVectorWriteMessage(int32_t vectorID, int64_t fromChunkID, int64_t chunkID, int from, int tid, int idx, int64_t send_num, int64_t combined, int lv) {
	TG_DistributedVectorBase::vectorID2vector(vectorID)->RespondOutputVectorMessagePush(fromChunkID, chunkID, from, tid, idx, send_num, combined, lv);
}

void RequestRespond::RespondAdjListBatchIoFullListFromCache(int win_lv, AdjListRequestBatch* req, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, int64_t& bytes_served_from_memory, node_t& total_num_vids_cnts, node_t& num_vertices_served_from_memory, Range<int> version_range) {
    LOG_ASSERT(false);  // by SYKO at 2019/10/07: The function seems to be no longer used
}

void RequestRespond::RespondAdjListBatchIoFullList(AdjListRequestBatch* req, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
    LOG_ASSERT(false);  // by SYKO at 2019/10/04
}

void RequestRespond::RespondAdjListBatchIoFullListParallel(AdjListRequestBatch* req, Range<node_t> vid_range, Range<node_t> bitmap_vid_range, TwoLevelBitMap<node_t>* requested_vertices, TwoLevelBitMap<node_t>* vertices_from_cache, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, Range<int> version_range, EdgeType e_type, DynamicDBType d_type) {
    ALWAYS_ASSERT(UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
    // Create tasks and throw at AdjList Materialize Worker Pool
    TwoLevelBitMap<node_t>& vids = *requested_vertices;
    node_t bitmap_first_vid = bitmap_vid_range.GetBegin();
    //fprintf(stdout, "[%ld] vids total = %ld, (%ld in [%ld, %ld])\n", PartitionStatistics::my_machine_id(), vids.GetTotal(), vids.GetTotalInRange(vid_range.GetBegin(), vid_range.GetEnd()), vid_range.GetBegin(), vid_range.GetEnd());

    node_t total_num_vids_cnts = 0;
    node_t num_vertices_served_from_memory = 0;
    int64_t bytes_served_from_memory = 0;
    bool is_full_list = (req->dst_edge_partition_range.GetBegin() == 0 && req->dst_edge_partition_range.GetEnd() == PartitionStatistics::num_subchunks_per_edge_chunk() * PartitionStatistics::num_target_vector_chunks() - 1) ? true : false;
    if (is_full_list && UserArguments::ITERATOR_MODE == IteratorMode_t::PARTIAL_LIST) is_full_list = false;
    
    ALWAYS_ASSERT(is_full_list == (UserArguments::ITERATOR_MODE == IteratorMode_t::FULL_LIST));
    ALWAYS_ASSERT(bitmap_first_vid == PartitionStatistics::my_first_node_id());

    turbo_timer timer;

    node_t vid_cnts = 0;
    PageID page_cnts = 0;
    Range<node_t> ProcessedVertices(-1, -1);

    double partial_list_size_approx_factor = ((double) req->dst_edge_partition_range.length()) / (PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk());

    int64_t adjlist_data_size = 0;
    int64_t slot_data_size = 0;
    int64_t page_metadata_size = sizeof(node_t);
    int64_t size_to_request = 0;
    
    int64_t adjlist_data_size_cache = 0;
    int64_t slot_data_size_cache = 0;
    int64_t page_metadata_size_cache = sizeof(node_t);
    int64_t size_to_request_cache = 0;

    int64_t bytes_served = 0;
    int64_t bytes_served_from_disk = 0;

    int64_t alloc_size_for_CurPid = FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>::recursive_size_in_bytes(2, version_range.GetEnd() + 1, PartitionStatistics::my_total_num_subchunks());
    INVARIANT(alloc_size_for_CurPid < 4 * 1024 * 1024L);
    SimpleContainer container_CurPid = RequestRespond::GetTempDataBuffer(alloc_size_for_CurPid);
    SimpleContainer container_vertices_served_by_cache = RequestRespond::GetTempDataBuffer(DEFAULT_NIO_BUFFER_SIZE);
    
    FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>> CurPid(container_CurPid);
    FixedSizeVector<node_t> vertices_served_by_cache(container_vertices_served_by_cache);
       
    CurPid.recursive_resize(container_CurPid.data, 2, version_range.GetEnd() + 1, PartitionStatistics::my_total_num_subchunks());
    bool is_CurPid_initialized = false;
    bool use_fulllist_db = (TurboDB::GetTurboDB(e_type)->is_full_list_db_exist()) && UserArguments::USE_FULLIST_DB;
     
    std::list<std::future<void>> reqs_to_wait;

    timer.start_timer(0);
    
    timer.start_timer(1);
    int win_lv = 1 - 1;
    //VidToAdjacencyListMappingTable::VidToAdjTable.Lock(win_lv);
	VidToAdjacencyListMappingTable::VidToAdjTable.AcquireReference(win_lv, true);
    timer.stop_timer(1);
	
    timer.start_timer(2);
FAST_SCAN:
    timer.start_timer(9);
    Range<node_t> idx_range(vid_range.GetBegin() - bitmap_first_vid, vid_range.GetEnd() - bitmap_first_vid);
    node_t idx_begin = vids.FindFirstMarkedEntry(idx_range.GetBegin(), idx_range.GetEnd());

    if (idx_begin == -1) {
        idx_range.SetBegin(idx_range.GetEnd() + 1);
    } else {
        idx_range.SetBegin(idx_begin);
    }
    vid_range.Set(idx_range.GetBegin() + bitmap_first_vid, idx_range.GetEnd() + bitmap_first_vid);
    timer.stop_timer(9);
  
    timer.start_timer(10);
    // XXX - Optimize
    // How to use 'InvokeIfMarked'?
    //fprintf(stdout, "[%ld] vid_range [%ld, %ld] total %ld\n", PartitionStatistics::my_machine_id(), vid_range.GetBegin(), vid_range.GetEnd(), vids.GetTotalInRange(vid_range.GetBegin(), vid_range.GetEnd()));
    for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
        node_t lvid = vid - PartitionStatistics::my_first_node_id();
        if (!vids.BitMap<node_t>::Get(lvid)) continue;
        //node_t vid = lvid + PartitionStatistics::my_first_node_id();
#if USE_DEGREE_THRESHOLD
        if (UserArguments::ITERATOR_MODE == IteratorMode_t::FULL_LIST) {
            node_t prev_degree = TurboDB::OutDegree(vid) - TurboDB::OutInsertDegree(vid) + TurboDB::OutDeleteDegree(vid);
            node_t cur_degree = TurboDB::OutDegree(vid);
            //if (!(cur_degree > UserArguments::DEGREE_THRESHOLD || prev_degree > UserArguments::DEGREE_THRESHOLD)) {
            if (!((TurboDB::OutDegree(vid) + TurboDB::OutDeleteDegree(vid)) > UserArguments::DEGREE_THRESHOLD)) {
                vids.BitMap<node_t>::Clear(lvid);
                continue;
            }
        }
#endif
        
        timer.start_timer(12);
        if (!VidToAdjacencyListMappingTable::VidToAdjTable.Has(vid, win_lv)) {
            if (ProcessedVertices.GetBegin() != -1 && vid - ProcessedVertices.GetBegin() >= 96*1024L) {
                if (std::atomic_load((std::atomic<int64_t>*) stop_flag) == 1) {
                    break;
                }

                timer.start_timer(7);
                if (use_fulllist_db) FindFullListPageIdsToReadFrom(version_range, req->dst_edge_partition_range, vid_range, is_CurPid_initialized, e_type, d_type, CurPid, ProcessedVertices);
                else FindPageIdsToReadFrom(version_range, req->dst_edge_partition_range, vid_range, is_CurPid_initialized, e_type, d_type, CurPid, ProcessedVertices);
                is_CurPid_initialized = true;
                timer.stop_timer(7);

                timer.start_timer(14);
                INVARIANT(alloc_size_for_CurPid < 4 * 1024 * 1024L);
                SimpleContainer new_container_CurPid = RequestRespond::GetTempDataBuffer(alloc_size_for_CurPid);
                std::memcpy((void*) new_container_CurPid.data, (void*) container_CurPid.data, alloc_size_for_CurPid);

                MaterializedAdjacencyLists::LockBufferIfEnoughFreeFrames(page_cnts, true);
                MaterializedAdjacencyLists::IncrementNumUsedFrames(page_cnts);
                bytes_served_from_disk += TBGPP_PAGE_SIZE * page_cnts;
                //fprintf(stdout, "[%ld] (%p) MaterializeAndEnqueue A [%ld, %ld] %ld\n", PartitionStatistics::my_machine_id(), pthread_self(), ProcessedVertices.GetBegin(), ProcessedVertices.GetEnd(), size_to_request);
                reqs_to_wait.push_back(Aio_Helper::async_pool.enqueue(MaterializedAdjacencyLists::MaterializeAndEnqueue, req_data_queue, bitmap_vid_range, requested_vertices, ProcessedVertices, req->dst_edge_partition_range, req->from, req->lv, stop_flag, is_full_list, version_range, e_type, d_type, CurPid));
                MaterializedAdjacencyLists::UnlockBuffer();
                timer.stop_timer(14);

                container_CurPid = new_container_CurPid;
                CurPid.set_container(container_CurPid);
                CurPid.recursive_resize(container_CurPid.data, 2, version_range.GetEnd() + 1, PartitionStatistics::my_total_num_subchunks());

                ProcessedVertices.SetBegin(-1);
                total_num_vids_cnts += vid_cnts;
                page_cnts = 0;
                vid_cnts = 0;
                adjlist_data_size = 0;
                slot_data_size = 0;
            }
            
            
            timer.start_timer(13);
            vid_cnts++;
            if (ProcessedVertices.GetBegin() == -1) ProcessedVertices.SetBegin(vid);
            ProcessedVertices.SetEnd(vid);
            adjlist_data_size += sizeof(node_t) * (TurboDB::OutDegree(vid));
            slot_data_size += sizeof(Slot32);
            size_to_request = partial_list_size_approx_factor * (adjlist_data_size) + slot_data_size + page_metadata_size;
            timer.stop_timer(13);
            
            if (size_to_request < RequestRespond::small_data_size * 0.90 && ProcessedVertices.length() < 96*1024L) continue;
            if (std::atomic_load((std::atomic<int64_t>*) stop_flag) == 1) break;

            timer.start_timer(7);
            if (use_fulllist_db) FindFullListPageIdsToReadFrom(version_range, req->dst_edge_partition_range, vid_range, is_CurPid_initialized, e_type, d_type, CurPid, ProcessedVertices);
            else FindPageIdsToReadFrom(version_range, req->dst_edge_partition_range, vid_range, is_CurPid_initialized, e_type, d_type, CurPid, ProcessedVertices);
            is_CurPid_initialized = true;
            timer.stop_timer(7);
            
            timer.start_timer(14);
            INVARIANT(alloc_size_for_CurPid < 4 * 1024 * 1024L);
            SimpleContainer new_container_CurPid = RequestRespond::GetTempDataBuffer(alloc_size_for_CurPid);
            std::memcpy((void*) new_container_CurPid.data, (void*) container_CurPid.data, alloc_size_for_CurPid);

            MaterializedAdjacencyLists::LockBufferIfEnoughFreeFrames(page_cnts, true);
            MaterializedAdjacencyLists::IncrementNumUsedFrames(page_cnts);
            bytes_served_from_disk += TBGPP_PAGE_SIZE * page_cnts;
            //fprintf(stdout, "[%ld] (%p) MaterializeAndEnqueue B [%ld, %ld] %ld\n", PartitionStatistics::my_machine_id(), pthread_self(), ProcessedVertices.GetBegin(), ProcessedVertices.GetEnd(), size_to_request);
            reqs_to_wait.push_back(Aio_Helper::async_pool.enqueue(MaterializedAdjacencyLists::MaterializeAndEnqueue, req_data_queue, bitmap_vid_range, requested_vertices, ProcessedVertices, req->dst_edge_partition_range, req->from, req->lv, stop_flag, is_full_list, version_range, e_type, d_type, CurPid));
            MaterializedAdjacencyLists::UnlockBuffer();
            timer.stop_timer(14);

            container_CurPid = new_container_CurPid;
            CurPid.set_container(container_CurPid);
            CurPid.recursive_resize(container_CurPid.data, 2, version_range.GetEnd() + 1, PartitionStatistics::my_total_num_subchunks());

            ProcessedVertices.SetBegin(-1);
            total_num_vids_cnts += vid_cnts;
            page_cnts = 0;
            vid_cnts = 0;
            adjlist_data_size = 0;
            slot_data_size = 0;

            timer.start_timer(11);
            while (reqs_to_wait.size() > MAT_ADJ_MAX_PENDING_REQS) {
                reqs_to_wait.remove_if([&](std::future<void>& wait) { 
                    return wait.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
                });
            }
            timer.stop_timer(11);
        } else {
            timer.start_timer(15);
			vertices_served_by_cache.push_back(vid);
            vids.BitMap<node_t>::Clear(lvid);
            vertices_from_cache->TwoLevelBitMap<node_t>::Set(lvid);
			
            AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(vid, win_lv);
			MaterializedAdjacencyLists* adj_ptr = (MaterializedAdjacencyLists*) ptr.ptr;
			int64_t adjlist_data_sz = adj_ptr->GetNumEntries(ptr.slot_id) * sizeof(node_t);
			ALWAYS_ASSERT (adjlist_data_sz != 0);	// Assuming undirected graph.
            adjlist_data_size_cache += adjlist_data_sz;
			slot_data_size_cache += sizeof(Slot32);
            size_to_request_cache = adjlist_data_size_cache + slot_data_size_cache + page_metadata_size_cache;
            timer.stop_timer(15);
            
            if (size_to_request_cache < RequestRespond::small_data_size * 0.95 && !vertices_served_by_cache.full()) continue;

            timer.start_timer(16);
			SimpleContainer cont = RequestRespond::GetDataSendBuffer(size_to_request_cache);
			MaterializedAdjacencyLists mat_adj;
			mat_adj.Initialize(size_to_request_cache, cont.capacity, cont.data);
            timer.stop_timer(16);

            timer.start_timer(6);
            CopyMaterializedAdjListsFromCache(mat_adj, cont, adjlist_data_size_cache, slot_data_size_cache, vertices_served_by_cache, vid_range, size_to_request_cache);
            timer.stop_timer(6);
            
            ALWAYS_ASSERT(mat_adj.GetNumAdjacencyLists() == slot_data_size_cache / sizeof(Slot32));
            ALWAYS_ASSERT(mat_adj.GetNumEdges() == adjlist_data_size_cache / sizeof(node_t));
            ALWAYS_ASSERT(mat_adj.GetNumAdjacencyLists() != 0 && mat_adj.GetNumEdges() != 0);
            ALWAYS_ASSERT(vid_range.contains(mat_adj.GetSrcVidRange()));

            timer.start_timer(17);
            // Add request to request queue
			UdfSendRequest udf_req;
			udf_req.req.rt = UserCallback;
			udf_req.req.from = PartitionStatistics::my_machine_id();
			udf_req.req.to = req->from;
			udf_req.req.parm1 = 1;  // Already sorted by degree order
			udf_req.req.data_size = mat_adj.Size();
			udf_req.cont = mat_adj.GetContainer();
			udf_req.req.vid_range.Set(vertices_served_by_cache.front(), vertices_served_by_cache.back());
			udf_req.req.lv = req->lv;
            timer.stop_timer(17);

            timer.start_timer(8);
            //MaterializedAdjacencyLists::ClearVerticesIntoBitMap(udf_req, requested_vertices, bitmap_first_vid);
            req_data_queue->push(udf_req);
            timer.stop_timer(8);
			
            timer.start_timer(18);
			num_vertices_served_from_memory += vertices_served_by_cache.size();
			adjlist_data_size_cache = 0;
			slot_data_size_cache = 0;
            vertices_served_by_cache.clear();
            timer.stop_timer(18);
            
            bytes_served_from_memory += size_to_request_cache;
            if (std::atomic_load((std::atomic<int64_t>*) stop_flag) == 1) break;
        }
        timer.stop_timer(12);
    }
    timer.stop_timer(10);
    timer.stop_timer(2);

    timer.start_timer(3);
    // Remaining vertices to materialize
    if (vid_cnts > 0 && std::atomic_load((std::atomic<int64_t>*) stop_flag) == 0) {
        timer.start_timer(7);
        if (use_fulllist_db) FindFullListPageIdsToReadFrom(version_range, req->dst_edge_partition_range, vid_range, is_CurPid_initialized, e_type, d_type, CurPid, ProcessedVertices);
        else FindPageIdsToReadFrom(version_range, req->dst_edge_partition_range, vid_range, is_CurPid_initialized, e_type, d_type, CurPid, ProcessedVertices);
        timer.stop_timer(7);
        
        MaterializedAdjacencyLists::LockBufferIfEnoughFreeFrames(page_cnts, true);
        MaterializedAdjacencyLists::IncrementNumUsedFrames(page_cnts);
        bytes_served_from_disk += TBGPP_PAGE_SIZE * page_cnts;
        //fprintf(stdout, "[%ld] (%p) MaterializeAndEnqueue C [%ld, %ld] %ld\n", PartitionStatistics::my_machine_id(), pthread_self(), ProcessedVertices.GetBegin(), ProcessedVertices.GetEnd(), size_to_request);
        reqs_to_wait.push_back(Aio_Helper::async_pool.enqueue(MaterializedAdjacencyLists::MaterializeAndEnqueue, req_data_queue, bitmap_vid_range, requested_vertices, ProcessedVertices, req->dst_edge_partition_range, req->from, req->lv, stop_flag, is_full_list, version_range, e_type, d_type, CurPid));
        MaterializedAdjacencyLists::UnlockBuffer();
        container_CurPid.data = nullptr;
    }
    total_num_vids_cnts += vid_cnts;
    timer.stop_timer(3);
    
    timer.start_timer(4);
    if (vertices_served_by_cache.size() > 0) {
        SimpleContainer cont = RequestRespond::GetDataSendBuffer(size_to_request_cache);
        MaterializedAdjacencyLists mat_adj;
        mat_adj.Initialize(size_to_request_cache, cont.capacity, cont.data);

        timer.start_timer(6);
        CopyMaterializedAdjListsFromCache(mat_adj, cont, adjlist_data_size_cache, slot_data_size_cache, vertices_served_by_cache, vid_range, size_to_request_cache);
        timer.stop_timer(6);

        ALWAYS_ASSERT(mat_adj.GetNumAdjacencyLists() == slot_data_size_cache / sizeof(Slot32));
        ALWAYS_ASSERT(mat_adj.GetNumEdges() == adjlist_data_size_cache / sizeof(node_t));
        ALWAYS_ASSERT(mat_adj.GetNumAdjacencyLists() != 0 && mat_adj.GetNumEdges() != 0);
        ALWAYS_ASSERT(vid_range.contains(mat_adj.GetSrcVidRange()));

        // Add request to request queue
        UdfSendRequest udf_req;
        udf_req.req.rt = UserCallback;
        udf_req.req.from = PartitionStatistics::my_machine_id();
        udf_req.req.to = req->from;
        udf_req.req.parm1 = 1;  // Already sorted by degree order
        udf_req.req.data_size = mat_adj.Size();
        udf_req.cont = mat_adj.GetContainer();
        udf_req.req.vid_range.Set(vertices_served_by_cache.front(), vertices_served_by_cache.back());
        udf_req.req.lv = req->lv;

        timer.start_timer(8);
        //MaterializedAdjacencyLists::ClearVerticesIntoBitMap(udf_req, requested_vertices, bitmap_first_vid);
        req_data_queue->push(udf_req);
        timer.stop_timer(8);

        num_vertices_served_from_memory += vertices_served_by_cache.size();
        bytes_served_from_memory += size_to_request_cache;
        vertices_served_by_cache.clear();
    }
    total_num_vids_cnts += num_vertices_served_from_memory;

    VidToAdjacencyListMappingTable::VidToAdjTable.ReleaseReference(win_lv);
    //VidToAdjacencyListMappingTable::VidToAdjTable.Unlock(win_lv);
    RequestRespond::matadj_hit_bytes += bytes_served_from_memory;
    timer.stop_timer(4);

    timer.start_timer(5);
    while (!reqs_to_wait.empty()) {
        reqs_to_wait.remove_if([&](std::future<void>& wait) { 
            return wait.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        });
    }
    ALWAYS_ASSERT(reqs_to_wait.empty());
    timer.stop_timer(5);
    timer.stop_timer(0);
    
    RequestRespond::matadj_miss_bytes += bytes_served_from_disk;
    if (container_CurPid.data != nullptr) RequestRespond::ReturnTempDataBuffer(container_CurPid);
    RequestRespond::ReturnTempDataBuffer(container_vertices_served_by_cache);

#ifdef REPORT_PROFILING_TIMERS
    fprintf(stdout, "[%ld] %ld [RespondAdjListBatchIoFullList] %lld->%lld, %lld in (idx_begin = %ld) [%lld, %lld], %ld from cache, %ld from disk // %.6f = %.2f + %.2f + %.2f + %.2f + %.2f // %.2f %.2f %.2f // %.3f = %.3f + %.3f + %.3f + %.3f %.3f %.3f %.3f %.3f %.3f %.3f\n", req->from, UserArguments::UPDATE_VERSION, req->from, req->to, total_num_vids_cnts, (int64_t) idx_begin, (int64_t) vid_range.GetBegin(), (int64_t) vid_range.GetEnd(), num_vertices_served_from_memory, total_num_vids_cnts - num_vertices_served_from_memory, 
            timer.get_timer(0), timer.get_timer(1), timer.get_timer(2), timer.get_timer(3), timer.get_timer(4), timer.get_timer(5), 
            timer.get_timer(6), timer.get_timer(7), timer.get_timer(8), 
            timer.get_timer(2), timer.get_timer(9), timer.get_timer(10), timer.get_timer(11), timer.get_timer(12), 
            timer.get_timer(13), timer.get_timer(14), timer.get_timer(15), timer.get_timer(16), timer.get_timer(17), timer.get_timer(18));
#endif
}

int64_t RequestRespond::ComputeMaterializedAdjListsSize(Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, int64_t MaxBytesToSend) {
	double partial_list_size_approx_factor = ((double) dst_edge_partition_range.length()) / (PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk());
    BitMap<node_t>& vids = *requested_vertices;

    node_t v_cnts = 0;
	int64_t total_bytes = 0;
	int64_t accm_page_metadata_size = sizeof(node_t);
    int64_t num_max_slots = 0.90 * (DEFAULT_NIO_BUFFER_SIZE) / sizeof(Slot32);
    
    //fprintf(stdout, "[%ld] %ld [ComputeMaterializedAdjListsSize] vid in [%ld, %ld], MaxBytes = %ld, %.4f, sizeof(Slot32) = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, vid_range.GetBegin(), vid_range.GetEnd(), MaxBytesToSend, partial_list_size_approx_factor, sizeof(Slot32));
    int64_t adj_list_size_temp = 0;
    int64_t slot_size_temp = 0;
    int64_t page_mete_temp = 0;

    struct PerThreadAccumulator {
        int64_t v_cnts = 0;
        int64_t total_bytes = 0;
        int64_t accm_adjlist_data_size = 0;
        int64_t accm_slot_data_size = 0;
        CACHE_PADOUT;
    };

    PerThreadAccumulator PerThreadAccm[MAX_NUM_CPU_CORES];

    vids.InvokeIfMarked([&](node_t lvid) {
        int tid = omp_get_thread_num();
        PerThreadAccm[tid].v_cnts++;
        
        node_t v = lvid + PartitionStatistics::my_first_node_id();
        int64_t adjlist_data_sz;
        
        if (UserArguments::USE_DELTA_NWSM && UserArguments::UPDATE_VERSION >= 1) {
            adjlist_data_sz = (TurboDB::OutDegree(v) - TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v) + TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v))* sizeof(node_t);
        } else {
            adjlist_data_sz = TurboDB::OutDegree(v) * sizeof(node_t);
        }
		INVARIANT (!((adjlist_data_sz + sizeof(Slot32) + accm_page_metadata_size) > MaxBytesToSend));


        std::atomic_fetch_add((std::atomic<int64_t>*) &adj_list_size_temp, +adjlist_data_sz);
        std::atomic_fetch_add((std::atomic<int64_t>*) &slot_size_temp, (int64_t) sizeof(Slot32));
		int64_t size_to_request = std::ceil(partial_list_size_approx_factor * (PerThreadAccm[tid].accm_adjlist_data_size + adjlist_data_sz)) + PerThreadAccm[tid].accm_slot_data_size + sizeof(Slot32) + accm_page_metadata_size;

		if (size_to_request > RequestRespond::small_data_size * 0.95 || PerThreadAccm[tid].v_cnts % num_max_slots == 0) {
            //if (UserArguments::UPDATE_VERSION >= 1 && PartitionStatistics::my_machine_id() == 0)
            //    fprintf(stdout, "[%ld] tid %d, size_to_request = %ld >? small_data_size * 0.95 = %.2f, v_cnts = %ld, num_max_slots = %ld, accm_adjlist_data_size = %ld, accm_slot_data_size = %ld\n", PartitionStatistics::my_machine_id(), tid, size_to_request, RequestRespond::small_data_size * 0.95, PerThreadAccm[tid].v_cnts, num_max_slots, PerThreadAccm[tid].accm_adjlist_data_size, PerThreadAccm[tid].accm_slot_data_size);
			PerThreadAccm[tid].accm_adjlist_data_size = 0;
			PerThreadAccm[tid].accm_slot_data_size = 0;
			PerThreadAccm[tid].total_bytes += size_to_request;
            std::atomic_fetch_add((std::atomic<int64_t>*) &page_mete_temp, +8L);
		} else {
			PerThreadAccm[tid].accm_adjlist_data_size += adjlist_data_sz;
			PerThreadAccm[tid].accm_slot_data_size += sizeof(Slot32);
		}
    });
    
    for (int tid = 0; tid < MAX_NUM_CPU_CORES; tid++) {
        if (PerThreadAccm[tid].accm_slot_data_size > 0) {
            int64_t size_to_request = std::ceil(partial_list_size_approx_factor * PerThreadAccm[tid].accm_adjlist_data_size) + PerThreadAccm[tid].accm_slot_data_size + accm_page_metadata_size;
            PerThreadAccm[tid].total_bytes += size_to_request;
            std::atomic_fetch_add((std::atomic<int64_t>*) &page_mete_temp, +8L);
        }
        total_bytes += PerThreadAccm[tid].total_bytes;
        v_cnts += PerThreadAccm[tid].v_cnts;
    }

    total_bytes = total_bytes * 1.01;
    if (total_bytes > MaxBytesToSend) {
        total_bytes = MaxBytesToSend;
    }

    /*
	int64_t accm_adjlist_data_size = 0;
	int64_t accm_slot_data_size = 0;
    bool overflow = false;
	for (node_t v = vid_range.GetBegin(); v <= vid_range.GetEnd(); v++) {
		node_t lvid = v - PartitionStatistics::my_first_node_id();
        if (!vids.Get(lvid)) continue;
		v_cnts++;
		
        int64_t adjlist_data_sz = -1;
        if (UserArguments::USE_DELTA_NWSM && UserArguments::UPDATE_VERSION >= 1) {
            adjlist_data_sz = (TurboDB::OutDegree(v) - TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v) + TurboDB::OutInsertDegree(v) + TurboDB::OutDeleteDegree(v))* sizeof(node_t);
        } else {
            adjlist_data_sz = TurboDB::OutDegree(v) * sizeof(node_t);
        }
		INVARIANT (!((adjlist_data_sz + sizeof(Slot32) + accm_page_metadata_size) > MaxBytesToSend));

		int64_t size_to_request = std::ceil(partial_list_size_approx_factor * (accm_adjlist_data_size + adjlist_data_sz)) + accm_slot_data_size + sizeof(Slot32) + accm_page_metadata_size;

		if (total_bytes + size_to_request > MaxBytesToSend) {
			accm_adjlist_data_size = 0;
			accm_slot_data_size = 0;
			overflow = true;
            break;
		}

		if (size_to_request > RequestRespond::small_data_size * 0.95 || v_cnts % num_max_slots == 0) {
			accm_adjlist_data_size = 0;
			accm_slot_data_size = 0;
			total_bytes += size_to_request;
		} else {
			accm_adjlist_data_size += adjlist_data_sz;
			accm_slot_data_size += sizeof(Slot32);
		}
	}
	
    if (accm_slot_data_size != 0) {
		int64_t size_to_request = std::ceil(partial_list_size_approx_factor * accm_adjlist_data_size) + accm_slot_data_size + accm_page_metadata_size;
		total_bytes += size_to_request;
	}
	INVARIANT (total_bytes <= MaxBytesToSend);

    if (overflow) {
        total_bytes = MaxBytesToSend;
    }
    */

	//fprintf(stdout, "[%ld] ComputeMaterializedAdjListsSize vid_range [%ld, %ld] %ld <= %ld, v_cnts = %ld (adjlist %ld slot %ld, page_meta %ld)\n", PartitionStatistics::my_machine_id(), vid_range.GetBegin(), vid_range.GetEnd(), total_bytes, MaxBytesToSend, v_cnts, adj_list_size_temp, slot_size_temp, page_mete_temp);
    
    return total_bytes;
}

void RequestRespond::FindPageIdsToReadFrom(Range<int> version_range, Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, bool is_initialized, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, Range<node_t> ProcessedVertices) {
    int begin_dbtype, end_dbtype;
    if (d_type == INSERT || d_type == ALL) begin_dbtype = 0;
    else begin_dbtype = 1;
    if (d_type == DELETE || d_type == ALL) end_dbtype = 2;
    else end_dbtype = 1;

    int64_t vid_range_begin = ProcessedVertices.GetBegin();
    int64_t vid_range_end = ProcessedVertices.GetEnd();
    turbo_timer timer;

    timer.start_timer(0);
    if (!is_initialized) {
        // TODO - Optimize
        // Maybe improved by using "omp for collapse" ?
        for (int dbtype = begin_dbtype; dbtype < end_dbtype; dbtype++) {
            //fprintf(stdout, "CurPid[%ld] is resized as %ld\n", dbtype, version_range.length());
            ALWAYS_ASSERT(version_range.GetEnd() >= 0);
            //CurPid[dbtype].resize(version_range.GetEnd() + 1);
            ALWAYS_ASSERT(CurPid[dbtype].size() == version_range.GetEnd() + 1);
            for (int64_t version_id = version_range.GetBegin(); version_id <= version_range.GetEnd(); version_id++) {
                CurPid[dbtype][version_id].set(-1);
                //CurPid[dbtype][version_id].resize(PartitionStatistics::my_total_num_subchunks(), -1);
                ALWAYS_ASSERT(CurPid[dbtype][version_id].size() == PartitionStatistics::my_total_num_subchunks());
                if (!TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType) dbtype, version_id)) continue;
                for (PartitionID i = dst_edge_partition_range.GetBegin(); i <= dst_edge_partition_range.GetEnd(); i++) {
                    VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version_id, (DynamicDBType)dbtype);
                    if (CurPid[dbtype][version_id][i] == -1 && version_id == 0) {
                        CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageByVidRange(i, ProcessedVertices, version_id, e_type, (DynamicDBType)dbtype, false);
                    } else if (CurPid[dbtype][version_id][i] == -1 && version_id > 0) {
                        CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageId(i, version_id, (DynamicDBType)dbtype);
                        while (CurPid[dbtype][version_id][i] <= TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version_id, (DynamicDBType)dbtype)) {
                            if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).contains(vid_range_begin)) break;
                            else if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).GetBegin() > vid_range_begin) break;
                            else CurPid[dbtype][version_id][i]++;
                        }
                    }
                }
            }
        }
        
    } else {
        // TODO - Optimize
        // Maybe improved by using "omp for collapse" ?
        for (int dbtype = begin_dbtype; dbtype < end_dbtype; dbtype++) {
            for (int64_t version_id = version_range.GetBegin(); version_id <= version_range.GetEnd(); version_id++) {
                if (!TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType) dbtype, version_id)) continue;
                for (PartitionID i = dst_edge_partition_range.GetBegin(); i <= dst_edge_partition_range.GetEnd(); i++) {
                    VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPage(version_id, (DynamicDBType)dbtype);
                    if (CurPid[dbtype][version_id][i] == -1 && version_id == 0) {
                        timer.start_timer(1);
                        CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageByVidRange(i, ProcessedVertices, version_id, e_type, (DynamicDBType)dbtype, false);
                        timer.stop_timer(1);
                        continue;
                    } else if (CurPid[dbtype][version_id][i] == -1 && version_id > 0) {
                        CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageId(i, version_id, (DynamicDBType)dbtype);
                    }
                    timer.start_timer(3);
                    while (CurPid[dbtype][version_id][i] <= TurboDB::GetTurboDB(e_type)->GetLastPageId(i, version_id, (DynamicDBType)dbtype)) {
                        if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).contains(vid_range_begin)) break;
                        else if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).GetBegin() > vid_range_begin) break;
                        else CurPid[dbtype][version_id][i]++;
                    }
                    timer.stop_timer(3);
                }
            }
        } 
    }
    timer.stop_timer(0);
    //fprintf(stdout, "[%ld] FindPageIdsToReadFrom initialized? %s, %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), is_initialized ? "yes" : "no", timer.get_timer(0), timer.get_timer(1), timer.get_timer(2), timer.get_timer(3));
}

void RequestRespond::FindFullListPageIdsToReadFrom(Range<int> version_range, Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, bool is_initialized, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, Range<node_t> ProcessedVertices) {
    int begin_dbtype, end_dbtype;
    if (d_type == INSERT || d_type == ALL) begin_dbtype = 0;
    else begin_dbtype = 1;
    if (d_type == DELETE || d_type == ALL) end_dbtype = 2;
    else end_dbtype = 1;

    if (!is_initialized) {
        for (int dbtype = begin_dbtype; dbtype < end_dbtype; dbtype++) {
            //CurPid[dbtype].resize(version_range.length());
            for (int64_t version_id = version_range.GetBegin(); version_id <= version_range.GetEnd(); version_id++) {
                CurPid[dbtype][version_id].set(-1);
                //CurPid[dbtype][version_id - version_range.GetBegin()].resize(1, -1);
                if (!TurboDB::GetTurboDB(e_type)->is_version_exist_full_list((DynamicDBType)dbtype, version_id)) {
                    INVARIANT(!TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType)dbtype, version_id));
                    continue;
                }
                VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPageFullList(version_id, (DynamicDBType)dbtype);
                int i = 0;
                if (CurPid[dbtype][version_id][i] == -1 && version_id == 0) {
                    CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageByVidRange(0, Range<node_t>(ProcessedVertices.GetBegin(), ProcessedVertices.GetEnd()), version_id, e_type, (DynamicDBType)dbtype, false, true);
                } else if (CurPid[dbtype][version_id][i] == -1 && version_id > 0) {
                    CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageIdFullList(version_id, (DynamicDBType)dbtype);
                    while (CurPid[dbtype][version_id][i] <= TurboDB::GetTurboDB(e_type)->GetLastPageIdFullList(version_id, (DynamicDBType)dbtype)) {
                        if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).contains(ProcessedVertices.GetBegin())) break;
                        else if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).GetBegin() > ProcessedVertices.GetBegin()) break;
                        else CurPid[dbtype][version_id][i]++;
                    }
                }
            }
        }
    } else {
        for (int dbtype = begin_dbtype; dbtype < end_dbtype; dbtype++) {
            for (int64_t version_id = version_range.GetBegin(); version_id <= version_range.GetEnd(); version_id++) {
                int i = 0;
                if (!TurboDB::GetTurboDB(e_type)->is_version_exist_full_list((DynamicDBType)dbtype, version_id)) {
                    INVARIANT(!TurboDB::GetTurboDB(e_type)->is_version_exist((DynamicDBType)dbtype, version_id));
                    continue;
                }
                VidRangePerPage& vidrangeperpage = TurboDB::GetTurboDB(e_type)->GetVidRangePerPageFullList(version_id, (DynamicDBType)dbtype);
                if (CurPid[dbtype][version_id][i] == -1) {
                    CurPid[dbtype][version_id][i] = TurboDB::GetTurboDB(e_type)->GetFirstPageByVidRange(i, Range<node_t>(ProcessedVertices.GetBegin(), ProcessedVertices.GetEnd()), version_id, e_type, (DynamicDBType)dbtype, false, true);
                    continue;
                }
                while (CurPid[dbtype][version_id][i] <= TurboDB::GetTurboDB(e_type)->GetLastPageIdFullList(version_id, (DynamicDBType)dbtype)) {
                    if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).contains(ProcessedVertices.GetBegin())) break;
                    else if (vidrangeperpage.Get(CurPid[dbtype][version_id][i]).GetBegin() > ProcessedVertices.GetBegin()) break;
                    else CurPid[dbtype][version_id][i]++;
                }
            }
        }
    }
}

void RequestRespond::CopyMaterializedAdjListsFromCache(MaterializedAdjacencyLists& mat_adj, SimpleContainer& cont, int64_t adjlist_data_size_cache, int64_t slot_data_size_cache, FixedSizeVector<node_t>& vertices, Range<node_t> vid_range, int64_t size_to_request_cache) {
    ALWAYS_ASSERT(UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
    ALWAYS_ASSERT(vertices.size() == slot_data_size_cache / sizeof(Slot32));
    ALWAYS_ASSERT (VidToAdjacencyListMappingTable::VidToAdjTable.IsValid(0) || VidToAdjacencyListMappingTable::VidToAdjTable.BeingReferenced(0));
    
    char* tmp_container = cont.data;
    int64_t win_lv = 1 - 1;
    int64_t current_slot_idx = 0;
    int64_t current_data_offset = 0;

    mat_adj.NumEntry() = slot_data_size_cache / sizeof(Slot32);
    for (int64_t idx = 0; idx < vertices.size(); idx++) {
        node_t vid = vertices[idx];

        ALWAYS_ASSERT(VidToAdjacencyListMappingTable::VidToAdjTable.Has(vid, win_lv));
        ALWAYS_ASSERT(vid_range.contains(vid));

        AdjacencyListMemoryPointer& ptr = VidToAdjacencyListMappingTable::VidToAdjTable.Get(vid, win_lv);
        MaterializedAdjacencyLists* adj_ptr = (MaterializedAdjacencyLists*) ptr.ptr;
        node_t* data = adj_ptr->GetBeginPtr(ptr.slot_id);
        int64_t sz = adj_ptr->GetNumEntries(ptr.slot_id);

        ALWAYS_ASSERT (vid == adj_ptr->GetSlotVid(ptr.slot_id));
#ifndef PERFORMANCE
        if (UserArguments::USE_DELTA_NWSM) {
            ALWAYS_ASSERT (TurboDB::OutDegree(vid) - TurboDB::OutInsertDegree(vid) + TurboDB::OutDeleteDegree(vid) + TurboDB::OutInsertDegree(vid) + TurboDB::OutDeleteDegree(vid) == sz);
        } else {
            ALWAYS_ASSERT (TurboDB::OutDegree(vid) == sz);
        }
#endif
        ALWAYS_ASSERT (current_data_offset * sizeof(node_t) + sz * sizeof(node_t) <= adjlist_data_size_cache);

        mat_adj.GetSlot(current_slot_idx)->src_vid = adj_ptr->GetSlot(ptr.slot_id)->src_vid;
        mat_adj.GetSlot(current_slot_idx)->end_offset = (current_data_offset + sz);
        mat_adj.GetSlot(current_slot_idx)->end_offset -= adj_ptr->GetNumEntriesOfDeltaInsert(ptr.slot_id);;
        mat_adj.GetSlot(current_slot_idx)->end_offset -= adj_ptr->GetNumEntriesOfDeltaDelete(ptr.slot_id);;
        mat_adj.GetSlot(current_slot_idx)->delta_end_offset[0] = mat_adj.GetSlot(current_slot_idx)->end_offset + adj_ptr->GetNumEntriesOfDeltaInsert(ptr.slot_id);
        mat_adj.GetSlot(current_slot_idx)->delta_end_offset[1] = mat_adj.GetSlot(current_slot_idx)->delta_end_offset[0] + adj_ptr->GetNumEntriesOfDeltaDelete(ptr.slot_id);


        char* copy_to = tmp_container + current_data_offset * sizeof(node_t);
        char* copy_from = (char*) data;

        ALWAYS_ASSERT (copy_to >= tmp_container && copy_to < tmp_container + size_to_request_cache);
        ALWAYS_ASSERT (copy_from >= adj_ptr->Data() && copy_from < adj_ptr->Data() + adj_ptr->Size());

        memcpy ((void*) copy_to, (void*) copy_from, sz * sizeof(node_t));

        current_slot_idx++;
        current_data_offset += sz;
    }
}

void RequestRespond::RespondAdjListBatchIO(char* buf) {
    ALWAYS_ASSERT(UserArguments::USE_DEGREE_ORDER_REPRESENTATION);
	ALWAYS_ASSERT(buf != nullptr);
    turbo_timer timer;

    timer.start_timer(0);
    AdjListRequestBatch* req = (AdjListRequestBatch*) buf;
    Range<int> version_range = Range<int>(req->version_range.GetBegin(), req->version_range.GetEnd());
	Range<node_t> total_req_vid_range = Range<node_t>(PartitionStatistics::per_machine_vid_range(req->src_edge_partition_range.GetBegin()).GetBegin(), PartitionStatistics::per_machine_vid_range(req->src_edge_partition_range.GetEnd()).GetEnd());
    EdgeType e_type = req->e_type;
    DynamicDBType d_type = req->d_type;
	
    ALWAYS_ASSERT (req->to == PartitionStatistics::my_machine_id());
	ALWAYS_ASSERT (PartitionStatistics::per_machine_vid_range(req->to).contains(total_req_vid_range));
    ALWAYS_ASSERT(version_range.GetBegin() >= 0 && version_range.GetEnd() < MAX_NUM_VERSIONS);
    
	InitializeIoInterface();

	TwoLevelBitMap<node_t>* requested_vertices = GetTempNodeBitMap(false, 150); // roughly 5 seconds
    if (requested_vertices == nullptr) {
        rr_.async_resp_pool.enqueue_hot(RequestRespond::RespondAdjListBatchIO, buf);
        return;
    }
	
    TwoLevelBitMap<node_t>* vertices_from_cache = GetTempNodeBitMap(false, 150); // roughly 5 seconds
    if (vertices_from_cache == nullptr) {
        ReturnTempNodeBitMap(false, requested_vertices);
        rr_.async_resp_pool.enqueue_hot(RequestRespond::RespondAdjListBatchIO, buf);
        return;
    }
    vertices_from_cache->ClearAll();

	MPI_Status stat;
	MPI_Message msg;
	int64_t recv_bytes = 0;
	int64_t recv_bytes2 = 0;
	
    timer.start_timer(2);
    turbo_tcp::Recv(&RequestRespond::general_server_sockets[0], (char*) requested_vertices->data(), 0, req->from, &recv_bytes, requested_vertices->BitMap<node_t>::container_size(), true);
    turbo_tcp::Recv(&RequestRespond::general_server_sockets[0], (char*) requested_vertices->two_level_data(), 0, req->from, &recv_bytes, requested_vertices->two_level_container_size(), true);
    //fprintf(stdout, "[%ld] from %d, requested total = %ld\n", PartitionStatistics::my_machine_id(), req->from, requested_vertices->GetTotal());
    timer.stop_timer(2);

	// Estimate the respond size
    timer.start_timer(3);
	int64_t est_output_size = 0;
	if (req->lv < UserArguments::MAX_LEVEL - 1) {
		est_output_size = ComputeMaterializedAdjListsSize(req->dst_edge_partition_range, total_req_vid_range, requested_vertices, req->MaxBytesToPin); //TODO - add version
		RequestRespond::client_sockets.send_to_server_lock(req->from);
		int64_t bytes_sent = RequestRespond::client_sockets.send_to_server((char*) &est_output_size, 0, sizeof(int64_t), req->from);
		INVARIANT (bytes_sent == sizeof(int64_t));
		RequestRespond::client_sockets.send_to_server_unlock(req->from);
	} else {
		est_output_size = req->MaxBytesToPin;
	}
    timer.stop_timer(3);

//#ifdef REPORT_PROFILING_TIMERS
	//fprintf(stdout, "ComputeMaterializedAdjListsSize %ld -> %ld; MaxBytesRequested = %ld, MaxBytesToBeSent = %ld\n", req->from, req->to, req->MaxBytesToPin, est_output_size);
//#endif
       
    /*
    if (req->lv == 0) {
        fprintf(stdout, "[%ld] %ld [RespondAdjListBatchIO] %lld->%lld, |requested_vertices| = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, req->from, req->to, requested_vertices->GetTotalSingleThread());
    }
    */

    if (req->lv < UserArguments::MAX_LEVEL - 1 && req->from == req->to) {
        int64_t tmp = -1;
        int64_t recv_bytes = 0;
        turbo_tcp::Recv(&RequestRespond::general_server_sockets[0], (char*) &tmp, 0, req->from, &recv_bytes);
    }

    if (req->lv == UserArguments::MAX_LEVEL - 1) {
        //requested_vertices->ClearAll();
    }

	// Spawn a network sending thread
	int64_t stop_flag = 0;
	tbb::concurrent_queue<UdfSendRequest>* req_data_queue = new tbb::concurrent_queue<UdfSendRequest>();
	tbb::concurrent_queue<UdfSendRequest>* stoped_req_data_queue = new tbb::concurrent_queue<UdfSendRequest>();
    std::future<void> wait_on_sender_thread = Aio_Helper::async_pool.enqueue_hot(RequestRespond::SendDataInBufferWithUdfRequest, req_data_queue, stoped_req_data_queue, req->from, total_req_vid_range, requested_vertices, RequestType::NewAdjListBatchIoData, 0, req->lv, est_output_size, &stop_flag);

    std::queue<std::future<void>> reqs_to_wait;
    node_t vid_range_length = total_req_vid_range.length();
    // XXX - syko
    //if (true) {
    if (vid_range_length < 1024) {
        node_t begin_vid = total_req_vid_range.GetBegin();
        node_t end_vid = total_req_vid_range.GetEnd();
        reqs_to_wait.push(Aio_Helper::async_pool.enqueue(RequestRespond::RespondAdjListBatchIoFullListParallel, req, Range<node_t>(begin_vid, end_vid), total_req_vid_range, requested_vertices, vertices_from_cache, req_data_queue, &stop_flag, version_range, e_type, d_type));
    } else {
        node_t vid_range_length_per_task = total_req_vid_range.length() / 4;
        vid_range_length_per_task = (vid_range_length_per_task / 64) * 64;
        INVARIANT(vid_range_length_per_task != 0L); // syko..
        for (int k = 0; k < 4; k++) {
            node_t begin_vid = total_req_vid_range.GetBegin() + k * vid_range_length_per_task;
            node_t end_vid = total_req_vid_range.GetBegin() + (k+1) * vid_range_length_per_task;
            if (k == 4 - 1) {
                end_vid = total_req_vid_range.GetEnd();
            } else {
                end_vid -= 1;
            }
            reqs_to_wait.push(Aio_Helper::async_pool.enqueue(RequestRespond::RespondAdjListBatchIoFullListParallel, req, Range<node_t>(begin_vid, end_vid), total_req_vid_range, requested_vertices, vertices_from_cache, req_data_queue, &stop_flag, version_range, e_type, d_type));
        }
    }
    timer.start_timer(4);
    while (!reqs_to_wait.empty()) {
        reqs_to_wait.front().get();
        reqs_to_wait.pop();
    }
    timer.stop_timer(4);

	UdfSendRequest udf_req;
	udf_req.req.rt = UserCallback;
	udf_req.req.from = PartitionStatistics::my_machine_id();
	udf_req.req.to = req->from;
	udf_req.req.parm1 = -1;
    udf_req.req.lv = req->lv;
	udf_req.req.data_size = 0;
	SimpleContainer cont;
	cont.data = nullptr;
	cont.size_used = 0;
	cont.capacity = 0;
	udf_req.cont = cont;
	req_data_queue->push(udf_req);

	// Wait for the sending request
	wait_on_sender_thread.get();
	ALWAYS_ASSERT(req_data_queue->empty());
    
    // Re-Mark the vertices that are not sent (STOPed)
    UdfSendRequest stoped_udf_req;
	while (stoped_req_data_queue->try_pop(stoped_udf_req)) {
        //MaterializedAdjacencyLists::MarkVerticesIntoBitMap(stoped_udf_req, requested_vertices, total_req_vid_range.GetBegin());
        SimpleContainer cont = stoped_udf_req.cont;
        if (cont.data != NULL) {
            ReturnDataSendBuffer(cont);
        }
    }

	// Return back 'requested vertices'
    timer.start_timer(5);
    //if (req->lv < UserArguments::MAX_LEVEL - 1) {
    if (true) {
        //fprintf(stdout, "[%ld] fuck vids total = %ld\n", PartitionStatistics::my_machine_id(), requested_vertices->GetTotal());
        TwoLevelBitMap<node_t>::UnionToRight(*vertices_from_cache, *requested_vertices);
        RequestRespond::client_sockets.send_to_server_lock(req->from);
        int64_t bytes_sent = RequestRespond::client_sockets.send_to_server((char*) requested_vertices->data(), 0, requested_vertices->container_size(), req->from);
        ALWAYS_ASSERT(bytes_sent == requested_vertices->container_size());
        bytes_sent = RequestRespond::client_sockets.send_to_server((char*) requested_vertices->two_level_data(), 0, requested_vertices->two_level_container_size(), req->from);
        ALWAYS_ASSERT(bytes_sent == requested_vertices->two_level_container_size());
        RequestRespond::client_sockets.send_to_server_unlock(req->from);
    }
    timer.stop_timer(5);
    timer.stop_timer(0);

#ifdef REPORT_PROFILING_TIMERS
    fprintf(stdout, "[%lld] %lld [RESPOND_ADJLIST_BATCH_IO] %lld->%lld, vids_tot: %ld [%lld, %lld], %.2f %.2f %.2f %.2f %.2f %.2f\n", (int64_t) PartitionStatistics::my_machine_id(), (int64_t) UserArguments::UPDATE_VERSION, (int64_t) req->to, (int64_t) req->from, requested_vertices->GetTotal(), (int64_t) total_req_vid_range.GetBegin(), (int64_t) total_req_vid_range.GetEnd(), timer.get_timer(0), timer.get_timer(1), timer.get_timer(2), timer.get_timer(3), timer.get_timer(4), timer.get_timer(5));
#endif

    INVARIANT (req_data_queue->empty());
	INVARIANT (stoped_req_data_queue->empty());
	delete req_data_queue;
	delete stoped_req_data_queue;

	// Return the buffer
    ReturnTempReqReceiveBuffer(buf);
    ReturnTempNodeBitMap(false, requested_vertices);
    ReturnTempNodeBitMap(false, vertices_from_cache);
}

void RequestRespond::MatAdjlistSanitycheck(char* data_buffer, int64_t data_size, int64_t capacity, int tag) {
	MaterializedAdjacencyLists mat_adj2;
	mat_adj2.Initialize(data_size, capacity, data_buffer);
	mat_adj2.SanityCheckDegreeOrdered(tag);
}

void RequestRespond::ReceiveRequest() {
	int has_msg = 0;
	std::future<void> respond;
	bool is_working = 0;
	int recv_bytes = 0;
	int mpi_error = -1;
	MPI_Status stat;
	char str_buf[256];

    turbo_timer tim;

	while(true) {
		has_msg = 0;
        tim.clear_all_timers();
        tim.start_timer(0);
		while(!has_msg) {
			mpi_error = MPI_Probe(MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &stat);
			ALWAYS_ASSERT (mpi_error == MPI_SUCCESS);
			has_msg = true;
			if(end && !has_msg) {
                tim.stop_timer(0);
				return;
			}
		}
        tim.stop_timer(0);
        
        tim.start_timer(1);
		ALWAYS_ASSERT (stat.MPI_SOURCE >= 0 && stat.MPI_SOURCE < PartitionStatistics::num_machines());

		char* tmp_buffer = GetTempReqReceiveBuffer();
        tim.stop_timer(1);
        tim.start_timer(2);
		MPI_Recv((void *)tmp_buffer, max_request_size, MPI_CHAR, stat.MPI_SOURCE, 1, MPI_COMM_WORLD, &stat);
		MPI_Get_count(&stat, MPI_CHAR, &recv_bytes);
        tim.stop_timer(2);
        //fprintf(stdout, "[%ld] ReceiveRequest (%d->%d) %.3f %.3f %.3f, %.3f(MB/sec)\n", PartitionStatistics::my_machine_id(), stat.MPI_SOURCE, PartitionStatistics::my_machine_id(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), (double) recv_bytes / (tim.get_timer(2) * 1024 * 1024));
        ALWAYS_ASSERT(0 < recv_bytes <= max_request_size);

        switch(((Request *)tmp_buffer)->rt) {
            case Exit: {
                is_working = false;
                break; }
            case OutputVectorRead: {
                OutputVectorReadRequest* tmp_r = (OutputVectorReadRequest*) tmp_buffer;
                respond = rr_.async_resp_pool.enqueue(RequestRespond::RespondOutputVectorRead, tmp_r->vectorID, tmp_r->chunkID, tmp_r->from, tmp_r->lv);
                is_working = true;
                ReturnTempReqReceiveBuffer(tmp_buffer);
                break; }
            case InputVectorRead: {
                InputVectorReadRequest* tmp_ivr = (InputVectorReadRequest*) tmp_buffer;
                respond = rr_.async_resp_pool.enqueue(RequestRespond::RespondInputVectorRead, tmp_ivr->vectorID, tmp_ivr->chunkID, tmp_ivr->from, tmp_ivr->lv);
                is_working = true;
                ReturnTempReqReceiveBuffer(tmp_buffer);
                break; }
            case OutputVectorWriteMessage: {
                OutputVectorWriteMessageRequest* tmp_wm = (OutputVectorWriteMessageRequest*) tmp_buffer;
                respond = rr_.async_resp_pool.enqueue(RequestRespond::RespondOutputVectorWriteMessage, tmp_wm->vectorID, tmp_wm->fromChunkID, tmp_wm->chunkID, tmp_wm->from, tmp_wm->tid, tmp_wm->idx, tmp_wm->send_num, tmp_wm->combined, tmp_wm->lv);
                is_working = true;
                ReturnTempReqReceiveBuffer(tmp_buffer);
                break; }
            case AdjListBatchIo: {
                ALWAYS_ASSERT (recv_bytes == sizeof(AdjListRequestBatch));
                respond =  rr_.async_resp_pool.enqueue(RequestRespond::RespondAdjListBatchIO, tmp_buffer);
                is_working = true;
                break; }
            case SequentialVectorRead: {
                SequentialVectorReadRequest* tmp_sr = (SequentialVectorReadRequest*) tmp_buffer;
                respond = rr_.async_resp_pool.enqueue(RequestRespond::RespondSequentialVectorRead, tmp_sr->vectorID, tmp_sr->chunkID, tmp_sr->from, tmp_sr->lv);
                is_working = true;
                ReturnTempReqReceiveBuffer(tmp_buffer);
                break; }
            default: {
                fprintf(stdout, "[RequestRespond] Undefined Request Type\n");
                ALWAYS_ASSERT(false);
                break; }
        }
        if(!is_working) {
            return;
        }
	}
}


void RequestRespond::UserCallbackSanityCheck(char* data_buffer, int64_t data_size, RequestType rt_data) {
	if (rt_data == RequestType::AdjListBatchIoData) {
		ALWAYS_ASSERT (false);
		abort();
	} else if (rt_data == RequestType::UserCallbackData) {
		MaterializedAdjacencyLists mat_adj;
		SimpleContainer cont;
		cont.data = data_buffer;
		cont.capacity = data_size;
		mat_adj.Initialize(cont, data_size);
	} else {
		ALWAYS_ASSERT (false);
		fprintf(stderr, "[RequestRespond::SendRequest] Undefined Request Type\n");
		abort();
	}

}

void RequestRespond::SendData(char* buf, int64_t count, int partition_id, int tag, SimpleContainer cont, bool return_container) {
	turbo_timer tmp_tim;
	int mpi_error = -1;

	tmp_tim.start_timer(0);
	mpi_error = MPI_Ssend((void *) buf, count, MPI_CHAR, partition_id, tag, MPI_COMM_WORLD);
	tmp_tim.stop_timer(0);
	ALWAYS_ASSERT (mpi_error == MPI_SUCCESS);

	if (return_container) {
		ReturnDataSendBuffer(cont);
	}

	double size_mb = (double) count / ((double) 1024*1024);
	double secs = tmp_tim.get_timer(0);
	double bw = size_mb / secs;
	/*
    sprintf(&str_buf[0], "END\tSendData (%.3f MB, %.4f secs. %.3f MB/sec) to %ld\n", size_mb, secs, bw, partition_id);
	LOG(INFO) << std::string(str_buf);
    */
}

void RequestRespond::SendDataInBufferWithUdfRequest(tbb::concurrent_queue<UdfSendRequest>* req_data_queue, tbb::concurrent_queue<UdfSendRequest>* stoped_req_data_queue,int partition_id, Range<node_t> vid_range, BitMap<node_t>* vids, int tag, int chunk_idx, int lv, int64_t MaxBytesToPin, int64_t* stop_flag) {
	RequestRespond::client_sockets.send_to_server_lock(partition_id);
	ALWAYS_ASSERT (req_data_queue != NULL);

	turbo_timer tmp_tim;
	tmp_tim.start_timer(3);
	int64_t bytes_sent;
	int mpi_error = -1;
	int64_t count = 0;
	int64_t ack = -1;
    bool all_reqs_in_queue = false;

	UdfSendRequest udf_req;
	SimpleContainer cont;
	UserCallbackRequest req;

    std::srand(std::time(nullptr));
    int64_t max_fail_count = 0;
	while (true) {
		tmp_tim.start_timer(1);
		int backoff = 1;
		while (!req_data_queue->try_pop(udf_req)) {
			ALWAYS_ASSERT (all_reqs_in_queue == false);
			usleep (backoff * 4);
			backoff = (backoff >= 1024) ? 1024 : 2 * backoff;
		}
		tmp_tim.stop_timer(1);
	    ALWAYS_ASSERT (udf_req.req.data_size == udf_req.cont.size_used);
		ALWAYS_ASSERT (udf_req.req.from == PartitionStatistics::my_machine_id() && udf_req.req.to == partition_id);

		cont = udf_req.cont;
		req = udf_req.req;
		req.lv = lv;

		if (cont.data == NULL && cont.capacity == 0 && cont.size_used == 0) {
			INVARIANT (!all_reqs_in_queue);
			all_reqs_in_queue = true;
			break;
		}
		ALWAYS_ASSERT (cont.size_used != 0);
		tmp_tim.start_timer(0);

		if (lv < UserArguments::MAX_LEVEL - 1 && count + udf_req.req.data_size > MaxBytesToPin) {
		//if (lv == 0 && (std::rand() % 10 <= 8 || count + udf_req.req.data_size > MaxBytesToPin)) {
		//if (false && count + udf_req.req.data_size > MaxBytesToPin) {
        //if (false) {
			std::atomic_store((std::atomic<int64_t>*) stop_flag, 1L);
			stoped_req_data_queue->push(udf_req);
            //fprintf(stdout, "[%ld] STOP SendDataInBufferWithUdfRequest to %ld; %ld + %ld > %ld\n", PartitionStatistics::my_machine_id(), partition_id, count, udf_req.req.data_size, MaxBytesToPin);
			continue;
		}

        bool retry = false;
		//fprintf(stdout, "[%ld] Sending1 (%ld -> %ld) [%ld, %ld] of %p, sz = %ld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id, req.vid_range.GetBegin(), req.vid_range.GetEnd(), cont.data, sizeof(UserCallbackRequest));
        bytes_sent = RequestRespond::client_sockets.send_to_server((char*) &req, 0, sizeof(UserCallbackRequest), partition_id, false);
        if (bytes_sent == -1 || bytes_sent != sizeof(UserCallbackRequest)) {
            //fprintf(stdout, "[%ld] Fail to Sending1 (%ld -> %ld) [%ld, %ld] of %p, sz = %ld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id, req.vid_range.GetBegin(), req.vid_range.GetEnd(), cont.data, sizeof(UserCallbackRequest));
            max_fail_count++;
            retry = true;
            goto Retry;
        }
		//if (bytes_sent != sizeof(UserCallbackRequest)) goto Retry;
        //INVARIANT (bytes_sent == sizeof(UserCallbackRequest));
		//fprintf(stdout, "[%ld] Sending2 (%ld -> %ld) [%ld, %ld] of %p, sz = %ld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id, req.vid_range.GetBegin(), req.vid_range.GetEnd(), cont.data, cont.size_used);
		bytes_sent = RequestRespond::client_sockets.send_to_server((char*) cont.data, 0, cont.size_used, partition_id, false);
        if (bytes_sent == -1 || bytes_sent != cont.size_used) {
            //fprintf(stdout, "[%ld] Fail to Sending2 (%ld -> %ld) [%ld, %ld] of %p, sz = %ld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), partition_id, req.vid_range.GetBegin(), req.vid_range.GetEnd(), cont.data, cont.size_used);
            max_fail_count++;
            retry = true;
            goto Retry;
        }
		//INVARIANT (bytes_sent == cont.size_used);
        
        //bytes_sent = RequestRespond::client_sockets.recv_from_server((char*) &ack, 0, partition_id);
		//if (bytes_sent != sizeof(int64_t)) goto Retry;
        //INVARIANT(ack == 9999);
		//fprintf(stdout, "[%ld] Sending to %ld, sz = %ld ACK received\n", PartitionStatistics::my_machine_id(), partition_id, cont.size_used);
		
        tmp_tim.stop_timer(0);

Retry: {
		ReturnDataSendBuffer(cont);
        if (!retry) {
            max_fail_count = 0;
            count += cont.size_used;
            //fprintf(stdout, "[%ld] lv:%d Success to Sending (%ld -> %ld) [%ld, %ld] of %p, sz = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::CURRENT_LEVEL, PartitionStatistics::my_machine_id(), partition_id, req.vid_range.GetBegin(), req.vid_range.GetEnd(), cont.data, cont.size_used);
        } else {
            //fprintf(stdout, "[%ld] Retry at SendDataInBuffer fail_count = %ld\n", PartitionStatistics::my_machine_id(), max_fail_count);
            if (max_fail_count > 10)
                break;
        }
        }
	}
DONE: {
	ALWAYS_ASSERT (all_reqs_in_queue);
	while (req_data_queue->try_pop(udf_req)) {
		stoped_req_data_queue->push(udf_req);
	}

	// Send EOM
	req.data_size = 0;
	req.lv = lv;
    do {
        bytes_sent = RequestRespond::client_sockets.send_to_server((char*) &req, 0, sizeof(UserCallbackRequest), partition_id);
    } while (bytes_sent == -1);
	INVARIANT (bytes_sent == sizeof(UserCallbackRequest));

	RequestRespond::client_sockets.send_to_server_unlock(partition_id);

	double size_mb = (double) count / ((double) 1024*1024);
	double total_secs = tmp_tim.stop_timer(3);
	double secs = tmp_tim.get_timer(0);
	double bw = size_mb / secs;
    }
}

// TODO - assume that the data being sent is from 'RequestOutputVectorWriteMessagePush'
void RequestRespond::SendDataInBuffer(tbb::concurrent_queue<SimpleContainer>* req_data_queue, turbo_tcp* vector_client_sockets, int partition_id, int tag, int chunk_idx, int lv, int64_t eom_cnt, bool delete_req_buffer_queue) {
	ALWAYS_ASSERT (chunk_idx >= 0 && chunk_idx < UserArguments::VECTOR_PARTITIONS);
	ALWAYS_ASSERT (req_data_queue != NULL);
    ALWAYS_ASSERT (!UserArguments::USE_PULL);
	
    vector_client_sockets->send_to_server_lock(partition_id);

	turbo_timer tmp_tim;
	int mpi_error = -1;
	int64_t count = 0;
	int64_t eom_cnt_received = 0;

	tmp_tim.start_timer(3);
	SimpleContainer cont_combined = RequestRespond::GetDataSendBuffer(DEFAULT_NIO_BUFFER_SIZE);
	SimpleContainer cont;
	bool all_reqs_in_queue = false;
	cont_combined.size_used = 0;

Start:
	while (true) {
		int backoff = 1;
		tmp_tim.start_timer(1);
		while (!req_data_queue->try_pop(cont)) {
			if (all_reqs_in_queue) goto DONE;
			ALWAYS_ASSERT (all_reqs_in_queue == false);
			usleep (backoff * 128);
			backoff = 2 * backoff;
			if (backoff >= 128) backoff = 128;
		}
		tmp_tim.stop_timer(1);

		if (cont.data == NULL && cont.capacity == 0 && cont.size_used == 0) {
			INVARIANT (!all_reqs_in_queue);
			eom_cnt_received++;
			if (eom_cnt_received == eom_cnt) {
				all_reqs_in_queue = true;
				break;
			}
			continue;
		}
		ALWAYS_ASSERT (cont.size_used != 0);

		// Combine
		int64_t size_to_combine = (cont_combined.size_available() > cont.size_used) ? cont.size_used : cont_combined.size_available();
		memcpy ((void*) (cont_combined.data + cont_combined.size_used), (void*) cont.data, size_to_combine);
		cont_combined.size_used += size_to_combine;

		if (cont_combined.size_available() == 0) {
			// Flush
			tmp_tim.start_timer(0);

			int64_t bytes_sent = vector_client_sockets->send_to_server((char*) cont_combined.data, 0, cont_combined.size_used, partition_id, true);
			INVARIANT (bytes_sent == cont_combined.size_used);
			double took = tmp_tim.stop_timer(0);
			count += cont_combined.size_used;
			cont_combined.size_used = 0;
		}

		if (size_to_combine != cont.size_used) {
			INVARIANT(size_to_combine < cont.size_used);
			INVARIANT(cont_combined.size_used == 0);
			memcpy ((void*) (cont_combined.data + cont_combined.size_used), (void*) (cont.data + size_to_combine), cont.size_used - size_to_combine);
			cont_combined.size_used = cont.size_used - size_to_combine;
		}

		// return buffer
		ReturnDataSendBuffer(cont);
	}

	if (cont_combined.size_used != 0) {
		int64_t bytes_sent = vector_client_sockets->send_to_server((char*) cont_combined.data, 0, cont_combined.size_used, partition_id, true);
		INVARIANT (bytes_sent == cont_combined.size_used);
		double took = tmp_tim.stop_timer(0);
		count += cont_combined.size_used;
	}
	ReturnDataSendBuffer(cont_combined);

DONE:
	if (delete_req_buffer_queue) {
		delete req_data_queue;
	}

	// Send EOM
	char dummy;
	int64_t bytes_sent = vector_client_sockets->send_to_server((char*) &dummy, 0, 0, partition_id, true);
	INVARIANT (bytes_sent == cont.size_used);
	double size_mb = (double) count / ((double) 1024*1024);
	double total_secs = tmp_tim.stop_timer(3);
	double secs = tmp_tim.get_timer(0);
    double bw = size_mb / secs;
   
    /*
    sprintf(&str_buf[0], "END\tSendData { (WAIT_FOR_DATA_TO_BE_QUEUED) %.4f sec, (MPI_IO) %.3f MB, %.3f secs. %.3f MB/sec) to %ld; took %.4f\n", tmp_tim.get_timer(1), size_mb, secs, bw, partition_id, total_secs);
	LOG(INFO) << std::string(str_buf);
    */
	vector_client_sockets->send_to_server_unlock(partition_id);
}

/*template <typename T, Op op> 
void RequestRespond::TurboAllReduceInPlaceAsync(char* buf, int count, int size_in_bytes, Op op, int queue_idx) {
    // TODO if # machines == 1, correct?
    // Get available sockets
    int available_socket_idx = -1;
    if (PartitionStatistics::my_machine_id() == 0) {
        while (true) {
            auto it = std::find (RequestRespond::general_connection_pool.begin(), RequestRespond::general_connection_pool.end(), true);
            if (it == RequestRespond::general_connection_pool.end()) usleep(1000);
            else {
                available_socket_idx = it - RequestRespond::general_connection_pool.begin();
                break;
            }
        }
        INVARIANT(available_socket_idx >= 0 && available_socket_idx < DEFAULT_NUM_GENERAL_TCP_CONNECTIONS);
        RequestRespond::general_server_sockets[available_socket_idx].lock_socket();
        RequestRespond::general_connection_pool[available_socket_idx] = false;
        RequestRespond::general_server_sockets[available_socket_idx].unlock_socket();
        MPI_Bcast((void*) &available_socket_idx, sizeof(int), MPI_INT, 0, MPI_COMM_WORLD);
    } else {
        MPI_Bcast((void*) &available_socket_idx, sizeof(int), MPI_INT, 0, MPI_COMM_WORLD);
    }


    // Enqueue into Async Pool
    RequestRespond::reqs_to_wait_allreduce[queue_idx].push(Aio_Helper::async_pool.enqueue(RequestRespond::TurboAllReduceInPlace<T, op>, &RequestRespond::general_server_sockets[available_socket_idx], &RequestRespond::general_client_sockets[available_socket_idx], available_socket_idx, buf, count, size_in_bytes, op));
}*/

void RequestRespond::Barrier() {
    RequestRespond::TurboBarrier(&RequestRespond::general_server_sockets[1], &RequestRespond::general_client_sockets[1]);
    return;
}

/*template <typename T, Op op>
void RequestRespond::TurboAllReduceSync(char* buf, int count, int size_in_bytes, Op op) {
    RequestRespond::TurboAllReduceInPlace<T, op>(&RequestRespond::general_server_sockets[1], &RequestRespond::general_client_sockets[1], 1, buf, count, size_in_bytes, op);
}*/

void RequestRespond::WaitAllreduce() {
    while (!RequestRespond::reqs_to_wait_allreduce[0].empty()) {
        RequestRespond::reqs_to_wait_allreduce[0].front().get();
        RequestRespond::reqs_to_wait_allreduce[0].pop();
    }
    while (!RequestRespond::reqs_to_wait_allreduce[1].empty()) {
        RequestRespond::reqs_to_wait_allreduce[1].front().get();
        RequestRespond::reqs_to_wait_allreduce[1].pop();
    }
}

void RequestRespond::PopAllreduce() {
    if (!RequestRespond::reqs_to_wait_allreduce[0].empty()) {
        RequestRespond::reqs_to_wait_allreduce[0].front().get();
        RequestRespond::reqs_to_wait_allreduce[0].pop();
    }
}

// TODO : support other data types including char, support other operations also

void RequestRespond::TurboBarrier(turbo_tcp* server_socket, turbo_tcp* client_socket) {
    int socket_idx = 1;
    int64_t total_bytes_sent = 0, total_bytes_received = 0;
    int64_t total_size = 1;

    int granule = 2;
    char dummy;
    
    int iteration = 0;
    int max_iteration = std::ceil (std::log2 ((double) PartitionStatistics::num_machines()));
    bool is_finished = false;

    // Reduce
    while (iteration < max_iteration) {
        total_bytes_sent = 0;
        total_bytes_received = 0;
        if (((PartitionStatistics::my_machine_id() + 1) % granule == 1) && ((PartitionStatistics::my_machine_id() + 1) + (granule / 2) > PartitionStatistics::num_machines()) || is_finished) {
        } else {
            if ((PartitionStatistics::my_machine_id() + 1) % granule == 1) { // recv from my_id + (granule / 2)
                while (total_bytes_received != total_size) {
                    int64_t bytes_receive = server_socket->recv_from_client((char*) &dummy, 0, PartitionStatistics::my_machine_id() + (granule / 2));
                    total_bytes_received += bytes_receive;
                }
            } else { // send to my_id - (granule / 2)
                while (total_bytes_sent != total_size) {
                    int64_t size_to_send = total_size;
                    int64_t bytes_send = client_socket->send_to_server((char*) &dummy, total_bytes_sent, size_to_send, PartitionStatistics::my_machine_id() - (granule / 2));
                    INVARIANT(bytes_send == size_to_send);
                    total_bytes_sent += bytes_send;
                }
                is_finished = true;
            }
        }
        iteration++;
        granule *= 2;
    }
    
    // Gather
    iteration = 0;
    while (iteration < max_iteration) {
        total_bytes_sent = 0;
        total_bytes_received = 0;
        granule /= 2;
        if (((PartitionStatistics::my_machine_id() + 1) % granule != 1) && ((PartitionStatistics::my_machine_id() + 1) % granule != ((granule / 2) + 1) % granule)) {
        } else if (((PartitionStatistics::my_machine_id() + 1) % granule == 1) && (PartitionStatistics::my_machine_id() + (granule / 2) >= PartitionStatistics::num_machines())){
        } else {
            if ((PartitionStatistics::my_machine_id() + 1) % granule == 1) { // send to my_id + (granule / 2)
                while (total_bytes_sent != total_size) {
                    int64_t size_to_send = total_size;
                    int64_t bytes_send = client_socket->send_to_server((char*) &dummy, total_bytes_sent, size_to_send, PartitionStatistics::my_machine_id() + (granule / 2));
                    INVARIANT(bytes_send == size_to_send);
                    total_bytes_sent += bytes_send;
                }
            } else { // recv from my_id - (granule / 2)
                while (total_bytes_received != total_size) {
                    int64_t bytes_receive = server_socket->recv_from_client((char*) &dummy, 0, PartitionStatistics::my_machine_id() - (granule / 2));
                    total_bytes_received += bytes_receive;
                }
            }
        }
        iteration++;
    }
    ALWAYS_ASSERT(granule == 2);
}

SimpleContainer RequestRespond::GetDataSendBuffer (int64_t req_buf_size) {
	SimpleContainer cont;
	//if (true) {
    if (req_buf_size > large_data_size) {
		int backoff = 1;
		void* buffer = NULL;

		while ((buffer = (void*) circular_stream_send_buffer.Allocate(req_buf_size)) == NULL) {
			usleep(backoff * 1024);
			backoff= 2 * backoff;
			if (backoff >= 256) backoff = 256;
		}
		INVARIANT (buffer != NULL);

		cont.data = (char*) buffer;
		cont.capacity = req_buf_size;
		cont.size_used = 0;
	} else {
		cont = GetTempDataBuffer(req_buf_size);
		cont.size_used = 0;
	}
	return cont;
}

SimpleContainer RequestRespond::GetDataRecvBuffer (int64_t req_buf_size) {
	SimpleContainer cont;
	if (req_buf_size > large_data_size) {
		int backoff = 1;
		void* buffer = NULL;

		while ((buffer = (void*) circular_stream_recv_buffer.Allocate(req_buf_size)) == NULL) {
			usleep(backoff * 1024);
			backoff= 2 * backoff;
			if (backoff >= 256) backoff = 256;
		}

		INVARIANT (buffer != NULL);
		cont.data = (char*) buffer;
		cont.capacity = req_buf_size;
		cont.size_used = 0;
	} else {
		cont = GetTempDataBuffer(req_buf_size);
	}
	return cont;
}


void RequestRespond::ReturnDataSendBuffer (SimpleContainer cont) {
	//if (true) {
	if (cont.capacity > large_data_size) {
		circular_stream_send_buffer.Free(cont.data, cont.capacity);
	} else {
		ReturnTempDataBuffer(cont);
	}
	return;
}

void RequestRespond::ReturnDataRecvBuffer (SimpleContainer cont) {
	if (cont.capacity > large_data_size) {
		circular_stream_recv_buffer.Free(cont.data, cont.capacity);
	} else {
		ReturnTempDataBuffer(cont);
	}
	return;
}


