#include "analytics/core/turbo_buffer_manager.hpp"
#include "analytics/datastructure/disk_aio_factory.hpp"
#include "analytics/core/TurboDB.hpp"

void(*turbo_callback::AsyncCallbackUpdateDirectTableFunc)(diskaio::DiskAioRequestUserInfo &user_info) = AsyncCallbackUpdateDirectTable;
turbo_buffer_manager turbo_buffer_manager::buf_mgr;


void AsyncCallbackUpdateDirectTable(diskaio::DiskAioRequestUserInfo &user_info) {
    EdgeType e_type = (EdgeType)user_info.db_info.e_type;
    DynamicDBType d_type = (DynamicDBType)user_info.db_info.d_type;
    PageID table_page_id = user_info.db_info.table_page_id;
    ALWAYS_ASSERT(table_page_id != INVALID_PAGE);
    TurboDB::GetBufMgr()->GetDirectTable()->Insert(table_page_id, user_info.frame_id);
	ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(table_page_id));
    bool x = TurboDB::GetBufMgr()->GetDirectTable()->unlock(table_page_id);
    if (!x) {
        fprintf(stdout, "[%ld] AsyncCallbackUpdateDirectTable frame %d, pid %d, fid %d, version %d, tpid %d\n", PartitionStatistics::my_machine_id(), user_info.frame_id, user_info.db_info.page_id, user_info.db_info.file_id, user_info.db_info.version_id, user_info.db_info.table_page_id);
        INVARIANT(false);
    }
}

void AsyncCallbackUserRead(diskaio::DiskAioRequest* req) { 
	TurboDB::GetBufMgr()->GetFrame(req->user_info.frame_id).Lock();
	TurboDB::GetBufMgr()->GetFrame(req->user_info.frame_id).Unpin();
	TurboDB::GetBufMgr()->GetFrame(req->user_info.frame_id).Unlock();

	if (TurboDB::GetBufMgr()->GetFrame(req->user_info.frame_id).IsPinned()){
		PageAndFrameID victim_table_pid_fid = TurboDB::GetBufMgr()->PickVictim();

		if (victim_table_pid_fid.pid != INVALID_PAGE) {
            TurboDB::GetBufMgr()->GetDirectTable()->lock(victim_table_pid_fid.pid);
        }
		if (TurboDB::GetBufMgr()->GetDirectTable()->IsDirty(victim_table_pid_fid.pid)) {
			TurboDB::GetBufMgr()->GetDirectTable()->Insert(victim_table_pid_fid.pid, victim_table_pid_fid.fid);

			AioRequest write_req;
            write_req.user_info.db_info.table_page_id = victim_table_pid_fid.pid;
            
            // Request information for callback (Read after flush dirty page)
			write_req.user_info = req->user_info;
            ALWAYS_ASSERT(write_req.user_info.func);
			write_req.user_info.do_user_cb = false;
			write_req.user_info.do_user_only_req_cb = true;
			write_req.user_info.frame_id = victim_table_pid_fid.fid;
			write_req.user_info.db_info_cb.page_id = req->user_info.db_info_cb.page_id;
            write_req.user_info.db_info_cb.file_id = req->user_info.db_info_cb.file_id;
            write_req.user_info.db_info_cb.table_page_id = req->user_info.db_info_cb.table_page_id;
            write_req.user_info.db_info_cb.e_type = req->user_info.db_info_cb.e_type;
            write_req.user_info.db_info_cb.d_type = req->user_info.db_info_cb.d_type;
            write_req.user_info.db_info_cb.version_id = req->user_info.db_info_cb.version_id;
            INVARIANT(write_req.user_info.db_info.table_page_id != INVALID_PAGE);
                
            // Request information for flushing dirty page (page_id and file_id)
            TurboDB::ConvertDirectTablePageID(victim_table_pid_fid.pid, write_req.user_info.db_info.page_id, write_req.user_info.db_info.file_id);
            
			TurboDB::GetBufMgr()->GetFrame(write_req.user_info.frame_id).SetPageID(victim_table_pid_fid.pid);
			TurboDB::GetBufMgr()->GetFrame(write_req.user_info.frame_id).Pin();
			TurboDB::GetBufMgr()->GetFrame(write_req.user_info.frame_id).Unlock();

			write_req.buf = (char*) TurboDB::GetBufMgr()->GetFrame(write_req.user_info.frame_id).GetPage();
			write_req.user_info.read_buf = (char*) TurboDB::GetBufMgr()->GetFrame(write_req.user_info.frame_id).GetPage();

			ALWAYS_ASSERT(write_req.user_info.func != NULL);
			ALWAYS_ASSERT(write_req.user_info.caller != NULL || !write_req.user_info.do_user_cb);
			ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(victim_table_pid_fid.pid));
			ALWAYS_ASSERT(write_req.user_info.read_my_io != NULL);
			ALWAYS_ASSERT(write_req.buf != NULL);
			ALWAYS_ASSERT(write_req.user_info.read_buf != NULL);

			write_req.start_pos = write_req.user_info.db_info.page_id * TBGPP_PAGE_SIZE;
			write_req.io_size = TBGPP_PAGE_SIZE * 1;

			TurboDB::GetBufMgr()->GetDirectTable()->ResetDirty(victim_table_pid_fid.pid);
			TurboDB::GetBufMgr()->GetDirectTable()->unlock(victim_table_pid_fid.pid);

            std::atomic_fetch_add((std::atomic<int64_t>*) &TurboDB::GetBufMgr()->num_write_pages, +1L);
			bool success = DiskAioFactory::GetPtr()->AWrite(write_req, (diskaio::DiskAioInterface*)write_req.user_info.read_my_io);
			if (!success) {
				std::cerr << "Turbo_DB::WritePage, write page error" << std::endl;
				return;
			}
			ALWAYS_ASSERT(write_req.buf != NULL);
		} else {
			//do read request
			AioRequest read_req;
			read_req.user_info.db_info.page_id = req->user_info.db_info_cb.page_id;
			read_req.user_info.db_info.file_id = req->user_info.db_info_cb.file_id;
            read_req.user_info.db_info.table_page_id = req->user_info.db_info_cb.table_page_id;
            read_req.user_info.db_info.e_type = req->user_info.db_info_cb.e_type;
            read_req.user_info.db_info.d_type = req->user_info.db_info_cb.d_type;
            read_req.user_info.db_info.version_id = req->user_info.db_info_cb.version_id;
			read_req.user_info.frame_id = victim_table_pid_fid.fid;
			read_req.user_info.task_id = req->user_info.task_id;
			read_req.user_info.caller = req->user_info.caller;
            read_req.user_info.func = (void*) &InvokeUserCallback;
			read_req.user_info.do_user_cb = true;
			read_req.user_info.do_user_only_req_cb = false;
            INVARIANT(read_req.user_info.db_info.table_page_id != INVALID_PAGE);

			TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Clear();
            TurboDB::GetBufMgr()->GetDirectTable()->Insert(req->user_info.db_info_cb.table_page_id, read_req.user_info.frame_id);
			TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).SetPageID(req->user_info.db_info_cb.table_page_id);
			TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Pin();      
			TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Unlock();   
			read_req.buf = (char*) TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).GetPage();


			ALWAYS_ASSERT(read_req.user_info.caller != NULL || read_req.user_info.do_user_cb);
			ALWAYS_ASSERT(TurboDB::GetBufMgr()->IsPinnedPage(read_req.user_info.db_info.table_page_id));
			//ALWAYS_ASSERT(read_req.user_info.func == NULL); 
			ALWAYS_ASSERT(read_req.buf != NULL);

			read_req.start_pos = read_req.user_info.db_info.page_id * TBGPP_PAGE_SIZE;
			read_req.io_size = TBGPP_PAGE_SIZE * 1;
			if(victim_table_pid_fid.pid != INVALID_PAGE){
				TurboDB::GetBufMgr()->GetDirectTable()->unlock(victim_table_pid_fid.pid);
			}
            std::atomic_fetch_add((std::atomic<int64_t>*) &TurboDB::GetBufMgr()->num_read_pages, +1L);
			bool success = DiskAioFactory::GetPtr()->ARead(read_req, (diskaio::DiskAioInterface*)req->user_info.read_my_io);
			if (!success) {
				std::cerr << "Turbo_DB::ReadPage, read page error" << std::endl;
				return;
			}
			ALWAYS_ASSERT (read_req.buf != NULL);

		}
		return;
	} else {
		AioRequest read_req;
		read_req.user_info.db_info.page_id = req->user_info.db_info_cb.page_id;
		read_req.user_info.db_info.file_id = req->user_info.db_info_cb.file_id;
		read_req.user_info.db_info.table_page_id = req->user_info.db_info_cb.table_page_id;
        read_req.user_info.db_info.e_type = req->user_info.db_info_cb.e_type;
        read_req.user_info.db_info.d_type = req->user_info.db_info_cb.d_type;
        read_req.user_info.db_info.version_id = req->user_info.db_info_cb.version_id;
		read_req.user_info.frame_id = req->user_info.frame_id;
		read_req.user_info.task_id = req->user_info.task_id;
		read_req.user_info.caller = req->user_info.caller;
        read_req.user_info.func = (void*) &InvokeUserCallback;
		read_req.user_info.do_user_cb = true;
		read_req.user_info.do_user_only_req_cb = false;
		read_req.buf = req->user_info.read_buf;
        INVARIANT(read_req.user_info.db_info.table_page_id != INVALID_PAGE);

		TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Lock();
		TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Clear();
		TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).SetPageID(req->user_info.db_info_cb.table_page_id);
		TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Pin();      
		TurboDB::GetBufMgr()->GetFrame(read_req.user_info.frame_id).Unlock();

		//ALWAYS_ASSERT(read_req.user_info.func == NULL); 

		ALWAYS_ASSERT(read_req.buf != NULL);
		read_req.start_pos = read_req.user_info.db_info.page_id * TBGPP_PAGE_SIZE;
		read_req.io_size = TBGPP_PAGE_SIZE * 1;

        std::atomic_fetch_add((std::atomic<int64_t>*) &TurboDB::GetBufMgr()->num_read_pages, +1L);
		bool success = DiskAioFactory::GetPtr()->ARead(read_req, (diskaio::DiskAioInterface*)req->user_info.read_my_io);
		if (!success) {
			std::cerr << "Turbo_DB::ReadPage, read page error" << std::endl;
			return;
		}
		ALWAYS_ASSERT (read_req.buf != NULL);
	}
	return;

}

void(*turbo_callback::AsyncCallbackUserReadFunction)(diskaio::DiskAioRequest* req) = AsyncCallbackUserRead;

turbo_buffer_manager::turbo_buffer_manager()
	: m_frames(NULL)
	, m_replacer(NULL)
	, m_pBufStartAddr(NULL)
	, m_NumFrames(0)
	, m_BytesPerSector(0)
    , m_NumPages(0) {

	m_frames = NULL;
	m_directTable = NULL;
	m_pBufStartAddr = NULL;
	m_replacer = NULL;

    num_read_pages = 0;
    num_write_pages = 0;
}

void AsyncCallbackUserWrite(diskaio::DiskAioRequest* req) { 
    return;
}

void turbo_buffer_manager::Close() {
	if(GetNumberOfAvailableFrames() != m_NumFrames) {
        fprintf(stdout, "GetNumberOfAvailableFrames() = %ld, m_NumFrames = %ld\n", GetNumberOfAvailableFrames(), m_NumFrames);
		//dump_list_of_pinned_frames();
		ALWAYS_ASSERT(0);
		ALWAYS_ASSERT(false);
	}

	if (m_directTable != NULL) {
		FlushDirtyPages();
		delete m_directTable;
		m_directTable = NULL;
	}

	if (m_frames != NULL) {
		NumaHelper::free_numa_memory(m_frames, sizeof(Turbo_Buffer_Frame) * m_NumFrames);
		m_frames = NULL;
	}

	if (m_pBufStartAddr != NULL) {
		NumaHelper::free_numa_memory (m_pBufStartAddr, m_BufAllocSize);
		m_pBufStartAddr = NULL;
	}
	LocalStatistics::register_mem_alloc_info("Page Buffer Pool", 0);

	if (m_replacer != NULL) {
		for (int i = 0; i < NumaHelper::num_sockets(); i++) {
			if (m_replacer[i] == NULL) continue;
			delete m_replacer[i];
		}
		delete[] m_replacer;
		m_replacer = NULL;
	}
}

turbo_buffer_manager::~turbo_buffer_manager() {
	Close();
}


ReturnStatus turbo_buffer_manager::Init(std::size_t buffer_pool_size_bytes, std::size_t num_pages, std::size_t bytes_per_sector) {
	Close();
	m_SystemPageSize = sysconf(_SC_PAGE_SIZE);
//	ALWAYS_ASSERT (m_SystemPageSize % TBGPP_PAGE_SIZE == 0);

	int64_t total_num_system_pages = (buffer_pool_size_bytes) / (m_SystemPageSize);
	total_num_system_pages = NumaHelper::num_sockets() * ((total_num_system_pages + NumaHelper::num_sockets() - 1) / NumaHelper::num_sockets());

	ALWAYS_ASSERT (total_num_system_pages % NumaHelper::num_sockets() == 0);

	m_NumFrames = (total_num_system_pages * m_SystemPageSize) / TBGPP_PAGE_SIZE;
	size_t num_sockets = NumaHelper::num_sockets();
	m_NumFramesPerSocket = m_NumFrames / num_sockets;
	m_BufAllocSize = m_NumFrames * TBGPP_PAGE_SIZE + TBGPP_PAGE_SIZE;
    //m_NumPages = num_pages;

	m_BytesPerSector = bytes_per_sector;

	ALWAYS_ASSERT (m_NumFrames % NumaHelper::num_sockets() == 0);

	m_pBufStartAddr = (char*) NumaHelper::alloc_numa_interleaved_memory(m_BufAllocSize);
	m_frames = (Turbo_Buffer_Frame*) NumaHelper::alloc_numa_interleaved_memory(sizeof(Turbo_Buffer_Frame) * m_NumFrames);

	if (m_pBufStartAddr == NULL || m_frames == NULL) {
		throw std::runtime_error("[Exception] TurboBufferManager memory allocation failed;\n");
	}

	for (int socket_id = 0; socket_id < NumaHelper::num_sockets(); socket_id++) {
		//numa_tonode_memory ( (char*) m_pBufStartAddr + socket_id * (TBGPP_PAGE_SIZE) * m_NumFramesPerSocket, TBGPP_PAGE_SIZE * m_NumFramesPerSocket, socket_id);
		//numa_tonode_memory ( (char*) m_frames + socket_id * sizeof(Turbo_Buffer_Frame) * m_NumFramesPerSocket, sizeof(Turbo_Buffer_Frame) * m_NumFramesPerSocket, socket_id);
	}

	std::size_t address = (std::size_t) m_pBufStartAddr;
	std::size_t ext = address % m_BytesPerSector;
	if(ext != 0) {
		ext = m_BytesPerSector - ext;
	}
	char * p = m_pBufStartAddr + ext;

	NumberOfUnpinnedFrames = m_NumFrames;

	// Touch the memory region
	#pragma omp parallel for
	for (std::size_t idx = 0; idx < m_NumFrames; idx++) {
		memset (m_pBufStartAddr + idx * TBGPP_PAGE_SIZE, -1, TBGPP_PAGE_SIZE);
		char * pCur = p + (std::size_t) TBGPP_PAGE_SIZE * idx;
		GetFrame(idx).Init((Page*)pCur, this);
	}
	LocalStatistics::register_mem_alloc_info("Page Buffer Pool", (m_BufAllocSize +sizeof(Turbo_Buffer_Frame) * m_NumFrames) / (1024*1024));

	// Page Table
	m_directTable = new turbo_direct_table(m_NumPages);

	// Replacer
	m_replacer = new Replacer*[NumaHelper::num_sockets()];
	for (int64_t i = 0; i < NumaHelper::num_sockets(); i++) {
		m_replacer[i] = NULL;
	}
	m_replacer[0] = new Turbo_Clock(m_NumFrames, &m_frames[0], m_directTable);
	for (int64_t i = 0; i < NumaHelper::num_sockets(); i++) {
        //		m_replacer[i] = new Turbo_Clock(m_NumFramesPerSocket, &m_frames[frame_from], m_directTable);
	}

	buffer_counters.resize(NUM_BUFFER_COUNTERS);
	for (int i = 0; i < NUM_BUFFER_COUNTERS; i++) {
		buffer_counters[i].hit = 0L;
		buffer_counters[i].miss = 0L;
	}

#if NO_COUNT_COMPULSORY_PAGE_MISS
	compulsory_miss.Init(m_NumPages);
	compulsory_miss.SetAll();
#endif

    num_read_pages = num_write_pages = 0;

	return OK;
}
    
void turbo_buffer_manager::ReportTimers(bool reset) {
    double total[16] = {0.0f};
    for (int tid = 0; tid < MAX_NUM_CPU_CORES; tid++) {
        turbo_timer& timer = timer_[tid].timer;
        for (int j = 0; j < 16; j++) {
            total[j] += timer.get_timer(j);
        }
        if (reset) timer.clear_all_timers();
    }    
#ifdef REPORT_PROFILING_TIMERS
    fprintf(stdout, "[%ld] %ld [BufMgr::ReportTimers] / %.3f %.3f %.3f %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, total[0], total[1], total[2], total[3], total[4], total[5], total[6]);
#endif
}

ReturnStatus turbo_buffer_manager::PinPageCallbackLikelyCached(
    PartitionID partition_id,
    AioRequest& req,
    diskaio::DiskAioInterface** my_io) {
	
    ENTER_BUF_MGR_CRIT_SECTION;
    ALWAYS_ASSERT(!req.user_info.do_user_cb);
    ALWAYS_ASSERT(req.user_info.db_info.version_id >= 0 && req.user_info.db_info.version_id < MAX_NUM_VERSIONS);
    
    EdgeType e_type = (EdgeType)req.user_info.db_info.e_type;
    DynamicDBType d_type = (DynamicDBType)req.user_info.db_info.d_type;
	
    PageID table_pid_to_read = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(req.user_info.db_info.version_id, req.user_info.db_info.page_id, e_type, d_type);
    //UpdateBufferHitCount(table_pid_to_read, true);
    //req.user_info.frame_id =  FindFrameAndPinPageIfCached(table_pid_to_read);
    //if (req.user_info.frame_id == INVALID_FRAME) {
    if (true) {
        return PinPageCallback(partition_id, req, my_io);
    }

    ALWAYS_ASSERT (GetFrame(req.user_info.frame_id).GetPageID() == table_pid_to_read);
    ALWAYS_ASSERT (req.user_info.frame_id == FindFrame(partition_id, table_pid_to_read));
    ALWAYS_ASSERT (IsPinnedPage(table_pid_to_read));
    
    //req.buf = (char*) GetFrame(req.user_info.frame_id).GetPage();
    //fprintf(stdout, "[%ld] %ld (%ld) [bfm::PinPageCallbackLikelyCached] %ld at %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, (int64_t) pthread_self(), table_pid_to_read, req.buf);
    return DONE;
}

ReturnStatus turbo_buffer_manager::PinPageCallback(
    PartitionID partition_id,
    AioRequest& req,
    diskaio::DiskAioInterface** my_io) {
	
    turbo_timer& timer = timer_[TG_ThreadContexts::thread_id].timer;

    timer.start_timer(0);

    ENTER_BUF_MGR_CRIT_SECTION;
retry:
    ALWAYS_ASSERT(req.user_info.db_info.version_id >= 0 && req.user_info.db_info.version_id < MAX_NUM_VERSIONS);
	req.user_info.frame_id = INVALID_FRAME;
	req.buf = NULL;
    EdgeType e_type = (EdgeType)req.user_info.db_info.e_type;
    DynamicDBType d_type = (DynamicDBType)req.user_info.db_info.d_type;
    PageID table_pid_to_read = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(req.user_info.db_info.version_id, req.user_info.db_info.page_id, e_type, d_type);
    req.user_info.db_info.file_id = TurboDB::GetBufMgr()->ConvertToFileID(req.user_info.db_info.version_id, (EdgeType)req.user_info.db_info.e_type, (DynamicDBType)req.user_info.db_info.d_type);

    timer.start_timer(1);
    while (!m_directTable->trylock(table_pid_to_read)) {
        my_io[WRITE_IO]->WaitForResponses(0);
        my_io[READ_IO]->WaitForResponses(0);
        _mm_pause();
    }
    timer.stop_timer(1);
	
    timer.start_timer(2);
    req.user_info.frame_id = FindFrame(partition_id, table_pid_to_read);
    timer.stop_timer(2);

	if (req.user_info.frame_id == INVALID_FRAME) {
        UpdateBufferHitCount(table_pid_to_read, false);

        // Page not in buffer.
        timer.start_timer(3);
        PageAndFrameID victim_table_pid_fid = PickVictim();
        req.user_info.frame_id = victim_table_pid_fid.fid; 
        timer.stop_timer(3);

        ALWAYS_ASSERT (req.user_info.frame_id != INVALID_FRAME);
        ALWAYS_ASSERT (!GetFrame(req.user_info.frame_id).IsReferenced());

        timer.start_timer(4);
        if (victim_table_pid_fid.pid != INVALID_PAGE) {
            while(!m_directTable->trylock(victim_table_pid_fid.pid)) {
                my_io[WRITE_IO]->WaitForResponses(0);
                my_io[READ_IO]->WaitForResponses(0);
                _mm_pause();
            }
        }
        timer.stop_timer(4);

        timer.start_timer(5);
        if (m_directTable->IsDirty(victim_table_pid_fid.pid)) {
            m_directTable->Insert(victim_table_pid_fid.pid, victim_table_pid_fid.fid);

            AioRequest write_req = req;
            write_req.user_info.do_user_cb = false;
            write_req.user_info.do_user_only_req_cb = true;
            write_req.user_info.db_info.table_page_id = victim_table_pid_fid.pid;

            // Request information for callback (Read after flush dirty page)
            write_req.user_info.func = (void*) &InvokeReadReq;	
            write_req.user_info.db_info_cb.page_id = req.user_info.db_info.page_id;
            write_req.user_info.db_info_cb.file_id = TurboDB::GetBufMgr()->ConvertToFileID(req.user_info.db_info.version_id, (EdgeType)req.user_info.db_info.e_type, (DynamicDBType)req.user_info.db_info.d_type);
            write_req.user_info.db_info_cb.table_page_id = table_pid_to_read;
            write_req.user_info.db_info_cb.e_type = e_type;
            write_req.user_info.db_info_cb.d_type = d_type;
            write_req.user_info.db_info_cb.version_id = req.user_info.db_info.version_id;
            INVARIANT(write_req.user_info.db_info.table_page_id != INVALID_PAGE);

            // Request information for flushing dirty page (convert table_pid --> pid and fid)
            TurboDB::ConvertDirectTablePageID(victim_table_pid_fid.pid, write_req.user_info.db_info.page_id, write_req.user_info.db_info.file_id);

            GetFrame(write_req.user_info.frame_id).SetPageID(victim_table_pid_fid.pid);
            GetFrame(write_req.user_info.frame_id).Pin();
            write_req.buf = (char*) GetFrame(write_req.user_info.frame_id).GetPage();
            write_req.user_info.read_my_io = (void*) my_io[READ_IO];
            write_req.user_info.read_buf = (char*) GetFrame(write_req.user_info.frame_id).GetPage(); //XXX

            GetFrame(req.user_info.frame_id).Unlock();

            m_directTable->ResetDirty(victim_table_pid_fid.pid);
            m_directTable->unlock(victim_table_pid_fid.pid);

            ALWAYS_ASSERT(write_req.user_info.func != NULL);
            ALWAYS_ASSERT(write_req.user_info.caller != NULL || !write_req.user_info.do_user_cb);
            ALWAYS_ASSERT(GetFrame(write_req.user_info.frame_id).IsPinned());
            ALWAYS_ASSERT(IsPinnedPage(victim_table_pid_fid.pid));
            ALWAYS_ASSERT(write_req.user_info.read_my_io != NULL);
            ALWAYS_ASSERT(write_req.buf != NULL);
            ALWAYS_ASSERT(write_req.user_info.read_buf != NULL);

            WritePage(write_req, my_io[WRITE_IO]);
        } else {
            GetFrame(req.user_info.frame_id).Clear();
            GetFrame(req.user_info.frame_id).SetPageID(table_pid_to_read);
            GetFrame(req.user_info.frame_id).Pin();
            req.buf = (char*) GetFrame(req.user_info.frame_id).GetPage();

            // Request information for read (victim page is not dirty page)
            req.user_info.db_info.file_id = TurboDB::GetBufMgr()->ConvertToFileID(req.user_info.db_info.version_id, (EdgeType)req.user_info.db_info.e_type, (DynamicDBType)req.user_info.db_info.d_type);
            req.user_info.db_info.table_page_id = table_pid_to_read; //XXX
            INVARIANT(table_pid_to_read != INVALID_PAGE);

            m_directTable->Insert(table_pid_to_read, req.user_info.frame_id);

            if (victim_table_pid_fid.pid != INVALID_PAGE) {
                m_directTable->unlock(victim_table_pid_fid.pid);
            }

            ALWAYS_ASSERT(req.user_info.caller != NULL || !req.user_info.do_user_cb);
            ALWAYS_ASSERT(IsPinnedPage(table_pid_to_read));
            ALWAYS_ASSERT(GetFrame(req.user_info.frame_id).IsPinned());
            ALWAYS_ASSERT(req.buf != NULL);

            GetFrame(req.user_info.frame_id).Unlock();
            req.user_info.do_user_cb = true; //XXX
            req.user_info.do_user_only_req_cb = false;
            ReadPage(req, my_io[READ_IO]);
        }
        timer.stop_timer(5);
        timer.stop_timer(0);
        return ON_GOING;
	} else {
		// When the page is already in the buffer
		// invoke callback explicitly
        timer.start_timer(6);
		GetFrame(req.user_info.frame_id).Lock();
		GetFrame(req.user_info.frame_id).Pin();
		m_directTable->Insert(table_pid_to_read, req.user_info.frame_id);
		m_directTable->unlock(table_pid_to_read);
		GetFrame(req.user_info.frame_id).Unlock();
		
		ALWAYS_ASSERT (GetFrame(req.user_info.frame_id).GetPageID() == table_pid_to_read);
		ALWAYS_ASSERT (req.user_info.frame_id == FindFrame(partition_id, table_pid_to_read));
        ALWAYS_ASSERT( IsPinnedPage(table_pid_to_read) );
		
		req.buf = (char*) GetFrame(req.user_info.frame_id).GetPage();
        UpdateBufferHitCount(table_pid_to_read, true);

		if (req.user_info.do_user_cb) {
			turbo_callback::AsyncCallbackUserFunction(req.user_info);
		}
        timer.stop_timer(6);
        
        timer.stop_timer(0);
        return DONE;
	}
	LOG_ASSERT(false);
    return OK;
}


ReturnStatus turbo_buffer_manager::PrePinPageUnsafe(PartitionID partition_id, AioRequest& req) {
	ENTER_BUF_MGR_CRIT_SECTION;

	req.user_info.frame_id = INVALID_FRAME;
	req.buf = NULL;
    PageID table_page_id = req.user_info.db_info.table_page_id;

	req.user_info.frame_id = FindFrame(partition_id, table_page_id);

	if (req.user_info.frame_id == INVALID_FRAME) {
		LOG_ASSERT(false);
        return FAIL;
	} else {
		GetFrame(req.user_info.frame_id).PinUnsafe();
		m_directTable->Insert(table_page_id, req.user_info.frame_id);
		
        ALWAYS_ASSERT(GetFrame(req.user_info.frame_id).GetPageID() == table_page_id);
		ALWAYS_ASSERT(GetFrame(req.user_info.frame_id).IsPinned());
		
        req.buf = (char*) GetFrame(req.user_info.frame_id).GetPage();
		if (req.user_info.do_user_cb)
			turbo_callback::AsyncCallbackUserFunction(req.user_info);
		
        UpdateBufferHitCount(table_page_id, true);
	}
	return OK;
}
ReturnStatus turbo_buffer_manager::PrePinPage(PartitionID partition_id, AioRequest& req) {
	ENTER_BUF_MGR_CRIT_SECTION;

	req.user_info.frame_id = INVALID_FRAME;
	req.buf = NULL;
    EdgeType e_type = (EdgeType)req.user_info.db_info.e_type;
    DynamicDBType d_type = (DynamicDBType)req.user_info.db_info.d_type;
    PageID table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(req.user_info.db_info.version_id, req.user_info.db_info.page_id, e_type, d_type);

	m_directTable->lock(table_page_id);
	req.user_info.frame_id = FindFrame(partition_id, table_page_id);

	if (req.user_info.frame_id == INVALID_FRAME) {
		m_directTable->unlock(table_page_id);
		return FAIL;
	} else {
		UpdateBufferHitCount(table_page_id, true);
		GetFrame(req.user_info.frame_id).Lock();
		GetFrame(req.user_info.frame_id).Pin();
		ALWAYS_ASSERT (GetFrame(req.user_info.frame_id).GetPageID() == table_page_id);
		m_directTable->Insert(table_page_id, req.user_info.frame_id);
		ALWAYS_ASSERT(GetFrame(req.user_info.frame_id).IsPinned());
		req.buf = (char*) GetFrame(req.user_info.frame_id).GetPage();
		if (req.user_info.do_user_cb)
			turbo_callback::AsyncCallbackUserFunction(req.user_info);
		m_directTable->unlock(table_page_id);
		GetFrame(req.user_info.frame_id).Unlock();
	}
	return OK;
}
ReturnStatus turbo_buffer_manager::ReplacePageCallback (
    PartitionID partition_id,
    AioRequest& req,
    PageID old_table_pid,
    bool& replaced,
    int old_version,
    diskaio::DiskAioInterface** my_io) {

    EdgeType e_type = (EdgeType)req.user_info.db_info.e_type;
    DynamicDBType d_type = (DynamicDBType)req.user_info.db_info.d_type;
    PageID old_table_page_id = old_table_pid;
    PageID new_table_page_id = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(req.user_info.db_info.version_id, req.user_info.db_info.page_id, e_type, d_type); //XXX

	ALWAYS_ASSERT (partition_id == PartitionStatistics::my_machine_id());
	ALWAYS_ASSERT (new_table_page_id >= 0 && new_table_page_id < m_NumPages);
	ALWAYS_ASSERT (old_table_page_id >= 0 && old_table_page_id < m_NumPages);

	ENTER_BUF_MGR_CRIT_SECTION;

	FrameID new_frameNo;

	m_directTable->lock(old_table_page_id);
	req.user_info.frame_id = FindFrame(partition_id, old_table_page_id);

	ALWAYS_ASSERT(req.user_info.frame_id != INVALID_FRAME);
	ALWAYS_ASSERT(GetFrame(req.user_info.frame_id).GetPageID() == old_table_page_id );
	ALWAYS_ASSERT(GetFrame(req.user_info.frame_id).IsPinned());

	GetFrame(req.user_info.frame_id).Lock();
	GetFrame(req.user_info.frame_id).Unpin();

	//bool realloc_numa_local_memory = (TG_ThreadContexts::socket_id != GetSocketIdByFrameId(frameNo));
	bool realloc_numa_local_memory = false;
	if (GetFrame(req.user_info.frame_id).IsPinned() || realloc_numa_local_memory) {
		m_directTable->unlock(old_table_page_id);
		GetFrame(req.user_info.frame_id).Unlock();
		Page* pPage = NULL;
		AioRequest new_req = req;
		new_req.buf = (char*) pPage;
		new_req.user_info.do_user_cb = true;
		replaced = false;
		return PinPageCallback(partition_id, new_req, my_io);
	}
	replaced = true;

	AioRequest new_req = req;

	m_directTable->lock(new_table_page_id);

	new_req.user_info.frame_id = FindFrame(partition_id, new_table_page_id);

	if (new_req.user_info.frame_id == INVALID_FRAME) {
		UpdateBufferHitCount(new_table_page_id, false);
		new_req.user_info.frame_id = req.user_info.frame_id;

		if(m_directTable->IsDirty(old_table_page_id)){ 
			GetFrame(req.user_info.frame_id).SetPageID(old_table_page_id);
			GetFrame(req.user_info.frame_id).Pin();
			new_req.buf = (char*) GetFrame(req.user_info.frame_id).GetPage();

			AioRequest write_req;

            // Request information for callback (Read after flush dirty page)
			write_req.user_info = req.user_info;
            write_req.user_info.db_info.table_page_id = old_table_page_id;
            write_req.user_info.do_user_cb = false;
			write_req.user_info.do_user_only_req_cb = true;
			write_req.user_info.func = (void*) &InvokeReadReq;	
			write_req.user_info.db_info_cb.page_id = req.user_info.db_info.page_id;
            write_req.user_info.db_info_cb.file_id = TurboDB::GetBufMgr()->ConvertToFileID(req.user_info.db_info.version_id, (EdgeType)req.user_info.db_info.e_type, (DynamicDBType)req.user_info.db_info.d_type);
            write_req.user_info.db_info_cb.table_page_id = new_table_page_id;
            write_req.user_info.db_info_cb.e_type = e_type;
            write_req.user_info.db_info_cb.d_type = d_type;
            write_req.user_info.db_info_cb.version_id = req.user_info.db_info.version_id;
            INVARIANT(write_req.user_info.db_info.table_page_id != INVALID_PAGE);

            // Request information for flushing dirty page (page_id and file_id)
            TurboDB::ConvertDirectTablePageID(old_table_page_id, write_req.user_info.db_info.page_id, write_req.user_info.db_info.file_id);

			write_req.user_info.read_buf = new_req.buf;
			write_req.buf = new_req.buf;
			write_req.user_info.read_my_io = (void*) my_io[READ_IO];

			ALWAYS_ASSERT(write_req.user_info.func != NULL);
			ALWAYS_ASSERT(write_req.user_info.caller != NULL || !write_req.user_info.do_user_cb);
			ALWAYS_ASSERT(IsPinnedPage(old_table_page_id));
			ALWAYS_ASSERT(write_req.buf != NULL);
			ALWAYS_ASSERT(write_req.user_info.read_buf != NULL);

			GetFrame(write_req.user_info.frame_id).Unlock();

			m_directTable->ResetDirty(old_table_page_id);
			m_directTable->unlock(old_table_page_id);

			WritePage(write_req, my_io[WRITE_IO]);
		} else {
			new_req.user_info.do_user_cb = true;
			new_req.user_info.do_user_only_req_cb = false;
			ALWAYS_ASSERT (new_req.user_info.caller != NULL);

			GetFrame(new_req.user_info.frame_id).Clear();
			GetFrame(new_req.user_info.frame_id).SetPageID(new_table_page_id);
			GetFrame(new_req.user_info.frame_id).Pin();
			new_req.buf = (char*) GetFrame(new_req.user_info.frame_id).GetPage();
                
            // Request information for read (victim page is not dirty page)
            new_req.user_info.db_info.file_id = TurboDB::GetBufMgr()->ConvertToFileID(req.user_info.db_info.version_id, (EdgeType)req.user_info.db_info.e_type, (DynamicDBType)req.user_info.db_info.d_type);
            new_req.user_info.db_info.table_page_id = new_table_page_id; 
            INVARIANT(new_table_page_id != INVALID_PAGE);

			m_directTable->unlock(old_table_page_id);
			GetFrame(new_req.user_info.frame_id).Unlock();

			ReadPage(new_req, my_io[READ_IO]);
		}
		return ON_GOING;
	} else {
		UpdateBufferHitCount(new_table_page_id, true);
		GetFrame(new_req.user_info.frame_id).Lock();
		GetFrame(new_req.user_info.frame_id).Pin();
		ALWAYS_ASSERT (GetFrame(new_req.user_info.frame_id).GetPageID() == new_table_page_id);

		m_directTable->Insert(new_table_page_id, new_req.user_info.frame_id);
		m_directTable->unlock(old_table_page_id);
		GetFrame(req.user_info.frame_id).Unlock();
		m_directTable->unlock(new_table_page_id);
		GetFrame(new_req.user_info.frame_id).Unlock();

		turbo_callback::AsyncCallbackUserFunction(new_req.user_info);
	}
	return OK;
}

ReturnStatus turbo_buffer_manager::GetPagePtr_Unsafe(PageID pid, Page*& page) {
	PartitionID partition_id = PartitionStatistics::my_machine_id();
	FrameID frameNo = FindFrame(partition_id, pid);

	if (frameNo == INVALID_FRAME || !GetFrame(frameNo).IsPinned()) {
        fprintf(stdout, "[%ld] %ld (%ld) [bfm::GetPagePtr_Unsafe] %ld at %p, FrameNo = %d\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, (int64_t) pthread_self(), pid, page, frameNo);
		LOG_ASSERT(false);
	}
	page = GetFrame(frameNo).GetPage();

	ALWAYS_ASSERT(GetFrame(frameNo).GetPageID() == pid);
	ALWAYS_ASSERT(page != NULL);
    //fprintf(stdout, "[%ld] %ld (%ld) [bfm::GetPagePtr_Unsafe] %ld at %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, (int64_t) pthread_self(), pid, page);
	return OK;
}

ReturnStatus turbo_buffer_manager::UnpinPageUnsafeBulk(PartitionID partition_id, PageID* pids, int num_pages) {
	if (num_pages == 0) return OK;
	ENTER_BUF_MGR_CRIT_SECTION;
	int cnts = 0;	
	for (int idx = 0; idx < num_pages; idx++) {
		PageID pid = pids[idx];
		ALWAYS_ASSERT(pid >= 0 && pid < m_NumPages);
		FrameID frameNo = m_directTable->LookUp(pid);
		ALWAYS_ASSERT(frameNo != INVALID_FRAME);
		ALWAYS_ASSERT(GetFrame(frameNo).GetPageID() == pid );
		ALWAYS_ASSERT(GetFrame(frameNo).IsPinned());
		bool pin_count_zero = false;
		GetFrame(frameNo).UnpinUnsafeWithoutUpdatingNumFrames(pin_count_zero);
		if (pin_count_zero) cnts++;
	}
        NumberOfUnpinnedFrames += cnts;
	return OK;
}

ReturnStatus turbo_buffer_manager::UnpinPageUnsafe( PartitionID partition_id, PageID pid ) {
	ALWAYS_ASSERT(pid >= 0 && pid < m_NumPages);
	ENTER_BUF_MGR_CRIT_SECTION;
	FrameID frameNo = m_directTable->LookUp(pid);
	ALWAYS_ASSERT(frameNo != INVALID_FRAME);
	ALWAYS_ASSERT(GetFrame(frameNo).GetPageID() == pid );
	ALWAYS_ASSERT(GetFrame(frameNo).IsPinned());
	GetFrame(frameNo).UnpinUnsafe();
	return OK;
}

ReturnStatus turbo_buffer_manager::UnpinPage( PartitionID partition_id, PageID pid ) {
	ALWAYS_ASSERT(partition_id == PartitionStatistics::my_machine_id());
	ALWAYS_ASSERT(pid >= 0 && pid < m_NumPages);
	ENTER_BUF_MGR_CRIT_SECTION;

	FrameID frameNo;

	m_directTable->lock(pid);
	frameNo = FindFrame(partition_id, pid);

	ALWAYS_ASSERT(frameNo != INVALID_FRAME);
	ALWAYS_ASSERT(GetFrame(frameNo).GetPageID() == pid );
	ALWAYS_ASSERT(GetFrame(frameNo).IsPinned());

	GetFrame(frameNo).Lock();
	GetFrame(frameNo).Unpin();
	m_directTable->unlock(pid);
	GetFrame(frameNo).Unlock();
	return OK;
}

ReturnStatus turbo_buffer_manager::UnpinPages(BitMap<PageID>& bitmap) {
	for (PageID pid = 0; pid < m_NumPages; pid++) {
		if (bitmap.Get(pid)) {
			UnpinPage(PartitionStatistics::my_machine_id(), pid);
		}
	}
}

ReturnStatus turbo_buffer_manager::UnpinAllPages() {
	for (FrameID i = 0; i < m_NumFrames; i++) {
		if (GetFrame(i).IsPinned()) {
			GetFrame(i).Unpin();
		}
	}
}

void turbo_buffer_manager::FlushDirtyPages() {
    diskaio::DiskAioInterface* my_io = DiskAioFactory::GetPtr()->GetAioInterface(0);
    for(PageID idx = 0; idx < m_directTable->GetNumEntry(); idx++){
        if(!m_directTable->IsDirty(idx)) continue;
        AioRequest req;
        req.user_info.do_user_cb = false;
        req.user_info.do_user_only_req_cb = false;
        req.user_info.func = ((void*) &AsyncCallbackUserWrite);
        TurboDB::ConvertDirectTablePageID(idx, req.user_info.db_info.page_id, req.user_info.db_info.file_id);
        req.user_info.db_info.table_page_id = idx;

        FrameID fid = FindFrame(PartitionStatistics::my_machine_id(), idx);
        if(fid != INVALID_FRAME){
            req.buf = (char*) GetFrame(fid).GetPageForClose();
            WritePage(req, NULL);
            m_directTable->ResetDirty(idx);
        }
    }
    my_io->WaitForResponses();
}

void turbo_buffer_manager::ClearAllFrames() {
	#pragma omp parallel for
	for (std::size_t idx = 0; idx < m_NumFrames; idx++) {
        GetFrame(idx).Lock();
        GetFrame(idx).Clear();
        ALWAYS_ASSERT(GetFrame(idx).GetPageID() == INVALID_PAGE);
		GetFrame(idx).Unlock();
	}
}

FrameID turbo_buffer_manager::FindFrameAndPinPageIfCached(PageID pid) {
	FrameID frameNo = m_directTable->LookUp(pid);
	if (frameNo == INVALID_FRAME) {
        return INVALID_FRAME;
    } else {
		Turbo_Buffer_Frame& frame = GetFrame(frameNo);
        frame.Lock();
        if (frameNo == m_directTable->LookUp(pid) && frame.GetPageID() == pid) {
            frame.Pin();
            //fprintf(stdout, "[%ld] %ld (%ld) [bfm::FindFrameAndPinPageIfCached] %ld at %p\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, (int64_t) pthread_self(), pid, frame.GetPage());
            frame.Unlock();
            ALWAYS_ASSERT(frameNo == m_directTable->LookUp(pid) && frame.GetPageID() == pid);
            return frameNo;
        } else {
            frame.Unlock();
            return INVALID_FRAME;
        }
    }
}

FrameID turbo_buffer_manager::FindFrame( PartitionID partition_id, PageID pid ) {
	FrameID frameNo = m_directTable->LookUp(pid);
	if (frameNo != INVALID_FRAME) {
		if (GetFrame(frameNo).GetPageID() == pid) {
			return frameNo;
		} else {
			bool take_lock = false;
			if (!m_directTable->IsLocked(pid)) {
				m_directTable->lock(pid);
				take_lock = true;
			}
			m_directTable->ClearFrameReference(pid);
			if (take_lock) {
				m_directTable->unlock(pid);
			}
			return INVALID_FRAME;
		}
    }
	return INVALID_FRAME;
}

FrameID turbo_buffer_manager::GetNumberOfAvailableFrames() {
	return NumberOfUnpinnedFrames.load();
}

FrameID turbo_buffer_manager::GetNumberOfPinnedFrames() {
	return m_NumFrames - NumberOfUnpinnedFrames.load();
}

FrameID turbo_buffer_manager::GetNumberOfFrames() {
	return m_NumFrames;
}

size_t turbo_buffer_manager::GetNumberOfTotalPages() {
    return m_NumPages;
}

void turbo_buffer_manager::ResetPageReadIO() {
    num_read_pages = 0;
    
}

void turbo_buffer_manager::ResetPageWriteIO() {
    num_write_pages = 0;
}
int64_t turbo_buffer_manager::GetPageReadIO() {
    int64_t prio = num_read_pages * TBGPP_PAGE_SIZE;
    return prio;
}

int64_t turbo_buffer_manager::GetPageWriteIO() {
    int64_t pwio = num_write_pages * TBGPP_PAGE_SIZE;
    return pwio;
}

void turbo_buffer_manager::dump_list_of_pinned_frames() {
	FrameID cnt = 0;
	for (FrameID i = 0; i < m_NumFrames; i++) {
		if (GetFrame(i).IsPinned()) {
			cnt++;
			std::cout << "[" << PartitionStatistics::my_machine_id() << "]"
			          << ",frame_id: " << i
			          << ", page_id: " << GetFrame(i).GetPageID()
			          << std::endl << std::flush;
		}
	}
}

char* turbo_buffer_manager::begin_of_buffer() {
	return (char*) m_frames[0].m_data;
}

char* turbo_buffer_manager::end_of_buffer() {
	return ((char*)m_frames[m_NumFrames - 1].m_data) + TBGPP_PAGE_SIZE;
}

bool turbo_buffer_manager::contains(char* ptr) {
	char* begin = begin_of_buffer();
	char* end = end_of_buffer();
	return (ptr >= begin && ptr < end);
}

PageAndFrameID turbo_buffer_manager::PickVictim() {
	ALWAYS_ASSERT (TG_ThreadContexts::socket_id == NumaHelper::get_socket_id_by_core_id(TG_ThreadContexts::core_id));
	ALWAYS_ASSERT (TG_ThreadContexts::socket_id >= 0 && TG_ThreadContexts::socket_id < NumaHelper::num_sockets());
	return m_replacer[0]->PickVictim();
	//return (TG_ThreadContexts::socket_id * m_NumFramesPerSocket) + m_replacer[TG_ThreadContexts::socket_id]->PickVictim();
}

int64_t turbo_buffer_manager::GetSocketIdByFrameId(FrameID fid) {
	ALWAYS_ASSERT (m_NumFramesPerSocket > 0);
	return (fid / m_NumFramesPerSocket);
}

Turbo_Buffer_Frame& turbo_buffer_manager::GetFrame(FrameID fid) {
	ALWAYS_ASSERT(fid >= 0 && fid < m_NumFrames);
	return m_frames[fid];
}

bool turbo_buffer_manager::IsPinnedPage(PageID pid) {
	ALWAYS_ASSERT(pid >= 0 && pid < m_NumPages);
	FrameID frameid = FindFrame(PartitionStatistics::my_machine_id(), pid);
	return GetFrame(frameid).IsPinned();
}

bool turbo_buffer_manager::IsPageInMemory(PageID pid) {
	ALWAYS_ASSERT(pid >= 0 && pid < m_NumPages);
	FrameID frameid = FindFrame(PartitionStatistics::my_machine_id(), pid);
	if (frameid == INVALID_FRAME || !GetFrame(frameid).IsInMemory()) {
		//ALWAYS_ASSERT (!GetFrame(frameid).IsInMemory());
		return false;
	} else {
		ALWAYS_ASSERT (GetFrame(frameid).IsInMemory());
		return true;
	}
}

ReturnStatus turbo_buffer_manager::ReadPage(AioRequest& req, diskaio::DiskAioInterface* my_io) {
	ALWAYS_ASSERT(req.buf != NULL);
	req.start_pos = req.user_info.db_info.page_id * TBGPP_PAGE_SIZE;
	req.io_size = TBGPP_PAGE_SIZE * 1;
    
    ALWAYS_ASSERT(req.user_info.db_info.file_id != -1);
    ALWAYS_ASSERT(req.user_info.db_info.page_id != -1);
    ALWAYS_ASSERT(req.user_info.db_info.table_page_id != -1);
    ALWAYS_ASSERT(req.user_info.db_info.version_id != -1);
    ALWAYS_ASSERT(req.user_info.db_info.e_type != -1);
    ALWAYS_ASSERT(req.user_info.db_info.d_type != -1);

    std::atomic_fetch_add((std::atomic<int64_t>*) &num_read_pages, +1L);
	bool success = DiskAioFactory::GetPtr()->ARead(req, my_io);
	if (!success) {
		std::cerr << "Turbo_DB::ReadPage, read page error" << std::endl;
		return FAIL;
	}
	ALWAYS_ASSERT (req.buf != NULL);
	return OK;
}

ReturnStatus turbo_buffer_manager::WritePage(AioRequest& req, diskaio::DiskAioInterface* my_io) {
	ALWAYS_ASSERT(req.buf != NULL);
	req.start_pos = req.user_info.db_info.page_id * TBGPP_PAGE_SIZE;
	req.io_size = TBGPP_PAGE_SIZE * 1;
    INVARIANT(req.user_info.db_info.table_page_id != INVALID_PAGE);

    std::atomic_fetch_add((std::atomic<int64_t>*) &num_write_pages, +1L);
	bool success = DiskAioFactory::GetPtr()->AWrite(req, my_io);
	if (!success) {
		std::cerr << "Turbo_DB::WritePage, write page error" << std::endl;
		return FAIL;
	}
	ALWAYS_ASSERT (req.buf != NULL);
	return OK;
}

ReturnStatus turbo_buffer_manager::OpenCDB(const char* file_path, int version_id, EdgeType e_type, DynamicDBType d_type, bool bDirectIO, int flag) {
	flag = bDirectIO ? flag | O_DIRECT : flag;
	file_ids[e_type][d_type][version_id] = DiskAioFactory::GetPtr()->OpenAioFile(file_path, flag);
    m_NumPages += (DiskAioFactory::GetPtr()->GetAioFileSize(file_ids[e_type][d_type][version_id]) + TBGPP_PAGE_SIZE - 1) / TBGPP_PAGE_SIZE;
	if (file_ids[e_type][d_type][version_id] < 0) {
		std::cerr << "[TurboDB] Failed to open the file : " << file_path << std::endl;
		return FAIL;
	}
	return OK;
}

ReturnStatus turbo_buffer_manager::CloseCDB(bool rm, int version_id, EdgeType e_type, DynamicDBType d_type) {
    if (file_ids[e_type][d_type][version_id] >= 0) {
		DiskAioFactory::GetPtr()->CloseAioFile(file_ids[e_type][d_type][version_id]);
        file_ids[e_type][d_type][version_id] = -1;
    }
	return OK;
}

void turbo_buffer_manager::GetPageBufferHitMissBytes(int64_t& hit, int64_t& miss) {
	int64_t total_hit = 0;
	int64_t total_miss = 0;
	for (int i = 0; i < NUM_BUFFER_COUNTERS; i++) {
		total_hit += buffer_counters[i].hit;
		total_miss += buffer_counters[i].miss;
	}

	hit = total_hit * TBGPP_PAGE_SIZE;
	miss = total_miss * TBGPP_PAGE_SIZE;
	return;
}


void turbo_buffer_manager::GetPageBufferHitMissRatio(double& hit, double& miss) {
	int64_t total_hit = 0;
	int64_t total_miss = 0;
	for (int i = 0; i < NUM_BUFFER_COUNTERS; i++) {
		total_hit += buffer_counters[i].hit;
		total_miss += buffer_counters[i].miss;
	}

	hit = (double) total_hit / (double) (total_hit + total_miss);
	miss = (double) total_miss / (double) (total_hit + total_miss);

	return;
}

void turbo_buffer_manager::UpdateBufferHitCount(PageID pid, bool hit) {
	int64_t idx = pid % NUM_BUFFER_COUNTERS;
	if (hit) {
		std::atomic_fetch_add((std::atomic<int64_t>*) &buffer_counters[idx].hit, 1L);
	} else {
#if NO_COUNT_COMPULSORY_PAGE_MISS
		if (compulsory_miss.Get_Atomic(pid)) {
			compulsory_miss.Clear_Atomic(pid);
		} else {
			std::atomic_fetch_add((std::atomic<int64_t>*) &buffer_counters[idx].miss, 1L);
		}
#else
		std::atomic_fetch_add((std::atomic<int64_t>*) &buffer_counters[idx].miss, 1L);
#endif
	}
	return;
}

void turbo_buffer_manager::GetSnapshotOfResidentPages (TwoLevelBitMap<PageID>& bitmap) {
	#pragma omp parallel for
	for (FrameID i = 0; i < m_NumFrames; i++) {
		if (!GetFrame(i).IsInMemory()) continue;
		ALWAYS_ASSERT (IsPageInMemory(GetFrame(i).m_current_page_id));
		bitmap.Set_Atomic(GetFrame(i).m_current_page_id);
	}
}

void turbo_buffer_manager::GetHitPages (TwoLevelBitMap<PageID>& input_page, TwoLevelBitMap<PageID>& hit_page) {
	/*#pragma omp parallel for
	for (FrameID i = 0; i < m_NumFrames; i++) {
		if (!GetFrame(i).IsInMemory()) continue;
		ALWAYS_ASSERT (IsPageInMemory(GetFrame(i).m_current_page_id));
		PageID pid = GetFrame(i).m_current_page_id;
		if (input_page.Get(pid)) hit_page.Set_Atomic(pid);
	}*/
    // XXX correctness check need
    input_page.InvokeIfMarked([&](PageID table_pid) {
        FrameID frameNo = m_directTable->LookUp(table_pid);
        if (frameNo != INVALID_FRAME) {
            if (GetFrame(frameNo).GetPageID() == table_pid) {
                //hit_page.Set_Atomic(table_pid);
                hit_page.Set(table_pid);
            }
        }
    });

}

turbo_direct_table* turbo_buffer_manager::GetDirectTable() {
    return m_directTable;
}

int turbo_buffer_manager::ConvertToFileID(int version_id, EdgeType e_type, DynamicDBType d_type, bool use_fulllist_db) {
    if (UserArguments::USE_FULLIST_DB && use_fulllist_db) {
        e_type = (e_type == OUTEDGE) ? OUTEDGEFULLLIST : (e_type == INEDGE) ? INEDGEFULLLIST : e_type;
    }
    return file_ids[e_type][d_type][version_id];
}

