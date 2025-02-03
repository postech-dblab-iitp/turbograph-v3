#include "analytics/core/TurboDB.hpp"
TurboDB* TurboDB::smpTurboDB[2];

turbo_buffer_manager& TurboDB::buf_mgr_ = turbo_buffer_manager::buf_mgr;
std::vector<Range<node_t>> TurboDB::EdgeSubchunksVidRange;
DBStatistics TurboDB::db_statistics[2][MAX_NUM_VERSIONS];
int32_t* TurboDB::out_degree_vector_buf = NULL;
int32_t* TurboDB::out_insert_degree_vector_buf = NULL;
int32_t* TurboDB::out_delete_degree_vector_buf = NULL;
MemoryMappedArray<int32_t> TurboDB::out_degree_vector_;
MemoryMappedArray<int32_t> TurboDB::out_insert_degree_vector_;
MemoryMappedArray<int32_t> TurboDB::out_delete_degree_vector_;
bool TurboDB::is_out_degree_vector_initialized_ = false;
int TurboDB::db_type;
int64_t TurboDB::num_pages_loaded = 0;
std::vector<std::vector<node_t>> TurboDB::graph_in_memory;

static int GetNumOfCores() {
	return std::thread::hardware_concurrency();
}

static long long getAvailableMemorySizeInMB() { // there is not std function which returns available memory size.
	/*MEMORYSTATUSEX statex;
	  statex.dwLength = sizeof(statex);
	  GlobalMemoryStatusEx(&statex);
	  return statex.ullAvailPhys / DIV;*/
	return -1;
}
static long long getInputFileSize(const char *filename, bool &success) {
	abort();
	return -1;
}

static long long dumpMemoryStat() {
	abort();
	return 0;
}

TurboDB::TurboDB()  {
	PartitionStatistics::my_num_internal_nodes() = -1;
	/*PartitionStatistics::in_my_num_edges() = -1;
	PartitionStatistics::out_my_num_edges() = -1;*/ //XXX - tslee
	PartitionStatistics::my_first_node_id() = -1;
	PartitionStatistics::my_last_node_id() = -1;

	mDBPath = "";
	//smpTurboDB = this;
}

TurboDB::~TurboDB() {
    CloseDB(false);
}

ReturnStatus TurboDB::LoadDB(const char* dir_name, const RunConfig& config, EdgeType e_type_, DynamicDBType d_type, Range<int> version) {
    mConfigure = config;
    mDBPath = std::string(dir_name);
    smpTurboDB[e_type_] = this;

    LoadIdx(dir_name, e_type_);

    if (d_type == ALL) {
        system_fprintf(0, stdout, "Load Insert and Delete DB\n");
        UserArguments::LOAD_DELETEDB = true;
        LoadDB(dir_name, e_type_, INSERT, version);
        LoadDB(dir_name, e_type_, DELETE, version);
        LoadFullListDB(dir_name, e_type_, INSERT, version);
        LoadFullListDB(dir_name, e_type_, DELETE, version);
    } else {
        UserArguments::LOAD_DELETEDB = false;
        LoadDB(dir_name, e_type_, d_type, version);
        LoadFullListDB(dir_name, e_type_, d_type, version);
        if (d_type == INSERT) system_fprintf(0, stdout, "Load Insert DB(e_type %d) Complete\n", (int)e_type_);
        if (d_type == DELETE) system_fprintf(0, stdout, "Load Delete DB(e_type %d) Complete\n", (int)e_type_);
    }
    return ReturnStatus::OK;
}

ReturnStatus TurboDB::LoadDB(const char* dir_name, EdgeType e_type_, DynamicDBType d_type, Range<int> version) {
    bool prev_use_fullist_db = UserArguments::USE_FULLIST_DB;
    UserArguments::USE_FULLIST_DB = false;
    ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);
    PageID num_pages;
    PageID first_table_pid = num_pages_loaded;
    int num_loaded_db = 0;
    e_type = e_type_;

    int insert_version_begin_id = version.GetBegin(), delete_version_begin_id = version.GetBegin();
    int version_begin_id, version_end_id = version.GetEnd();

    std::string db_path;
    if (d_type == INSERT) {
        db_path = std::string(dir_name) + "/Insert";
        version_begin_id = insert_version_begin_id;
        
        insert_version_existence.Init(version.GetEnd() + 1);
        insert_version_existence.ClearAll();
    } else if (d_type == DELETE) {
        db_path = std::string(dir_name) + "/Delete";
        version_begin_id = delete_version_begin_id;
        
        delete_version_existence.Init(version.GetEnd() + 1);
        delete_version_existence.ClearAll();
    } else {
        LOG_ASSERT(false);
    }
    
    FirstPageIds[d_type].resize(MAX_NUM_VERSIONS);
    vid_range_per_page_[d_type].resize(MAX_NUM_VERSIONS);
    accumulated_num_pages_[d_type].resize(MAX_NUM_VERSIONS, num_pages_loaded);
    
/*
    if (versions.GetBegin() < 0 || version.GetEnd() < versions.GetBegin()) {
        printf("version begin: %d, version end: %d\n", versions.GetBegin(), versions.GetEnd());
    }
    INVARIANT(version.GetBegin() >= 0 && version.GetEnd() >= version.GetBegin());
*/
    for (int version_id = version_begin_id; version_id <= version_end_id; version_id++) {
        std::string file_name_prefix = db_path + "/" + std::to_string(version_id);
        if (!CheckFileExistence(file_name_prefix, R_OK | W_OK)) {
            TurboDB::db_statistics[d_type][version_id].init();
            TurboDB::db_statistics[d_type][version_id].my_num_pages(e_type) = 0;
            TurboDB::db_statistics[d_type][version_id].init_replicate(e_type);
            TurboDB::db_statistics[d_type][version_id].replicate(e_type);
            for (int i = 0; i < PartitionStatistics::num_target_vector_chunks(); i++) {
                for (int j = 0; j < PartitionStatistics::num_subchunks_per_edge_chunk(); j++) {
                    int64_t idx = i * PartitionStatistics::num_subchunks_per_edge_chunk() + j;
                    TurboDB::db_statistics[d_type][version_id].my_num_pages(idx, e_type) = 0;
                }
            }
            continue;
        }
        
        if (d_type == INSERT) insert_version_existence.Set(version_id);
        else if (d_type == DELETE) delete_version_existence.Set(version_id);
        num_loaded_db++;

        std::string edgedb_file_name = file_name_prefix + "/edgedb/edgedb";
        std::string firstpageids_file_name = file_name_prefix + "/FirstPageIds.txt";
        std::string vidrangeperpage_file_name = file_name_prefix + "/edgedbVidRangePerPage";

        // 1. Open Edge DB
        OpenFile(edgedb_file_name.c_str(), version_id, e_type, d_type);

        // 2. Open First Page IDs
        _Init_FirstPageIds(firstpageids_file_name, version_id, d_type);

        // 3. Init DB Statistics for each DB
        TurboDB::db_statistics[d_type][version_id].init();

        // 4. Replicate the statistics across partitions
        int64_t num_edges = -1;

        // 5. set number of pages
        INVARIANT (DiskAioFactory::GetPtr() != NULL);
        num_pages = 0;
        ALWAYS_ASSERT (DiskAioFactory::GetPtr()->GetAioFileSize(TurboDB::GetBufMgr()->ConvertToFileID(version_id, e_type, d_type, false)) % TBGPP_PAGE_SIZE == 0);
        num_pages = (DiskAioFactory::GetPtr()->GetAioFileSize(TurboDB::GetBufMgr()->ConvertToFileID(version_id, e_type, d_type, false)) + TBGPP_PAGE_SIZE - 1) / TBGPP_PAGE_SIZE;
        TurboDB::db_statistics[d_type][version_id].my_num_pages(e_type) = num_pages; //XXX in + out?
        num_pages_loaded += num_pages;

        // 6. Open VidRangePerPage
        _Init_VidRangePerPage(vidrangeperpage_file_name, version_id, d_type);

        // 7. Replicate all the data in 'PartitionStatistics'
        TurboDB::db_statistics[d_type][version_id].init_replicate(e_type);
        TurboDB::db_statistics[d_type][version_id].replicate(e_type);
        for (int i = 0; i < PartitionStatistics::num_target_vector_chunks(); i++) {
            for (int j = 0; j < PartitionStatistics::num_subchunks_per_edge_chunk(); j++) {
                int64_t idx = i * PartitionStatistics::num_subchunks_per_edge_chunk() + j;
                if (idx < PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk() - 1) {
                    TurboDB::db_statistics[d_type][version_id].my_num_pages(idx, e_type) = GetFirstPageId(idx+1, version_id, d_type) - GetFirstPageId(idx, version_id, d_type);
								} else {
                    TurboDB::db_statistics[d_type][version_id].my_num_pages(idx, e_type) = num_pages - GetFirstPageId(idx, version_id, d_type);
								}
            }
        }
        TurboDB::db_statistics[d_type][version_id].set_accumulated_num_pages(e_type);

        CheckDbConsistency();
    }

    for (int i = 1; i < MAX_NUM_VERSIONS; i++) {
        accumulated_num_pages_[d_type][i] = accumulated_num_pages_[d_type][i - 1] + TurboDB::db_statistics[d_type][i - 1].my_num_pages(e_type);
    }
		UserArguments::USE_FULLIST_DB = prev_use_fullist_db;
    fprintf(stdout, "[%ld] Load Partial List DB (%s, %s) %d of %d versions loaded, table_pid range [%d, %d)\n", PartitionStatistics::my_machine_id(), (e_type_ == OUTEDGE) ? "OUTEDGE" : "INEDGE", (d_type == INSERT) ? "INSERT" : "DELETE", num_loaded_db, version_end_id - version_begin_id + 1, first_table_pid, num_pages_loaded);
    return ReturnStatus::OK;
}

ReturnStatus TurboDB::LoadFullListDB(const char* dir_name, EdgeType e_type_, DynamicDBType d_type, Range<int> version) {
    int insert_version_begin_id = version.GetBegin(), delete_version_begin_id = version.GetBegin();
    int version_begin_id, version_end_id = version.GetEnd();
    int num_loaded_fulllistdb = 0;
		PageID first_table_pid = num_pages_loaded;
    std::string db_path;
    EdgeType e_type_for_fulllist;
    if (e_type_ == OUTEDGE) e_type_for_fulllist = OUTEDGEFULLLIST;
    else if (e_type_ == INEDGE) e_type_for_fulllist = INEDGEFULLLIST;
    
    vid_range_per_page_full_list_[d_type].resize(MAX_NUM_VERSIONS);
    
    if (d_type == INSERT) {
        db_path = std::string(dir_name) + "/Insert";
        version_begin_id = insert_version_begin_id;
        
        insert_version_existence_for_full_list.Init(version.GetEnd() + 1);
        insert_version_existence_for_full_list.ClearAll();
    } else if (d_type == DELETE) {
        db_path = std::string(dir_name) + "/Delete";
        version_begin_id = delete_version_begin_id;
        
        delete_version_existence_for_full_list.Init(version.GetEnd() + 1);
        delete_version_existence_for_full_list.ClearAll();
    }
    accumulated_num_pages_full_list_[d_type].resize(MAX_NUM_VERSIONS, num_pages_loaded);

    for (int version_id = version_begin_id; version_id <= version_end_id; version_id++) {
        std::string file_name_prefix = db_path + "/" + std::to_string(version_id);
        std::string full_list_db_file_name = file_name_prefix + "/edgedb/edgedbForFullList";
        std::string full_list_vidrangeperpage_file_name_prefix = file_name_prefix + "/edgedbVidRangePerPageForFullList";
        std::string full_list_vidrangeperpage_file_name = file_name_prefix + "/edgedbVidRangePerPageForFullList0";
        
        if (!UserArguments::USE_FULLIST_DB) continue;
        if (!is_version_exist(d_type, version_id)) continue;
        if (!CheckFileExistence(full_list_db_file_name, R_OK) || !CheckFileExistence(full_list_vidrangeperpage_file_name, R_OK)) {
            if (UserArguments::USE_FULLIST_DB) {
                fprintf(stdout, "[%ld] UserArguments::USE_FULLIST_DB == true, but there is no full list DB\n", PartitionStatistics::my_machine_id());
                INVARIANT(false);
            }
            continue;
        }
    
        if (d_type == INSERT) insert_version_existence_for_full_list.Set(version_id);
        else if (d_type == DELETE) delete_version_existence_for_full_list.Set(version_id);
        
        OpenFile(full_list_db_file_name.c_str(), version_id, e_type_for_fulllist, d_type);
        INVARIANT (DiskAioFactory::GetPtr() != NULL);
        
        ALWAYS_ASSERT (DiskAioFactory::GetPtr()->GetAioFileSize(TurboDB::GetBufMgr()->ConvertToFileID(version_id, e_type_for_fulllist, d_type)) % TBGPP_PAGE_SIZE == 0);
        int64_t num_pages = (DiskAioFactory::GetPtr()->GetAioFileSize(TurboDB::GetBufMgr()->ConvertToFileID(version_id, e_type_for_fulllist, d_type)) + TBGPP_PAGE_SIZE - 1) / TBGPP_PAGE_SIZE;
        
        TurboDB::db_statistics[d_type][version_id].my_num_pages(e_type_for_fulllist) = num_pages;
        num_pages_loaded += num_pages;
        TurboDB::vid_range_per_page_full_list_[d_type][version_id].OpenVidRangePerPage(1, full_list_vidrangeperpage_file_name_prefix);
        TurboDB::vid_range_per_page_full_list_[d_type][version_id].Load();

        is_full_list_db_existed_ = true;
        num_loaded_fulllistdb++;
    } 
    for (int i = 1; i < MAX_NUM_VERSIONS; i++) {
        accumulated_num_pages_full_list_[d_type][i] = accumulated_num_pages_full_list_[d_type][i - 1] + TurboDB::db_statistics[d_type][i - 1].my_num_pages(e_type_for_fulllist);
    }
    fprintf(stdout, "[%ld] Load Full List DB (%s, %s) %d of %d versions loaded, table_pid range [%d, %d)\n", PartitionStatistics::my_machine_id(), (e_type_ == OUTEDGE) ? "OUTEDGE" : "INEDGE", (d_type == INSERT) ? "INSERT" : "DELETE", num_loaded_fulllistdb, version_end_id - version_begin_id + 1, first_table_pid, num_pages_loaded);

    return ReturnStatus::OK;
}

ReturnStatus TurboDB::LoadIdx(const char* dir_name, EdgeType e_type_) { //XXX idx directory
    std::string dir = std::string(dir_name) + "/Insert";
    
    // Open PartitionRanges and SubchunkRanges. Assume that 0 version(original graph) always exists
    std::string partition_range_file_name = dir + "/0/PartitionRanges/";
    std::string edge_subchunks_vid_range_file_name = dir + "/0/EdgeSubChunkVidRanges/";
    //std::string oldtonewvidmapping_file_name = mDBPath + "/0/OldtoNewVidMapping";
    
    if (e_type_ == OUTEDGE) {
        _Init_PartitionRanges(partition_range_file_name);
        _Init_EdgeSubchunks(edge_subchunks_vid_range_file_name);
        PartitionStatistics::replicate();
    }
    
    FirstPageIds.resize(2);
    accumulated_num_pages_.resize(2);
    accumulated_num_pages_full_list_.resize(2);
    vid_range_per_page_.resize(2);
    vid_range_per_page_full_list_.resize(2);
    return ReturnStatus::OK;
}

// XXX - bool rm = true?
ReturnStatus TurboDB::CloseDB(bool rm, DynamicDBType d_type_, Range<int> version) {
	if (mDBPath.compare("") == 0) {
		return ReturnStatus::OK;
	}
	int err;
	std::string file_name_prefix(mDBPath.c_str());
    if (d_type_ == INSERT) file_name_prefix = file_name_prefix + "/Insert";
    else if (d_type_ == DELETE) file_name_prefix = file_name_prefix + "/Delete";

	std::string edgedb_file_name = file_name_prefix + "/edgedb/edgedb";
	std::string firstpageids_file_name = file_name_prefix + "/FirstPageIds.txt";
	std::string vidrangeperpage_file_name = file_name_prefix + "/edgedbVidRangePerPage";
	std::string partition_range_file_name = file_name_prefix + "/PartitionRanges/";
	std::string oldtonewvidmapping_file_name = file_name_prefix + "/OldtoNewVidMapping";
	std::string outdegree_dir_name = file_name_prefix + "/OutDegreeVector";
	std::string outdegree_file_name = file_name_prefix + "/OutDegreeVector/SeqReadVector";
	std::string edge_subchunks_vid_range_file_name = file_name_prefix + "/EdgeSubChunkVidRanges/" + "tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt";

	// 0. Close BufMgr
    if (true) buf_mgr_.Close(); //XXX now assume that InEdge will be deleted finally
    //if (e_type == INEDGE) buf_mgr_.Close(); //XXX now assume that InEdge will be deleted finally

    for (int version_id = version.GetBegin(); version_id <= version.GetEnd(); version_id++) {
        // 1. Close EdgeDB File
        CloseFile(rm, version_id, e_type, d_type_);
        
        // 2. Clear First Page IDs
        FirstPageIds[d_type_][version_id].clear();
        /*if (rm) {
            err = std::remove(firstpageids_file_name.c_str());
            if (err != 0) {
                fprintf(stdout, "Cannot delete %s ErrorNo=%d\n", firstpageids_file_name.c_str(), errno);
            }
        }*/

        // 3. Close and clear VidRangePerPage
        //temp//TurboDB::vid_range_per_page_[version_id].Close(rm); //XXX

        //temp//TurboDB::db_statistics[version_id].close();
    }

	// 4. Close and clear PartitionRanges
	PartitionStatistics::my_first_node_id() = -1;
	PartitionStatistics::my_last_node_id() = -1;
	PartitionStatistics::my_num_internal_nodes() = -1;
    //db_statistics.close(); //XXX - tslee
	/*PartitionStatistics::in_my_num_edges() = 0;
	PartitionStatistics::out_my_num_edges() = 0;
	PartitionStatistics::out_my_num_pages() = 0;
	PartitionStatistics::my_num_pages() = 0;*/

	// 5. Close and clear EdgeSubchunksVidRange
	if (true) EdgeSubchunksVidRange.clear(); //XXX
	//if (e_type == INEDGE) EdgeSubchunksVidRange.clear(); //XXX
	if (rm) {
		remove(edge_subchunks_vid_range_file_name.c_str());
	}

    if (d_type_ == INSERT) out_insert_degree_vector_.Close();
    else if (d_type_ == DELETE) out_delete_degree_vector_.Close();
    else if (d_type_ == ALL) out_degree_vector_.Close();
    mDBPath = "";
    return ReturnStatus::OK;
}

ReturnStatus TurboDB::_Init_OutDegreeTable(std::string& dir_path, std::string& file_path, DynamicDBType d_type) {
 

    
    /* // by syko - 2019.04.03
    MemoryMappedArray<node_t>* out_degree_vector;
    if (d_type == INSERT) out_degree_vector = &TurboDB::out_insert_degree_vector_;
    else if (d_type == DELETE) out_degree_vector = &TurboDB::out_delete_degree_vector_;
    else if (d_type == ALL) out_degree_vector = &TurboDB::out_degree_vector_;

	mode_t old_umask;
	old_umask = umask(0);
	int is_error = mkdir((dir_path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
	if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
		fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (dir_path).c_str(), errno);
		umask(old_umask);
		assert(false);
		//throw unsupported_exception();
	}
	umask(old_umask);

	// Check and open if exists
	bool exist = true;
	int res = access(file_path.c_str(), R_OK);
	if (res < 0) {
		if (errno == ENOENT) {
			exist = false;
			if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "OutDegree file does not exist\n");
		} else if (errno == EACCES) {
			exist = false;
			if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "OutDegree file exists, but cannot read it\n");
			assert(false);
			//throw unsupported_exception();
		}
	}

	if (exist) {
		ReturnStatus rt = out_degree_vector->Open(file_path.c_str(), false);
		INVARIANT (rt == ReturnStatus::OK);
		if (UserArguments::ITERATOR_MODE == FULL_LIST) {
			// TODO - lock a chunk at a time
			out_degree_vector->Mlock(false);
			LocalStatistics::register_mem_alloc_info("OutDegreeVector (Full List)", (out_degree_vector_.length() * sizeof(node_t)) / (1024*1024));
		} else {
		}
		//is_out_degree_vector_initialized_ = true;
        return ReturnStatus::OK;
	} else {
		// Otherwise, create one and set flag that it is not initialized!
		ReturnStatus rt = out_degree_vector->Create(file_path.c_str(), PartitionStatistics::my_num_internal_nodes());
		INVARIANT (rt == ReturnStatus::OK);
		//is_out_degree_vector_initialized_ = false;
        return ReturnStatus::FAIL;
	}
    */
	return ReturnStatus::OK;
}

ReturnStatus TurboDB::_Init_BufferManager(int64_t buffer_pool_size_bytes) {
	LocalStatistics::page_buffer_size_in_bytes() = buffer_pool_size_bytes;
	//PageID num_pages = (DiskAioFactory::GetPtr()->GetTotalAioFileSize() + TBGPP_PAGE_SIZE - 1) / TBGPP_PAGE_SIZE;
	ALWAYS_ASSERT (buffer_pool_size_bytes > 0);
	//ALWAYS_ASSERT (num_pages > 0);
	buf_mgr_.Init(buffer_pool_size_bytes, 0, TBGPP_PAGE_SIZE); //XXX - tslee
	return ReturnStatus::OK;
}

ReturnStatus TurboDB::_Init_VidRangePerPage(std::string& file_path, int version_id, DynamicDBType d_type) {
	TurboDB::vid_range_per_page_[d_type][version_id].OpenVidRangePerPage(PartitionStatistics::my_total_num_subchunks(), file_path);
	TurboDB::vid_range_per_page_[d_type][version_id].Load();
	return ReturnStatus::OK;
}


ReturnStatus TurboDB::_Init_FirstPageIds(std::string& file_path, int version_id, DynamicDBType d_type) {
    //fprintf(stdout, "[%ld] FirstPageIds loading from %s\n", PartitionStatistics::my_machine_id(), file_path.c_str());
	TextFormatSequentialReader reader(file_path.c_str());
	FirstPageIds[d_type][version_id].clear();

	std::string line;

	int partition_idx;
	PageID first_page_id;
	while (reader.getNext(line) == ReturnStatus::OK) {
		std::istringstream iss(line);
		iss >> partition_idx >> first_page_id;
		FirstPageIds[d_type][version_id].push_back(first_page_id);
        //fprintf(stdout, "[%ld] FirstPageIds[%d][%d] = %ld\n", PartitionStatistics::my_machine_id(), (int)d_type, version_id, first_page_id);
	}
    //fprintf(stdout, "[%ld] sizeof(FirstPageIds[%d][%d]) = %ld\n", PartitionStatistics::my_machine_id(), (int)d_type, version_id, FirstPageIds[d_type][version_id].size());
	reader.Close(false);
	return ReturnStatus::OK;
}

ReturnStatus TurboDB::_Init_PartitionRanges(std::string& file_path) {
	// XXX-ihna
	std::vector<std::string> rangefile_list;
	DiskAioFactory::GetAioFileList(file_path, rangefile_list);
	for(int64_t i = 0 ; i < rangefile_list.size() ; i++)
		rangefile_list[i] = file_path + rangefile_list[i];
	MetaDataReader range_reader(rangefile_list);
	PartitionStatistics::my_first_node_id() = range_reader.FirstNodeId();
	PartitionStatistics::my_last_node_id() = range_reader.LastNodeId();
	PartitionStatistics::my_num_internal_nodes() = range_reader.MyNumNodes();

	return ReturnStatus::OK;
}

ReturnStatus TurboDB::_Init_EdgeSubchunks(std::string& file_path) {
	std::string temp_str;
	std::stringstream temp_stream;
	node_t begin, end;

	int64_t total_num_lines = 0;
	{
		std::string file_name(file_path + "tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt");
		std::ifstream subrange_file(file_name.c_str());
		if (!subrange_file.good()) {
			fprintf(stdout, "Failed to open %s\n", file_name.c_str());
		}
		INVARIANT(subrange_file.good());
		std::string unused;
		while ( std::getline(subrange_file, unused) ) {
			total_num_lines++;
		}
		subrange_file.close();
	}
	INVARIANT (total_num_lines % PartitionStatistics::num_machines() == 0);
	INVARIANT (total_num_lines % UserArguments::VECTOR_PARTITIONS == 0);
	INVARIANT (total_num_lines == PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS * UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK);

	std::ifstream subrange_file((file_path + "tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt").c_str());


	PartitionStatistics::num_subchunks_per_edge_chunk() = total_num_lines / PartitionStatistics::num_machines() / UserArguments::VECTOR_PARTITIONS;
	PartitionStatistics::my_total_num_subchunks() = total_num_lines;

	EdgeSubchunksVidRange.resize(PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_subchunks_per_edge_chunk());
	for (int i = 0; i < PartitionStatistics::num_machines() ; i++) {
		for (int j = 0; j < UserArguments::VECTOR_PARTITIONS; j++) {
			for (int k = 0; k < PartitionStatistics::num_subchunks_per_edge_chunk(); k++) {
				int64_t idx = i * UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_subchunks_per_edge_chunk() + j * PartitionStatistics::num_subchunks_per_edge_chunk() + k;
				getline(subrange_file, temp_str);
				temp_stream.clear();
				temp_stream.str(temp_str);
				temp_stream >> begin >> end;
				EdgeSubchunksVidRange[idx].SetBegin(begin);
				EdgeSubchunksVidRange[idx].SetEnd(end);
			}
		}
	}
	subrange_file.close();
	return ReturnStatus::OK;
}

void TurboDB::CheckDbConsistency() {
	// Check Vid Range of Edge Subchunks
	for (int i = 0; i < PartitionStatistics::num_machines() ; i++) {
		for (int j = 0; j < UserArguments::VECTOR_PARTITIONS; j++) {
			int64_t idx = i * UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_subchunks_per_edge_chunk() + (j + 1) * PartitionStatistics::num_subchunks_per_edge_chunk() - 1;
			INVARIANT(EdgeSubchunksVidRange[idx].GetEnd() == PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i, j).GetEnd());
		}
	}
}

bool TurboDB::CheckFileExistence(std::string file_path, int mode) {
    int is_exist = access(file_path.c_str(), mode);
    if (is_exist < 0) {
        if (errno == ENOENT) {
            //fprintf(stdout, "[%ld] Directory %s not exists\n", PartitionStatistics::my_machine_id(), file_name_prefix.c_str());
        } else if (errno == EACCES) {
            fprintf(stdout, "[%ld] Directory %s exists, but cannot read or write it\n", PartitionStatistics::my_machine_id(), file_path.c_str());
            INVARIANT(false);
        }
        return false;
    } else {
        return true;
    }
}

ReturnStatus TurboDB::_GetGraphDataFileLists(const char* dir_name, std::vector<std::string>& edgelist_file_lists_, std::vector<std::string>& metadata_file_lists_) {

	std::string path_to_dir(dir_name);
	std::string out_edges_dir = path_to_dir +"/OutEdges/";
	std::string partition_ranges_dir = path_to_dir + "/PartitionRanges/";

	bool read_files_with_its_partition_id_as_postfix = false;

	if (read_files_with_its_partition_id_as_postfix) {
		std::string in_partition_id_in_str = std::to_string((int64_t)PartitionStatistics::my_machine_id() + 1);
		edgelist_file_lists_.push_back(out_edges_dir + in_partition_id_in_str);
		metadata_file_lists_.push_back(partition_ranges_dir + in_partition_id_in_str);
	} else {
		DiskAioFactory::GetAioFileList(out_edges_dir, edgelist_file_lists_);
		DiskAioFactory::GetAioFileList(partition_ranges_dir, metadata_file_lists_);
		for(int i=0; i<edgelist_file_lists_.size() ; i++) {
			edgelist_file_lists_[i] = out_edges_dir + edgelist_file_lists_[i];
			metadata_file_lists_[i] = partition_ranges_dir + metadata_file_lists_[i];
		}
	}
	return ReturnStatus::OK;
}

ReturnStatus TurboDB::_AggregateMetadata() {
	return ReturnStatus::OK;
}

ReturnStatus TurboDB::_ReadMetaTable(Turbo_bin_mmapper& metadata_file_reader, node_t& first_node,
                                     node_t& last_node, node_t& num_nodes, node_t& num_edges, node_t& num_cut_nodes) {
	ReturnStatus st;
	char* pData = NULL;
	st = metadata_file_reader.ReadNext(sizeof(node_t), pData);
	if (st == FAIL) LOG(INFO) << "[TurboDB::_ReadMetaTable() failed to read 'first_node'";
	first_node = *(node_t *)pData;

	st = metadata_file_reader.ReadNext(sizeof(node_t), pData);
	if (st == FAIL) LOG(INFO) << "[TurboDB::_ReadMetaTable() failed to read 'last_node'";
	last_node = *(node_t *)pData;

	st = metadata_file_reader.ReadNext(sizeof(node_t), pData);
	if (st == FAIL) LOG(INFO) << "[TurboDB::_ReadMetaTable() failed to read 'num_nodes'";
	num_nodes = *(node_t *)pData;

	st = metadata_file_reader.ReadNext(sizeof(node_t), pData);
	if (st == FAIL) LOG(INFO) << "[TurboDB::_ReadMetaTable() failed to read 'num_edges'";
	num_edges = *(node_t *)pData;

	st = metadata_file_reader.ReadNext(sizeof(node_t), pData);
	if (st == FAIL) LOG(INFO) << "[TurboDB::_ReadMetaTable() failed to read 'num_cut_nodes'";
	num_cut_nodes = *(node_t *)pData;

	return ReturnStatus::OK;
}

TurboDB* TurboDB::GetTurboDB(EdgeType e_type) {
    if (e_type == OUTEDGEFULLLIST) return smpTurboDB[OUTEDGE];
    else if (e_type == INEDGEFULLLIST) return smpTurboDB[INEDGE];
    else return smpTurboDB[e_type];
    return NULL;
}

turbo_buffer_manager* TurboDB::GetBufMgr() {
	return &TurboDB::buf_mgr_;
}

DBStatistics* TurboDB::GetDBStatistics(int version, DynamicDBType d_type) {
    ALWAYS_ASSERT ((int) d_type < 2);
    ALWAYS_ASSERT (version < MAX_NUM_VERSIONS);
    return &TurboDB::db_statistics[d_type][version];
}

void TurboDB::SetDBType(EdgeType e_type, DynamicDBType d_type) {
    TurboDB::db_type = 3 * (int)e_type + (int)d_type;
}

VidRangePerPage& TurboDB::GetVidRangePerPage(int version, DynamicDBType d_type, bool use_fulllist_db) {
    if (UserArguments::USE_FULLIST_DB) return vid_range_per_page_full_list_[d_type][version];
    return vid_range_per_page_[d_type][version];
}

VidRangePerPage& TurboDB::GetVidRangePerPageFullList(int version, DynamicDBType d_type) {
	return vid_range_per_page_full_list_[d_type][version];
}


ReturnStatus TurboDB::OpenFile(const char* file_name, int version_id, EdgeType e_type, DynamicDBType d_type) {
	buf_mgr_.OpenCDB(file_name, version_id, e_type, d_type);
	return ReturnStatus::OK;
}
ReturnStatus TurboDB::CloseFile(bool rm, int version_id, EdgeType e_type, DynamicDBType d_type) {
	buf_mgr_.CloseCDB(rm, version_id, e_type, d_type);
	return ReturnStatus::OK;
}

PageID TurboDB::GetFirstPageId (PartID i, PartID j) {
	ALWAYS_ASSERT (false);
	ALWAYS_ASSERT (i >= 0 && i < PartitionStatistics::num_target_vector_chunks());
	ALWAYS_ASSERT (j >= 0 && j < PartitionStatistics::num_target_vector_chunks());
	PartID partition_id = i * UserArguments::VECTOR_PARTITIONS + j;
	return GetFirstPageId(partition_id);
}

PageID TurboDB::GetLastPageId (PartID i, PartID j) {
	ALWAYS_ASSERT (false);
	ALWAYS_ASSERT (i >= 0 && i < PartitionStatistics::num_target_vector_chunks());
	ALWAYS_ASSERT (j >= 0 && j < PartitionStatistics::num_target_vector_chunks());
	PartID partition_id = i * UserArguments::VECTOR_PARTITIONS + j;
	return GetLastPageId(partition_id);
}

PageID TurboDB::GetFirstPageId (PartID partition_id, int version_id, DynamicDBType d_type) {
    ALWAYS_ASSERT(partition_id >= 0 && partition_id < PartitionStatistics::my_total_num_subchunks() * PartitionStatistics::num_subchunks_per_edge_chunk());
    ALWAYS_ASSERT((int)d_type >= 0 && (int)d_type < 2);
    ALWAYS_ASSERT(partition_id >= 0 && partition_id < FirstPageIds[d_type][version_id].size());
    if (UserArguments::USE_FULLIST_DB) return 0;
    return FirstPageIds[d_type][version_id][partition_id];
}

PageID TurboDB::GetFirstPageIdFullList (int version_id, DynamicDBType d_type) {
    return 0;
}

PageID TurboDB::GetLastPageId (PartID partition_id, int version_id, DynamicDBType d_type, bool use_fulllist_db) {
    if (!is_version_exist(d_type, version_id)) return -1;
    if (UserArguments::USE_FULLIST_DB)  return GetLastPageIdFullList(version_id, d_type);
    ALWAYS_ASSERT(partition_id >= 0 && partition_id < PartitionStatistics::my_total_num_subchunks() * PartitionStatistics::num_subchunks_per_edge_chunk());
    if (partition_id < PartitionStatistics::my_total_num_subchunks() - 1) {
        ALWAYS_ASSERT(FirstPageIds[d_type][version_id][partition_id + 1] - 1 < TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type));
        return FirstPageIds[d_type][version_id][partition_id + 1] - 1;
    } else {
        return TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type) - 1;
    }
}

PageID TurboDB::GetLastPageIdFullList (int version_id, DynamicDBType d_type) {
    if (e_type == OUTEDGE) return TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(OUTEDGEFULLLIST) - 1;
    else if (e_type == INEDGE) return TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(INEDGEFULLLIST) - 1;
    return -1;
}

PageID TurboDB::ConvertToDirectTablePageID (int version_id, PageID pid, EdgeType e_type, DynamicDBType d_type) {
    ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);
    ALWAYS_ASSERT(version_id >= 0 && version_id < accumulated_num_pages_[d_type].size());
    if (e_type == OUTEDGEFULLLIST || e_type == INEDGEFULLLIST || UserArguments::USE_FULLIST_DB)  // TODO remove conditions related to e_type
        return accumulated_num_pages_full_list_[d_type][version_id] + pid;
    else 
        return accumulated_num_pages_[d_type][version_id] + pid;
}
    
PageID TurboDB::ConvertDirectTablePageIDToPageID (int version_id, PageID table_pid, EdgeType e_type, DynamicDBType d_type) {
    PageID pid = -1;
    if (e_type == OUTEDGEFULLLIST || e_type == INEDGEFULLLIST || UserArguments::USE_FULLIST_DB) {
        pid = table_pid - accumulated_num_pages_full_list_[d_type][version_id];
    } else {
        pid = table_pid - accumulated_num_pages_[d_type][version_id];
    }
    ALWAYS_ASSERT (pid >= 0);
    return pid;
}

void TurboDB::ConvertDirectTablePageIDToVersionID (PageID table_pid, PageID& pid, int& version_id, DynamicDBType d_type) {
    for (int64_t i = 1; i < MAX_NUM_VERSIONS; i++) {
        if (accumulated_num_pages_[d_type][i] > table_pid) {
            version_id = i - 1;
            INVARIANT(table_pid >= accumulated_num_pages_[d_type][i - 1]);
            pid = table_pid - accumulated_num_pages_[d_type][i - 1];
            return;
        }
    }
    INVARIANT(false);
}

void TurboDB::ConvertDirectTablePageID (PageID table_pid, PageID& pid, int& fid) {
    if (FindPidOffset(table_pid, pid, fid, OUTEDGE, INSERT) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, OUTEDGE, DELETE) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, OUTEDGEFULLLIST, INSERT) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, OUTEDGEFULLLIST, DELETE) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, INEDGE, INSERT) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, INEDGE, DELETE) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, INEDGEFULLLIST, INSERT) == ReturnStatus::OK) return;
    if (FindPidOffset(table_pid, pid, fid, INEDGEFULLLIST, DELETE) == ReturnStatus::OK) return;
    INVARIANT(false);
}

ReturnStatus TurboDB::FindPidOffset (PageID table_pid, PageID& pid, int& fid, EdgeType e_type, DynamicDBType d_type) {
    int version_id;

    for (int64_t i = 1; i < MAX_NUM_VERSIONS; i++) {
        PageID pid_offset_ceil = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(i, 0, e_type, d_type);
        if (pid_offset_ceil > table_pid) {
            version_id = i - 1;
            PageID pid_offset_floor = TurboDB::GetTurboDB(e_type)->ConvertToDirectTablePageID(version_id, 0, e_type, d_type);
						if (table_pid < pid_offset_floor) {
							fprintf(stdout, "[%ld] table_pid = %d, pid_offset_floor = %d, version_id = %d, e_type = %d, d_type = %d USE_FULLIST_DB = %s\n", PartitionStatistics::my_machine_id(), table_pid, pid_offset_floor, version_id, (int) e_type, (int) d_type, UserArguments::USE_FULLIST_DB ? "true" : "false");
						}
            INVARIANT(table_pid >= pid_offset_floor);
            pid = table_pid - pid_offset_floor;
            fid = TurboDB::GetBufMgr()->ConvertToFileID(version_id, e_type, d_type);
            return ReturnStatus::OK;
        }
    }
    return ReturnStatus::FAIL;
}

struct GetPageRangeByVidComparatonr {
	bool operator() (Range<node_t>& left, Range<node_t>& right) {
		return right.contains(left);
	}
	bool operator() (Range<node_t>& left, const Range<node_t>& right) {
		return right.contains(left);
	}
	bool operator() (const Range<node_t>& left, const Range<node_t>& right) {
		return right.contains(left);
	}
};

Range<PageID> TurboDB::GetPageRangeByVid(int edge_partition_id, node_t src_vid, int version_id, EdgeType e_type, DynamicDBType d_type) {
	Range<PageID> pid_range(-1,-1);
    bool use_fulllist_db = UserArguments::USE_FULLIST_DB;
    if (!is_version_exist(d_type, version_id)) return pid_range;
    if (use_fulllist_db && !is_version_exist_full_list(d_type, version_id)) return pid_range;
    VidRangePerPage& vidrangeperpage = GetVidRangePerPage(version_id, d_type, use_fulllist_db);
	// TODO Optimization
	int64_t low = 0;
	int64_t high, mid;
    int64_t num_pages;
    if (use_fulllist_db) {
        num_pages = TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type) -1;
        high = num_pages;
        while(low <= high) {
            mid = (low + high)/2;
            if(vidrangeperpage.Get(mid).contains(src_vid))
                break;
            else if(vidrangeperpage.Get(mid).GetBegin() > src_vid) {
                high = mid-1;
            } else if(vidrangeperpage.Get(mid).GetEnd() < src_vid) {
                low = mid+1;
            } else {
                ALWAYS_ASSERT(0);
            }
        }
        if( low > high) {
            ALWAYS_ASSERT(pid_range == Range<PageID> (-1,-1));
            //fprintf(stdout, "[%ld] not found [%d, %d]\n", PartitionStatistics::my_machine_id(), pid_range.GetBegin(), pid_range.GetEnd());
            return pid_range;
        }
        pid_range.Set(mid, mid);
        for(PageID pid = mid; pid >= 0; pid--) {
            if(vidrangeperpage.Get(pid).contains(src_vid))
                pid_range.SetBegin(pid);
            else
                break;
        }
        for(PageID pid = mid; pid < num_pages; pid++) {
            if(vidrangeperpage.Get(pid).contains(src_vid))
                pid_range.SetEnd(pid);
            else
                break;
        }
        pid_range.SetBegin(pid_range.GetBegin() + GetFirstPageId(edge_partition_id, version_id, d_type));
        pid_range.SetEnd(pid_range.GetEnd() + GetFirstPageId(edge_partition_id, version_id, d_type));
        ALWAYS_ASSERT(pid_range.GetBegin() >= 0 && pid_range.GetBegin() < TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type));
        ALWAYS_ASSERT(pid_range.GetEnd() >= 0 && pid_range.GetEnd() < TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type));
        ALWAYS_ASSERT(pid_range.GetEnd() >= pid_range.GetBegin());

        ALWAYS_ASSERT (vidrangeperpage.Get(pid_range.GetBegin()).contains(src_vid));
        ALWAYS_ASSERT (vidrangeperpage.Get(pid_range.GetEnd()).contains(src_vid));

        //fprintf(stdout, "[%ld] Vertex %ld in pid_range [%ld, %ld]\n", PartitionStatistics::my_machine_id(), src_vid, pid_range.GetBegin(), pid_range.GetEnd());
        return pid_range;
    } else {
        num_pages = TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(edge_partition_id, e_type) -1;
        high = num_pages;
        while(low <= high) {
            mid = (low + high)/2;
            if(vidrangeperpage.Get(edge_partition_id, mid).contains(src_vid))
                break;
            else if(vidrangeperpage.Get(edge_partition_id, mid).GetBegin() > src_vid) {
                high = mid-1;
            } else if(vidrangeperpage.Get(edge_partition_id, mid).GetEnd() < src_vid) {
                low = mid+1;
            } else {
                ALWAYS_ASSERT(0);
            }
        }
        if( low > high) {
            ALWAYS_ASSERT(pid_range == Range<PageID> (-1,-1));
            //fprintf(stdout, "[%ld] not found [%d, %d]\n", PartitionStatistics::my_machine_id(), pid_range.GetBegin(), pid_range.GetEnd());
            return pid_range;
        }
        pid_range.Set(mid, mid);
        for(PageID pid = mid; pid >= 0; pid--) {
            if(vidrangeperpage.Get(edge_partition_id, pid).contains(src_vid))
                pid_range.SetBegin(pid);
            else
                break;
        }
        for(PageID pid = mid; pid < num_pages; pid++) {
            if(vidrangeperpage.Get(edge_partition_id, pid).contains(src_vid))
                pid_range.SetEnd(pid);
            else
                break;
        }
        pid_range.SetBegin(pid_range.GetBegin() + GetFirstPageId(edge_partition_id, version_id, d_type));
        pid_range.SetEnd(pid_range.GetEnd() + GetFirstPageId(edge_partition_id, version_id, d_type));
        ALWAYS_ASSERT(pid_range.GetBegin() >= 0 && pid_range.GetBegin() < TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type));
        ALWAYS_ASSERT(pid_range.GetEnd() >= 0 && pid_range.GetEnd() < TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type));
        ALWAYS_ASSERT(pid_range.GetEnd() >= pid_range.GetBegin());

        ALWAYS_ASSERT (vidrangeperpage.Get(pid_range.GetBegin()).contains(src_vid));
        ALWAYS_ASSERT (vidrangeperpage.Get(pid_range.GetEnd()).contains(src_vid));

        //fprintf(stdout, "[%ld] Vertex %ld in pid_range [%ld, %ld]\n", PartitionStatistics::my_machine_id(), src_vid, pid_range.GetBegin(), pid_range.GetEnd());
        return pid_range;
    }
}

PageID TurboDB::GetFirstPageByVid(int edge_partition_id, node_t src_vid, int version_id, EdgeType e_type, DynamicDBType d_type, bool return_closest_page, bool is_full_list) {
    VidRangePerPage& vidrangeperpage = GetVidRangePerPage(version_id, d_type, is_full_list);
    EdgeType e_type_for_fulllist;
    if (e_type == OUTEDGE) e_type_for_fulllist = OUTEDGEFULLLIST;
    else if (e_type == INEDGE) e_type_for_fulllist = INEDGEFULLLIST;
	// TODO Optimization
	int64_t low = 0;
	int64_t high, mid;
    if (is_full_list) high = TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(e_type_for_fulllist) -1;
    else high = TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(edge_partition_id, e_type) -1;
	Range<PageID> pid_range(-1,-1);
	while (low <= high) {
		mid = (low + high)/2;
		if (vidrangeperpage.Get(edge_partition_id, mid).contains(src_vid)) break;
		else if (vidrangeperpage.Get(edge_partition_id, mid).GetBegin() > src_vid) high = mid-1;
		else if (vidrangeperpage.Get(edge_partition_id, mid).GetEnd() < src_vid) low = mid+1;
		else ALWAYS_ASSERT(0);
	}
	if (low > high) {
        ALWAYS_ASSERT(pid_range == Range<PageID> (-1,-1));
        if (!return_closest_page) return pid_range.GetBegin();
        
        PageID pid = mid;
        while (true) {
            if (pid < 0 || pid >= TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(edge_partition_id, e_type)) return -1;
            if (vidrangeperpage.Get(edge_partition_id, pid).GetBegin() > src_vid) {
                pid--;
                if (pid < 0) continue;
                if (vidrangeperpage.Get(edge_partition_id, pid).GetBegin() < src_vid) return pid+1;
            } else if (vidrangeperpage.Get(edge_partition_id, pid).GetEnd() < src_vid) {
                pid++;
                if (pid >= TurboDB::GetDBStatistics(version_id, d_type)->my_num_pages(edge_partition_id, e_type)) continue;
                if (vidrangeperpage.Get(edge_partition_id, pid).GetBegin() > src_vid) return pid;
            } else {
                INVARIANT(false);
            }
        }
	}
	pid_range.Set(mid, mid);
	for(PageID pid = mid; pid >= 0; pid--) {
		if (vidrangeperpage.Get(edge_partition_id, pid).contains(src_vid)) pid_range.SetBegin(pid);
		else break;
	}
    if (!is_full_list) pid_range.SetBegin(pid_range.GetBegin() + GetFirstPageId(edge_partition_id, version_id, d_type));

	ALWAYS_ASSERT (vidrangeperpage.Get(pid_range.GetBegin()).contains(src_vid));

	return pid_range.GetBegin();
}

Range<PageID> TurboDB::GetPageRangeByVidRange(int edge_partition_id, Range<node_t> src_vid, int version, EdgeType e_type, DynamicDBType d_type) {
	Range<PageID> pid_range(-1, -1);
	Range<PageID> from_pid_range(-1, -1);
	Range<PageID> to_pid_range(-1, -1);
	Range<node_t> input_src_vid(src_vid);

	while ((pid_range = GetPageRangeByVid(edge_partition_id, src_vid.GetBegin(), version, e_type, d_type)) == Range<PageID>(-1,-1)) {
		src_vid.SetBegin(src_vid.GetBegin() + 1);

		if (src_vid.GetBegin() < 0 || src_vid.GetBegin() > PartitionStatistics::my_last_node_id()) {
			//fprintf(stdout, "[TurboDB::GetPageRangeByVidRange](%ld, [%ld, %ld]) --> [%ld, %ld]\n", edge_partition_id, input_src_vid.GetBegin(), input_src_vid.GetEnd(), src_vid.GetBegin(), src_vid.GetEnd());
			return Range<PageID> (-1, -1);
		}

		ALWAYS_ASSERT (src_vid.GetBegin() <= src_vid.GetBegin());
		ALWAYS_ASSERT (src_vid.GetBegin() >= 0 && src_vid.GetBegin() <= PartitionStatistics::my_last_node_id());
	}
	from_pid_range = pid_range;
	//fprintf(stdout, "[TurboDB::GetPageRangeByVidRange](%ld, [%ld, %ld]) --> from [%ld, %ld]\n", edge_partition_id, input_src_vid.GetBegin(), input_src_vid.GetEnd(), from_pid_range.GetBegin(), from_pid_range.GetEnd());

	while ((pid_range = GetPageRangeByVid(edge_partition_id, src_vid.GetEnd(), version, e_type, d_type)) == Range<PageID>(-1,-1)) {
		src_vid.SetEnd(src_vid.GetEnd() - 1);
		if (src_vid.GetEnd() < 0 || src_vid.GetEnd() < PartitionStatistics::my_first_node_id()) {
			//fprintf(stdout, "[TurboDB::GetPageRangeByVidRange](%ld, [%ld, %ld]) --> [%ld, %ld]\n", edge_partition_id, input_src_vid.GetBegin(), input_src_vid.GetEnd(), src_vid.GetBegin(), src_vid.GetEnd());
			return Range<PageID> (-1, -1);
		}
        if (src_vid.GetEnd() < src_vid.GetBegin()) {
            fprintf(stdout, "[TurboDB::GetPageRangeByVidRange](%ld, [%ld, %ld]) --> [%ld, %ld]\n", edge_partition_id, input_src_vid.GetBegin(), input_src_vid.GetEnd(), src_vid.GetBegin(), src_vid.GetEnd());
        }
		ALWAYS_ASSERT (src_vid.GetEnd() >= src_vid.GetBegin());
		ALWAYS_ASSERT (src_vid.GetEnd() >= 0 && src_vid.GetEnd() >= PartitionStatistics::my_first_node_id());
	}
	to_pid_range = pid_range;
	//fprintf(stdout, "[TurboDB::GetPageRangeByVidRange](%ld, [%ld, %ld]) --> to [%ld, %ld]\n", edge_partition_id, input_src_vid.GetBegin(), input_src_vid.GetEnd(), to_pid_range.GetBegin(), to_pid_range.GetEnd());

	Range<PageID> result(from_pid_range.GetBegin(), to_pid_range.GetEnd());

	ALWAYS_ASSERT (result.GetBegin() >= GetFirstPageId(edge_partition_id, version, d_type));
	ALWAYS_ASSERT (result.GetEnd() <= GetLastPageId(edge_partition_id, version, d_type));
	return result;
}

PageID TurboDB::GetFirstPageByVidRange(int edge_partition_id, Range<node_t> src_vid, int version, EdgeType e_type, DynamicDBType d_type, bool return_closest_page, bool is_full_list) {
    PageID pid;

	while ((pid = GetFirstPageByVid(edge_partition_id, src_vid.GetBegin(), version, e_type, d_type, return_closest_page, is_full_list)) == -1) {
		src_vid.SetBegin(src_vid.GetBegin() + 1);

		if (src_vid.GetBegin() < 0 || src_vid.GetBegin() > PartitionStatistics::my_last_node_id()) return -1;
        if (src_vid.GetBegin() > src_vid.GetEnd()) return -1;

		ALWAYS_ASSERT (src_vid.GetBegin() >= 0 && src_vid.GetBegin() <= PartitionStatistics::my_last_node_id());
	}
    return pid;
}

Range<PageID> TurboDB::GetPageRangeByEdgeSubchunk (int edge_chunk_id, int subchunk_idx) {
	ALWAYS_ASSERT(subchunk_idx >= 0 && subchunk_idx < PartitionStatistics::num_subchunks_per_edge_chunk());
	int edge_subchunk_id = edge_chunk_id * PartitionStatistics::num_subchunks_per_edge_chunk() + subchunk_idx;
	return EdgeSubchunksPageRange[edge_subchunk_id];
}

std::string TurboDB::GetDBPath() {
    return mDBPath;
}

bool TurboDB::is_version_exist(DynamicDBType d_type, int version_id) {
    ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);
    if (d_type == INSERT) return insert_version_existence.Get(version_id);
    else if (d_type == DELETE) return delete_version_existence.Get(version_id);
    return false;
}

bool TurboDB::is_version_exist_full_list(DynamicDBType d_type, int version_id) {
    ALWAYS_ASSERT(d_type == INSERT || d_type == DELETE);
    if (d_type == INSERT) return insert_version_existence_for_full_list.Get(version_id);
    else if (d_type == DELETE) return delete_version_existence_for_full_list.Get(version_id);
    return false;
}

bool TurboDB::is_full_list_db_exist() {
    return is_full_list_db_existed_;
}

int64_t TurboDB::DstVidToEdgeSubChunkId(node_t dst_vid) {
	for(PartID pid = 0 ; pid < PartitionStatistics::num_machines() ; pid++) {
		for(int64_t chunkidx = 0 ; chunkidx < UserArguments::VECTOR_PARTITIONS ; chunkidx++) {
			for(int64_t subchunkidx = 0; subchunkidx < PartitionStatistics::num_subchunks_per_edge_chunk(); subchunkidx++) {
				int64_t subchunk_id = pid * UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_subchunks_per_edge_chunk() + chunkidx * PartitionStatistics::num_subchunks_per_edge_chunk() + subchunk_id;
				if (EdgeSubchunksVidRange[subchunk_id].contains(dst_vid)) {
					return subchunk_id;
				}
			}
		}
	}
	INVARIANT(false);
}

int64_t TurboDB::EdgeToEdgeSubChunkId(node_t src_vid, node_t dst_vid) {
	return -1;
	for(PartID pid = 0 ; pid < PartitionStatistics::num_machines() ; pid++) {
		for(int64_t chunkidx = 0 ; chunkidx < UserArguments::VECTOR_PARTITIONS ; chunkidx++) {
			for(int64_t subchunkidx = 0; subchunkidx < PartitionStatistics::num_subchunks_per_edge_chunk(); subchunkidx++) {
				int64_t subchunk_id = pid * UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_subchunks_per_edge_chunk() + chunkidx * PartitionStatistics::num_subchunks_per_edge_chunk() + subchunk_id;
				if (EdgeSubchunksVidRange[subchunk_id].contains(dst_vid)) {
					return subchunk_id;
				}
			}
		}
	}
}

