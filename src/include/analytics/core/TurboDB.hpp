
#ifndef TURBODB_H
#define TURBODB_H

#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <thread>

#include "analytics/core/turbo_dist_internal.hpp"
#include "analytics/core/turbo_buffer_manager.hpp"
#include "analytics/io/MetaDataReader.hpp"
#include "analytics/util/util.hpp"
#include "analytics/datastructure/BitMap.hpp"
#include "analytics/datastructure/VidRangePerPage.hpp"
#include "analytics/io/TextFormatSequentialReaderWriter.hpp"
#include "analytics/util/Aio_Helper.hpp"
#include "analytics/datastructure/MemoryMappedArray.hpp"
#include "analytics/util/ConfigurationProperties.hpp"
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"

#define PAGE_ID_MAX INT_MAX
#define DIV 1048576
#define WIDTH 7

static int GetNumOfCores();
static long long getAvailableMemorySizeInMB();
static long long dumpMemoryStat();

//for merge sort
template <typename EdgePairEntry_t>
struct EdgePairandFilenum {
	EdgePairEntry_t edge;
	int64_t file_num;
};
template <typename EdgePairEntry_t>
bool operator<(EdgePairandFilenum<EdgePairEntry_t> a, EdgePairandFilenum<EdgePairEntry_t> b) {
	return a.edge < b.edge;
}
template <typename EdgePairEntry_t>
bool operator>(EdgePairandFilenum<EdgePairEntry_t> a, EdgePairandFilenum<EdgePairEntry_t> b) {
	return a.edge > b.edge;
}

struct RunConfig {
	EDGE_IN_OUT_MODE edge_in_out_mode;
	int64_t page_buffer_size_in_MB;			// buffer size for data pages [MB]
	int64_t chunk_buffer_size_in_MB;			// buffer size for chunks (vectors) [MB]
};

struct DBCatalog {
    std::string db_catalog_path;
    std::string db_name;
    int64_t total_num_vertices;
    int64_t total_num_edges;
    int64_t largest_degree_of_vertices;
    int64_t p_value;
    int64_t q_value;
    int64_t r_value;
    int latest_version;

    const char* PROPERTY_KEY_PATH = "db_catalog_path";
    const char* PROPERTY_KEY_NAME = "db_name";
    const char* PROPERTY_KEY_NUM_VERTICES = "total_num_vertices";
    const char* PROPERTY_KEY_NUM_EDGES = "total_num_edges";
    const char* PROPERTY_KEY_LARGEST_DEGREE = "largest_degree_of_vertices";
    const char* PROPERTY_KEY_P = "p_value";
    const char* PROPERTY_KEY_Q = "q_value";
    const char* PROPERTY_KEY_R = "r_value";
    const char* PROPERTY_KEY_LATEST_VERSION = "latest_version";

    ReturnStatus read_catalog_from_file (const char* catalog_path){
        ConfigProperties properties;
        bool success = true;
        std::string tmp;
        std::ifstream catalog_f;
        catalog_f.open(catalog_path);
        while (!catalog_f.eof()) {
            std::getline(catalog_f, tmp);
            if (!tmp.empty()) properties.AddProperty(tmp);
        }

        success = success && properties.ReadProperty(PROPERTY_KEY_PATH, db_catalog_path); 
        success = success && properties.ReadProperty(PROPERTY_KEY_NAME, db_name); 
        success = success && properties.ReadPropertyLong(PROPERTY_KEY_NUM_VERTICES, total_num_vertices);
        success = success && properties.ReadPropertyLong(PROPERTY_KEY_NUM_EDGES, total_num_edges);
        success = success && properties.ReadPropertyLong(PROPERTY_KEY_LARGEST_DEGREE, largest_degree_of_vertices);
        success = success && properties.ReadPropertyLong(PROPERTY_KEY_P, p_value); 
        success = success && properties.ReadPropertyLong(PROPERTY_KEY_Q, q_value); 
        success = success && properties.ReadPropertyLong(PROPERTY_KEY_R, r_value);
        success = success && properties.ReadPropertyInt(PROPERTY_KEY_LATEST_VERSION, latest_version);

        return (success) ? ReturnStatus::OK : ReturnStatus::FAIL;
    }
};

struct DBStatistics {
    DBStatistics() { }
    ~DBStatistics() { }

    per_partition_elem<PageID> in_per_partition_num_pages_;
    per_partition_elem<PageID> out_per_partition_num_pages_;
    per_partition_elem<PageID> in_per_partition_num_pages_full_list_;
    per_partition_elem<PageID> out_per_partition_num_pages_full_list_;
    per_partition_elem<node_t> per_partition_first_node_id_;
    per_partition_elem<node_t> per_partition_last_node_id_;
    std::vector<std::vector<PageID>> my_num_pages_in_edge_grids_;
    std::vector<std::vector<PageID>> accumulated_num_pages_in_edge_grids_;

    edge_t num_total_out_edges_;
    edge_t num_total_in_edges_;

    PartID& my_partition_id_ = my_partition_id__;

    bool initialized = false;

    void init() {
        if (initialized) return;
        in_my_num_pages() = 0;
        out_my_num_pages() = 0;
        first_node_id() = -1;
        last_node_id() = -1;
        my_num_pages_in_edge_grids_.resize(2);
        accumulated_num_pages_in_edge_grids_.resize(2);
        initialized = true;
    }
    
    void init_replicate(EdgeType e_type) {
        my_num_pages_in_edge_grids_[e_type].resize(PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk());
        accumulated_num_pages_in_edge_grids_[e_type].resize(PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk() + 1);
    }

    void close() {
        in_my_num_pages() = 0;
        out_my_num_pages() = 0;
        in_per_partition_num_pages_.close();
        out_per_partition_num_pages_.close();
        in_per_partition_num_pages_full_list_.close();
        out_per_partition_num_pages_full_list_.close();
        per_partition_first_node_id_.close();
        per_partition_last_node_id_.close();
    }

    void set_accumulated_num_pages(EdgeType e_type) {
        accumulated_num_pages_in_edge_grids_[e_type][0] = 0;
        for (PartID i = 1; i <= PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk() ; i++) {
            accumulated_num_pages_in_edge_grids_[e_type][i] = my_num_pages_in_edge_grids_[e_type][i - 1] + accumulated_num_pages_in_edge_grids_[e_type][i - 1];
        }
    }

    void replicate(EdgeType e_type) {
        if (e_type == OUTEDGE) {
            out_per_partition_num_pages_.pull();
            out_per_partition_num_pages_full_list_.pull();
        } else if (e_type == INEDGE) { 
            in_per_partition_num_pages_.pull();
            in_per_partition_num_pages_full_list_.pull();
        }
    }

    edge_t& num_total_out_edges() { return num_total_out_edges_; }
    PageID& in_my_num_pages() { 
        ALWAYS_ASSERT(my_partition_id_ >= 0 && my_partition_id_ < PartitionStatistics::num_machines());
        return in_per_partition_num_pages_[my_partition_id_]; 
    }
    PageID& out_my_num_pages() { 
        ALWAYS_ASSERT(my_partition_id_ >= 0 && my_partition_id_ < PartitionStatistics::num_machines());
        return out_per_partition_num_pages_[my_partition_id_]; 
    }
    PageID& my_num_pages(EdgeType e_type) { 
        if (e_type == OUTEDGE) return out_my_num_pages();
        else if (e_type == INEDGE) return in_my_num_pages();
        else if (e_type == OUTEDGEFULLLIST) return out_per_partition_num_pages_full_list_[my_partition_id_];
        else if (e_type == INEDGEFULLLIST) return in_per_partition_num_pages_full_list_[my_partition_id_];
    }
    PageID& my_num_pages(int part_id, EdgeType e_type) {
        ALWAYS_ASSERT (part_id >= 0 && part_id < PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk());
        ALWAYS_ASSERT (e_type == OUTEDGE || e_type == INEDGE);
        ALWAYS_ASSERT (e_type < my_num_pages_in_edge_grids_.size());
        ALWAYS_ASSERT (part_id >= 0 && part_id < my_num_pages_in_edge_grids_[e_type].size());
        return my_num_pages_in_edge_grids_[e_type][part_id];
    }
    PageID my_last_page_id(int part_id, EdgeType e_type=OUTEDGE) {
        ALWAYS_ASSERT (part_id >= 0 && part_id < PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk());
        if(part_id == PartitionStatistics::num_target_vector_chunks() * PartitionStatistics::num_subchunks_per_edge_chunk() -1)
            return my_num_pages(e_type)-1;
        return accumulated_num_pages_in_edge_grids_[e_type][part_id + 1] -1;
    }
    node_t& first_node_id() { return per_partition_first_node_id_[my_partition_id_]; }
    node_t& last_node_id() { return per_partition_last_node_id_[my_partition_id_]; }
};

/*
 *	This class manage
 *
 */
class TurboDB {
  public:
	friend class AdjacencyListIterator;
	friend class NeighborIterator;

	// For Debugging Purpose
	friend class TG_LoadTestMain;

	/*
	 *	A public member variable.
	 *	Global valiable of TurboDB
	 */
	static TurboDB* smpTurboDB[2];


  public:

	TurboDB();
	~TurboDB();

	ReturnStatus Init_DIST();

	//assume that record type is array of "src vertex id, dst vertex id 1, dst vertex id 2, ..."
	template <typename EdgePairEntry_t>
	ReturnStatus BulkLoad(ReaderInterface<EdgePairEntry_t>& edge_reader,
	                      WriterInterface&  adj_writer,
	                      Turbo_bin_io_handler& EdgeIndex_writer, // EdgeIndex_writer writes beginedgetable
	                      Turbo_bin_io_handler& boundary_writer,
	                      MetaDataReader& meta_reader,
	                      Turbo_bin_io_handler& bin_meta_writer);

	//if raw data has no cut vertices, use this
	//if degree is more than cut_degree, make it as cut vertex.
	template <typename EdgePairEntry_t>
	ReturnStatus BulkLoad2(std::string temp_file_path,
	                       ReaderInterface<EdgePairEntry_t>& edge_reader,
	                       WriterInterface&  adj_writer,
	                       Turbo_bin_io_handler& EdgeIndex_writer, // EdgeIndex_writer writes beginedgetable
	                       Turbo_bin_io_handler& boundary_writer,
	                       MetaDataReader& meta_reader,
	                       Turbo_bin_io_handler& bin_meta_writer,
	                       node_t cut_degree);

	ReturnStatus ReLoadDB(const char* file_name, const RunConfig& config);
	ReturnStatus LoadDB(const char* dir_name, const RunConfig& config, EdgeType e_type_=OUTEDGE, DynamicDBType d_type=INSERT, Range<int> version = Range<int>(0, UserArguments::MAX_VERSION));
	ReturnStatus LoadDB(const char* dir_name, EdgeType e_type_=OUTEDGE, DynamicDBType d_type=INSERT, Range<int> version = Range<int>(0, UserArguments::MAX_VERSION));
    ReturnStatus LoadIdx(const char* dir_name, EdgeType e_type_);
	ReturnStatus LoadFullListDB(const char* dir_name, EdgeType e_type_=OUTEDGE, DynamicDBType d_type=INSERT, Range<int> version = Range<int>(0, UserArguments::MAX_VERSION));
	ReturnStatus CloseDB(bool rm, DynamicDBType d_type_=INSERT, Range<int> version = Range<int>(0, UserArguments::MAX_VERSION));

	//	ReturnStatus _GetListOfFilesInDirectory(std::string absolute_path, std::string files_to_find, std::vector<std::string>& lists);
	ReturnStatus _GetGraphDataFileLists(const char* dir_name, std::vector<std::string>& out_adjlists_file_lists, std::vector<std::string>& out_metadata_file_lists);
	ReturnStatus _AggregateMetadata();
	ReturnStatus _ReadMetaTable(Turbo_bin_mmapper& metadata_file_reader, node_t& first_node,
	                            node_t& last_node, node_t& num_nodes, node_t& num_edges, node_t& num_cut_nodes);
	ReturnStatus _Init_FirstPageIds(std::string& file_path, int version_id = 0, DynamicDBType d_type = INSERT);
	ReturnStatus _Init_VidRangePerPage(std::string& file_path, int version_id = 0, DynamicDBType d_type = INSERT);
	ReturnStatus _Init_PartitionRanges(std::string& file_path);
	ReturnStatus _Init_EdgeSubchunks(std::string& file_path);
	ReturnStatus _Init_OutDegreeTable(std::string& dir_path, std::string& file_path, DynamicDBType e_type);
	ReturnStatus _Init_HashMap(std::string& file_path); //XXX - added by tslee
	
    static ReturnStatus _Init_BufferManager(int64_t buffer_pool_size_bytes);

	void CheckDbConsistency();
    bool CheckFileExistence(std::string file_path, int mode);

	// File I/O
	ReturnStatus OpenFile(const char* file_name, int version_id, EdgeType e_type, DynamicDBType d_type);	// read-only
	ReturnStatus CloseFile(bool rm, int version_id, EdgeType e_type, DynamicDBType d_type=INSERT);
	//ReturnStatus ReadPage(PartID partition_id, PageID pageno, Page* pageptr, void *pCallbackParam = NULL);
	//ReturnStatus ReadPage(PageID pageno, Page* pageptr, void *pCallbackParam = NULL);

	// BufMgr
	void DoCallbackTask();

  public:
	char* GetDBName();
	static TurboDB* GetTurboDB(EdgeType e_type=OUTEDGE);
	static turbo_buffer_manager* GetBufMgr();
	static DBStatistics* GetDBStatistics(int version = 0, DynamicDBType d_type = INSERT);
    static void SetDBType(EdgeType e_type, DynamicDBType d_type);
    static void ConvertDirectTablePageID (PageID table_pid, PageID& pid, int& fid);
    static ReturnStatus FindPidOffset (PageID table_pid, PageID& pid, int& fid, EdgeType e_type, DynamicDBType d_type);

	VidRangePerPage& GetVidRangePerPage(int version = 0, DynamicDBType d_type=INSERT, bool use_fulllist_db=false);
	VidRangePerPage& GetVidRangePerPageFullList(int version = 0, DynamicDBType d_type=INSERT);

    std::vector<std::vector<VidRangePerPage>> vid_range_per_page_;
    std::vector<std::vector<VidRangePerPage>> vid_range_per_page_full_list_;

	PageID GetFirstPageId (PartID i, PartID j);
	PageID GetLastPageId (PartID i, PartID j);

	PageID GetFirstPageId (PartID edge_partition_id, int version_id = 0, DynamicDBType d_type=INSERT);
	PageID GetLastPageId (PartID edge_partition_id, int version_id = 0, DynamicDBType d_type=INSERT, bool use_fulllist_db=false);
	PageID GetFirstPageIdFullList (int version_id = 0, DynamicDBType d_type=INSERT);
	PageID GetLastPageIdFullList (int version_id = 0, DynamicDBType d_type=INSERT);
    PageID ConvertToDirectTablePageID (int version_id, PageID pid, EdgeType e_type, DynamicDBType d_type=INSERT);
    PageID ConvertDirectTablePageIDToPageID (int version_id, PageID table_pid, EdgeType e_type, DynamicDBType d_type=INSERT);
    void ConvertDirectTablePageIDToVersionID (PageID table_pid, PageID& pid, int& version_id, DynamicDBType d_type=INSERT);

	Range<PageID> GetPageRangeByVid(int edge_partition_id, node_t src_vid, int version_id = 0, EdgeType e_type=OUTEDGE, DynamicDBType d_type=INSERT);
	Range<PageID> GetPageRangeByVidRange(int edge_partition_id, Range<node_t> src_vid, int version_id = 0, EdgeType e_type=OUTEDGE, DynamicDBType d_type=INSERT);
	PageID GetFirstPageByVid(int edge_partition_id, node_t src_vid, int version_id = 0, EdgeType e_type=OUTEDGE, DynamicDBType d_type=INSERT, bool return_closest_page = false, bool is_full_list = false);
	PageID GetFirstPageByVidRange(int edge_partition_id, Range<node_t> src_vid, int version_id = 0, EdgeType e_type=OUTEDGE, DynamicDBType d_type=INSERT, bool return_closest_page = false, bool is_full_list = false);
	Range<PageID> GetPageRangeByEdgeSubchunk (int edge_chunk_id, int subchunk_idx);

	static Range<node_t> GetVidRangeByEdgeSubchunkId (int edge_subchunk_id) {
		ALWAYS_ASSERT (edge_subchunk_id >= 0 && edge_subchunk_id < EdgeSubchunksVidRange.size());
        if (UserArguments::USE_FULLIST_DB && UserArguments::ITERATOR_MODE == PARTIAL_LIST) return Range<node_t>(0, PartitionStatistics::num_total_nodes() - 1);
		return EdgeSubchunksVidRange[edge_subchunk_id];
	}

    std::string GetDBPath();
    bool is_version_exist(DynamicDBType d_type, int version_id);
    bool is_version_exist_full_list(DynamicDBType d_type, int version_id);
    bool is_full_list_db_exist();

	static std::vector<Range<node_t>> EdgeSubchunksVidRange;
	std::vector<Range<PageID>> EdgeSubchunksPageRange; //XXX unused
    std::vector<std::vector<std::vector<PageID>>> FirstPageIds;  // mFirstPageIds[EdgePartitionedDbIdx] ==> First PageID of the '~Idx'-th edge partition.
    std::vector<std::vector<PageID>> accumulated_num_pages_;
    std::vector<std::vector<PageID>> accumulated_num_pages_full_list_;

	int64_t DstVidToEdgeSubChunkId(node_t dst_vid);
	int64_t EdgeToEdgeSubChunkId(node_t src_vid, node_t dst_vid);

	static int32_t& OutDegree(node_t vid) {
		ALWAYS_ASSERT(vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id());
		int64_t lid = vid - PartitionStatistics::my_first_node_id();
		return out_degree_vector_buf[lid];
	}
	static int32_t& OutInsertDegree(node_t vid) {
        ALWAYS_ASSERT(vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id());
		int64_t lid = vid - PartitionStatistics::my_first_node_id();
		return out_insert_degree_vector_buf[lid];
	}
	static int32_t& OutDeleteDegree(node_t vid) {
		ALWAYS_ASSERT(vid >= PartitionStatistics::my_first_node_id() && vid <= PartitionStatistics::my_last_node_id());
		int64_t lid = vid - PartitionStatistics::my_first_node_id();
		return out_delete_degree_vector_buf[lid];
	}

    static void SetOutDegreeVectorBuffer(int32_t* buf, int32_t* buf1, int32_t* buf2) {
        out_degree_vector_buf = buf;
        out_insert_degree_vector_buf = buf1;
        out_delete_degree_vector_buf = buf2;
    }

	static int32_t* out_degree_vector_buf;
	static int32_t* out_insert_degree_vector_buf;
	static int32_t* out_delete_degree_vector_buf;
	static MemoryMappedArray<int32_t> out_degree_vector_;
	static MemoryMappedArray<int32_t> out_insert_degree_vector_;
	static MemoryMappedArray<int32_t> out_delete_degree_vector_;
	static bool is_out_degree_vector_initialized_;
    static DBStatistics db_statistics[2][MAX_NUM_VERSIONS];
    static turbo_buffer_manager& buf_mgr_;
    static int db_type;
    static int64_t num_pages_loaded;
    static std::vector<std::vector<node_t>> graph_in_memory;

  private:
	//turbo_buffer_manager buf_mgr_;

	bool mCreateNew;
	bool is_full_list_db_existed_ = false;
	char mDBDirName[256];

    EdgeType e_type;

	RunConfig mConfigure;

	std::string mDBPath;
    
    BitMap<node_t> insert_version_existence;
    BitMap<node_t> delete_version_existence;
    BitMap<node_t> insert_version_existence_for_full_list;
    BitMap<node_t> delete_version_existence_for_full_list;

};

#endif
