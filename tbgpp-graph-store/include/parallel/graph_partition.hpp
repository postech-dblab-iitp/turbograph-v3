#ifndef GRAPH_PARTITION_H
#define GRAPH_PARTITION_H

#include "common/common.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include <mpi.h>
#include <vector>
#include "extent/extent_manager.hpp"
#include "common/graph_simdcsv_parser.hpp"
// #include "common/graph_simdjson_parser.hpp" //This causes build error. Handle later.
#include <algorithm>
#include <iostream>
#include <fstream>
#include "turbo_tcp.hpp"
#include "parallel/RequestRespond.hpp"
#include <tbb/concurrent_queue.h>
#include <thread>
#include <atomic>


#include <boost/timer/timer.hpp>
#include <boost/filesystem.hpp>

#include "main/database.hpp"
#include "main/client_context.hpp"
#include "extent/extent_manager.hpp"
#include "extent/extent_iterator.hpp"
#include "index/index.hpp"
#include "index/art/art.hpp"
#include "cache/chunk_cache_manager.h"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/graph_csv_reader.hpp"
#include "parallel/util.hpp"

#define EXTENT_GENERATOR_THREAD_COUNT 8
#define SENDER_THREAD_COUNT 8
#define DEFAULT_AIO_THREAD 16
using namespace duckdb;

typedef std::pair<idx_t, idx_t> LidPair;

enum class DistributionPolicy {
    DIST_RANDOM = 0, //TODO: apply random distribution.
    DIST_HASH = 1
};

enum class Role {
    //Currently assume that only process with rank 0 is master.
    //Only master read, process the input file and send to other segments. Master itself do not store anything.

    MASTER = 0, 
    SEGMENT = 1
};

class ExtentWithMetaData {
    public:
    int32_t dest_process_id;
    int64_t size_extent;
    duckdb::ChunkDefinitionID chunk_def_id;
    duckdb::LogicalType type;
    char extent_dir_path[256]; //This is "/part_" + std::to_string(pid) + "/ext_" + std::to_string(new_eid). Not including WORKSPACE.
    //If need more metadata, add here.

    char data[1];
};

enum class GraphFileType {
    Vertex = 0, 
    Edge = 1,
    EdgeBackward = 2
};
class FileMetaInfo { //Vector of FileMetaInfo will be in GraphPartitioner.
    public:
    int file_seq_number = -1;
    GraphFileType file_type;
    std::pair<std::string, std::string> fileinfo;
    std::vector<std::string> key_names;
    std::vector<duckdb::LogicalType> types;
    std::vector<std::string> vertex_labels;
    PropertySchemaCatalogEntry* property_schema_cat;
    std::vector<std::string> hash_columns;
    std::vector<int64_t> key_column_idxs;
    PartitionCatalogEntry* part_cat;    //TODO: set this.
    //If need more for edge file, add here.
};

class GraphPartitioner {
    public:
    static void     InitializePartitioner(std::shared_ptr<ClientContext> client, Catalog* cat_instance, ExtentManager* ext_mng, GraphCatalogEntry* graph_cat);
    static void     ReadVertexFileAndCreateDataChunk(DistributionPolicy policy, std::vector<std::string> hash_columns, std::pair<std::string, std::string> fileinfo);
    static void     CreateVertexCatalogInfos(Catalog &cat_instance, std::shared_ptr<ClientContext> client, GraphCatalogEntry *graph_cat,
							  std::string &vertex_labelset_name, vector<string> &vertex_labels, vector<string> &key_names,
							  vector<LogicalType> &types, PartitionCatalogEntry *&partition_cat, PropertySchemaCatalogEntry *&property_schema_cat);

    static void     ReceiveAndStoreExtent(int size); 
    static bool     AmIMaster(int32_t process_rank);
    static void     ClearPartitioner();
    static void     SpawnGeneratorAndSenderThreads();
    static void     WaitForGeneratorThreads();
    static void     WaitForSenderThreads();
    //Following functions are for Aio_Helper. Thread safe.
    static void     GenerateExtentFromChunkQueue(int32_t my_generator_id);
    static void     SendExtentFromQueue(int32_t);
    // void DistributePartitionedFile();

    static void     ParseLabelSet(std::string& label_set, std::vector<std::string>& labels);
    static duckdb::DataChunk* AllocateNewChunk(int file_seq_number); //TODO: change this to use types.

    static duckdb::ProcessID process_rank;
    static std::string output_path; //Temporarily store output to "dir/process_rank" to test in single machine. In distributed system, this should be changed.
    static duckdb::ProcessID process_count;
    static Role role;
    static std::thread* rr_receiver_;

    //Followings are set at InputParser::getCmdOption()
    // vector<std::pair<string, string>> json_files;
    // vector<JsonFileType> json_file_types;
    // vector<vector<std::pair<string, string>>> json_file_vertices;
    // vector<vector<std::pair<string, string>>> json_file_edges;
    static vector<std::pair<string, string>> vertex_files;
    static vector<std::pair<string, string>> edge_files;
    static vector<std::pair<string, string>> edge_files_backward;
    static string output_dir;

    static bool load_edge;
    static bool load_backward_edge; 

    
    static std::vector<FileMetaInfo> file_meta_infos;   //indexed by file_seq_number. Used by GenerateExtentFromChunkQueue, ...

    //Followings are set at InitializePartitioner()
    static std::shared_ptr<ClientContext> client;
    static Catalog *cat_instance;
    static duckdb::ExtentManager *ext_mng;
    static GraphCatalogEntry *graph_cat;

    static turbo_tcp server_sockets;
    static turbo_tcp client_sockets;

    //Followings are only for master!
    static std::vector<tbb::concurrent_queue<std::pair<std::pair<int32_t, int32_t>, duckdb::DataChunk *>>> per_generator_datachunk_queue; //here int32_t is file seq number.
    static std::vector<tbb::concurrent_queue<::SimpleContainer>> per_sender_buffer_queue; //use: extent_queues[proc_rank].push(extent_with_meta_data)
    static std::queue<std::future<void>> extent_generator_futures;
    static std::queue<std::future<void>> extent_sender_futures;
    static std::atomic<bool> file_reading_finished;
    static std::atomic<bool> extent_generating_finished; //TODO set and use this in main / sender thread
    static vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_to_pid_map;
    static std::vector<atom> lid_to_pid_map_locks;
	static vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_pair_to_epid_map; // For Backward AdjList
    static std::vector<atom> lid_pair_to_epid_map_locks;
	static vector<std::pair<string, ART*>> lid_to_pid_index; // For Forward & Backward AdjList
    static std::atomic<int> local_file_seq_number;

    public:
    //Temporarily use very simple hash functions. Maybe need to change this if necessary.
    template <typename T>
    static int32_t PartitionHash(T value) {
        static_assert(std::is_integral<T>::value, "Hash function for integers");
        return (int32_t)(value%process_count-1) + 1; //1, 2, ..., process_count-1. Master node stores no grpah.
    }
};

#endif