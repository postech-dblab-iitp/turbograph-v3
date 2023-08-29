#ifndef GRAPH_PARTITION_H
#define GRAPH_PARTITION_H

#include "common/common.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include <mpi.h>
#include <vector>
#include "extent/extent_manager.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include <algorithm>
#include <iostream>
#include <fstream>
#include "turbo_tcp.hpp"
#include "RequestRespond.hpp"
#include <tbb/concurrent_queue.h>
#include <thread>
#include <atomic>
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
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"

using namespace duckdb;

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
    int64_t size_extent;
    duckdb::ChunkDefinitionID chunk_def_id;
    duckdb::LogicalType type;
    
    char data[1];
};

class GraphPartitioner {
    //Intended partitioning sequence: MPI_Init() -> InitializePartitioner() -> ProcessPartitioning() 
    //-> DistributePartitionedFile() -> ClearPartitioner() -> MPI_Finalize()

    public:
    void InitializePartitioner(std::pair<std::string, std::string> fileinfo);
    int ProcessPartitioning(DistributionPolicy policy, std::vector<std::string> hash_columns, Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *graph_cat, std::pair<std::string, std::string> fileinfo);
    static void SendExtentFromQueue();

    // void DistributePartitionedFile();
    void ClearPartitioner();

    duckdb::DataChunk* AllocateNewChunk(int32_t dest_process_rank);

    //Temporarily use very simple hash functions. Maybe need to change this if necessary.
    template <typename T>
    int32_t PartitionHash(T value) {
        static_assert(std::is_integral<T>::value, "Hash function for integers");
        return (int32_t)(value%process_count-1) + 1; //1, 2, ..., process_count-1. Master node stores no grpah.
    }
    duckdb::ProcessID process_rank;
    std::string output_path; //Temporarily store output to "dir/process_rank" to test in single machine. In distributed system, this should be changed.
    static duckdb::ProcessID process_count;

    private:
    Role role;
    // std::vector<int32_t> buffer_count;              //use: int64_t buffer_count = buffer_count[proc_rank]

    void ParseLabelSet(string &labelset, vector<string> &parsed_labelset);


    //Followings are only for master!
    std::vector<std::string> key_names;
    std::vector<duckdb::LogicalType> types; //This is better to be here since this is used for chunk allocation.
    std::vector<duckdb::ProcessID> allocated_chunk_count;
    std::unordered_map<duckdb::ProcessID, std::vector<duckdb::DataChunk *>> datachunks;
    static std::vector<tbb::concurrent_queue<SimpleContainer*>> per_segment_buffer_queue; //use: extent_queues[proc_rank].push(extent_with_meta_data)
    static std::atomic<int> finished_segment_count; //When this is process_count -1, sending finished. (since master is not counted)
    // duckdb::ExtentManager ext_mng;
    duckdb::GraphSIMDCSVFileParser reader;
    int32_t spawn_sender_thread_cnt;
    std::queue<std::future<void>> send_request_to_wait;
    // std::unordered_map<duckdb::ProcessID, std::vector<char*>> buffers;        //use: char* buffer = buffers[proc_rank][buffer_idx]
    // std::unordered_map<duckdb::ProcessID, std::vector<int64_t>> buffer_sizes; //use: int64_t buffer_size = buffer_sizes[proc_rank][buffer_idx]

    // //Followings are only for segment
    // std::vector<char*> recv_buffers;
    // int32_t buffer_count_seg;
};

class InputParser_{
  public:
    InputParser_ (int &argc, char **argv, int pos){
      for (int i=pos; i < argc; ++i) {
		this->tokens.push_back(std::string(argv[i]));
      }
    }
    std::pair<std::string, std::string> getCmdOption() const {
      std::vector<std::string>::const_iterator itr;
      for (itr = this->tokens.begin(); itr != this->tokens.end(); itr++) {
    	std::string current_str = *itr;
        if (std::strncmp(current_str.c_str(), "--nodes:", 8) == 0) {
        	std::pair<std::string, std::string> pair_to_insert;
        	pair_to_insert.first = std::string(*itr).substr(8);
        	itr++;
        	pair_to_insert.second = *itr;
        	return pair_to_insert;
        }
        // } else if (std::strncmp(current_str.c_str(), "--relationships:", 16) == 0) {
        // 	std::pair<std::string, std::string> pair_to_insert;
        // 	pair_to_insert.first = std::string(*itr).substr(16);
        // 	itr++;
        // 	pair_to_insert.second = *itr;
        // 	edge_files.push_back(pair_to_insert);
        // 	load_edge = true;
        // } else if (std::strncmp(current_str.c_str(), "--relationships_backward:", 25) == 0) {
        // 	// TODO check if a corresponding forward edge exists
        // 	std::pair<std::string, std::string> pair_to_insert;
        // 	pair_to_insert.first = std::string(*itr).substr(25);
        // 	itr++;
        // 	pair_to_insert.second = *itr;
        // 	edge_files_backward.push_back(pair_to_insert);
        // 	load_backward_edge = true;
        // } else if (std::strncmp(current_str.c_str(), "--output_dir:", 13) == 0) {
		// 	output_dir = std::string(*itr).substr(13);
		// }
      }
    }
  private:
    std::vector <std::string> tokens;
};


#endif