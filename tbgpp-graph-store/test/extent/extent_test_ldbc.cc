#include <string>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <memory>
#include <boost/program_options.hpp>

#include "common/json_reader.hpp"
#include "common/types/data_chunk.hpp"
#include "extent/extent_manager.hpp"

#include "catalog/catalog.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "cache/chunk_cache_manager.h"

using namespace duckdb;
namespace po = boost::program_options;

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

typedef boost::interprocess::managed_shared_memory::const_named_iterator const_named_it;

vector<std::pair<string, string>> vertex_files;
vector<std::pair<string, string>> edge_files;

void helper_deallocate_objects_in_shared_memory () {
  string server_socket = "/tmp/catalog_server";
  // setup unix domain socket with storage
  int server_conn_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_conn_ < 0) {
    perror("cannot socket");
    exit(-1);
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  strncpy(addr.sun_path, server_socket.c_str(), server_socket.size());
  addr.sun_family = AF_UNIX;
  int status = connect(server_conn_, (struct sockaddr *)&addr, sizeof(addr));
  if (status < 0) {
    perror("cannot connect to the store");
    exit(-1);
  }

  bool reinitialize_done = false;

  int nbytes_recv = recv(server_conn_, &reinitialize_done, sizeof(bool), 0);
  if (nbytes_recv != sizeof(bool)) {
    perror("error receiving the reinitialize_done bit");
    exit(-1);
  }

  if (!reinitialize_done) {
    std::cerr << "Re-initialize failure!" << std::endl;
    exit(-1);
  }

  fprintf(stdout, "Re-initialize shared memory\n");
}

TEST_CASE ("LDBC Data Bulk Insert", "[tile]") {
  // TODO Maybe we need some preprocessing..
  // Sort Vertex File & Edge File according to their vid/(src_vid, dst_vid)

  // Initialize Database
  helper_deallocate_objects_in_shared_memory(); // Initialize shared memory for Catalog
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();
  ExtentManager ext_mng; // TODO put this into database
  vector<std::pair<string, unordered_map<idx_t, idx_t>>> lid_to_pid_map;

  // Initialize ClientContext
  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());

  // Initialize Catalog Informations
  // Create Schema, Graph
  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  CreateGraphInfo graph_info("main", "graph1");
  GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);

  // Read Vertex JSON File & CreateVertexExtents
  for (auto vertex_file: vertex_files) {
    // Create Partition for each vertex (partitioned by label)
    vector<string> vertex_labels = {vertex_file.first};
    string partition_name = "vpart_" + vertex_file.first;
    CreatePartitionInfo partition_info("main", partition_name.c_str());
    PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
    PartitionID new_pid = graph_cat->GetNewPartitionID();
    graph_cat->AddVertexPartition(*client.get(), new_pid, vertex_labels);
    
    // Initialize GraphJsonFileReader
    GraphJsonFileReader reader;
    reader.InitJsonFile(vertex_file.second.c_str(), GraphComponentType::VERTEX);

    // Initialize Property Schema Catalog Entry using Schema of the vertex
    vector<string> key_names;
    vector<LogicalType> types;
    if (!reader.GetSchemaFromHeader(key_names, types)) {
      throw InvalidInputException("");
    }

    int64_t key_column_idx = reader.GetKeyColumnIndexFromHeader();
    
    string property_schema_name = "vps_" + vertex_file.first;
    CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str());
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
    vector<PropertyKeyID> property_key_ids;
    graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
    partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);

    // Initialize DataChunk
    DataChunk data;
    data.Initialize(types);

    // Initialize LID_TO_PID_MAP
    lid_to_pid_map.emplace_back(vertex_file.first, unordered_map<idx_t, idx_t>());
    unordered_map<idx_t, idx_t> &lid_to_pid_map_instance = lid_to_pid_map.back().second;

    // Read JSON File into DataChunk & CreateVertexExtent
    while (!reader.ReadJsonFile(key_names, types, data)) {
    fprintf(stdout, "Read JSON File Ongoing..\n");
      // Create Vertex Extent by Extent Manager
      ExtentID new_eid = ext_mng.CreateVertexExtent(*client.get(), data, *property_schema_cat);
      
      // Initialize pid base
      idx_t pid_base = (idx_t) new_eid;
      pid_base << 32;

      // Build Logical id To Physical id Mapping (= LID_TO_PID_MAP)
      if (key_column_idx < 0) continue;
      idx_t* key_column = (idx_t*) data.data[key_column_idx].GetData(); // XXX idx_t type?
      for (idx_t seqno = 0; seqno < data.size(); seqno++)
        lid_to_pid_map_instance.emplace(key_column[seqno], pid_base + seqno);
    }
  }

  // Read Edge JSON File & CreateEdgeExtents & Append Adj.List to VertexExtents
  for (auto edge_file: edge_files) {
    // Create Partition for each edge (partitioned by edge type)
    string edge_type = edge_file.first;
    string partition_name = "epart_" + edge_file.first;
    CreatePartitionInfo partition_info("main", partition_name.c_str());
    PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
    PartitionID new_pid = graph_cat->GetNewPartitionID();
    graph_cat->AddEdgePartition(*client.get(), new_pid, edge_type);

    // Initialize GraphJsonFileReader
    GraphJsonFileReader reader;
    reader.InitJsonFile(edge_file.second.c_str(), GraphComponentType::EDGE);

    // Initialize Property Schema Info using Schema of the edge
    vector<string> key_names;
    vector<LogicalType> types;
    if (!reader.GetSchemaFromHeader(key_names, types)) {
      throw InvalidInputException("");
    }

    int64_t src_column_idx, dst_column_idx;
    string src_column_name, dst_column_name;
    reader.GetSrcColumnIndexFromHeader(src_column_idx, src_column_name);
    reader.GetDstColumnIndexFromHeader(dst_column_idx, dst_column_name);
    if (src_column_idx < 0 || dst_column_idx < 0) throw InvalidInputException("");

    auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
      [&src_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == src_column_name; });
    if (src_it == lid_to_pid_map.end()) throw InvalidInputException("");
    unordered_map<idx_t, idx_t> &src_lid_to_pid_map_instance = src_it->second;

    auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
      [&dst_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == dst_column_name; });
    if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("");
    unordered_map<idx_t, idx_t> &dst_lid_to_pid_map_instance = dst_it->second;

    string property_schema_name = "eps_" + edge_file.first;
    CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str());
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
    vector<PropertyKeyID> property_key_ids;
    graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
    partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);

    // Initialize DataChunk
    DataChunk data;
    data.Initialize(types);

    vector<idx_t> offset_buffer;
    vector<idx_t> adj_list_buffer;

    // Initialize Min & Max Vertex ID in Src Vertex Extent
    idx_t min_id = ULLONG_MAX, max_id = ULLONG_MAX;
    idx_t vertex_seqno;
    idx_t *vertex_id_column;
    DataChunk vertex_id_chunk;
    vertex_id_chunk.Initialize(LogicalType::UBIGINT);

    // Read JSON File into DataChunk & CreateEdgeExtent
    while (!reader.ReadJsonFile(key_names, types, data)) {
      fprintf(stdout, "Read JSON File Ongoing..\n");      

      // Get New ExtentID for this chunk
      ExtentID new_eid = property_schema_cat->GetNewExtentID();

      // Initialize epid base
      idx_t epid_base = (idx_t) new_eid;
      epid_base << 32;

      // Convert lid to pid using LID_TO_PID_MAP
      idx_t *src_key_column = (idx_t*) data.data[src_column_idx].GetData();
      idx_t *dst_key_column = (idx_t*) data.data[dst_column_idx].GetData();

      idx_t src_seqno = 0, dst_seqno = 0;
      idx_t cur_src_id, cur_dst_id, cur_src_pid, cur_dst_pid;
      idx_t begin_idx, end_idx;
      idx_t max_seqno = data.size();
      idx_t prev_id = ULLONG_MAX;

      // For the first tuple
      begin_idx = src_seqno;
      prev_id = cur_src_id = src_key_column[src_seqno];
      
      D_ASSERT(src_lid_to_pid_map_instance.find(src_key_column[src_seqno]) != src_lid_to_pid_map_instance.end());
      cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
      src_key_column[src_seqno] = cur_src_pid;

      // Check if we need to load Correspind ID column of Src Vertex Extent
      if (min_id == ULLONG_MAX) {
        // TODO Read Corresponding ID column of Src Vertex Extent

        // TODO Initialize min & max id

        // Initialize vertex_seqno
        vertex_seqno = 0;
        vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
        while (vertex_id_column[vertex_seqno] < cur_src_id) {
          vertex_seqno++;
          offset_buffer.push_back(0);
        }
        D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
      }

      src_seqno++;
      while(src_seqno < max_seqno) {
        cur_src_id = src_key_column[src_seqno];
        if (cur_src_id == prev_id) {
          src_key_column[src_seqno] = cur_src_pid;
          src_seqno++;
        } else {
          end_idx = src_seqno;
          offset_buffer.push_back(end_idx);
          for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
            cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
            dst_key_column[dst_seqno] = cur_dst_pid;
            adj_list_buffer.push_back(cur_dst_pid);
            adj_list_buffer.push_back(epid_base + dst_seqno);
          }

          if (cur_src_id > max_id) {
            // Fill offset_buffer
            idx_t last_offset = offset_buffer.back();
            int remain_size = STANDARD_VECTOR_SIZE - offset_buffer.size();
            while(remain_size-- > 0) offset_buffer.push_back(last_offset);

            // TODO AddChunk for Adj.List to current Src Vertex Extent

            // Clear offset buffer & adjlist buffer for next Extent
            offset_buffer.clear();
            adj_list_buffer.clear();

            // TODO Read corresponding ID column of Src Vertex Extent

            // TODO Initialize min & max id

            // Initialize vertex_seqno
            vertex_seqno = 0;
            vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
            while (vertex_id_column[vertex_seqno] < cur_src_id) {
              vertex_seqno++;
              offset_buffer.push_back(0);
            }
            D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
          }
          while (vertex_id_column[vertex_seqno] < cur_src_id) {
            vertex_seqno++;
            offset_buffer.push_back(0);
            D_ASSERT(vertex_seqno < STANDARD_VECTOR_SIZE);
          }

          prev_id = cur_src_id;
          begin_idx = src_seqno;
        }
      }

      // Process remaining dst vertices
      end_idx = src_seqno;
      offset_buffer.push_back(end_idx);
      for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
        D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
        cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
        dst_key_column[dst_seqno] = cur_dst_pid;
        adj_list_buffer.push_back(cur_dst_pid);
        adj_list_buffer.push_back(epid_base + dst_seqno);
      }

      // Create Edge Extent by Extent Manager
      ext_mng.CreateEdgeExtent(*client.get(), data, *property_schema_cat, new_eid);
    }
    
    // Process remaining adjlist
    // Fill offset_buffer
    idx_t last_offset = offset_buffer.back();
    int remain_size = STANDARD_VECTOR_SIZE - offset_buffer.size();
    while(remain_size-- > 0) offset_buffer.push_back(last_offset);

    // TODO AddChunk for Adj.List to current Src Vertex Extent

    // Clear offset buffer & adjlist buffer for next Extent
    offset_buffer.clear();
    adj_list_buffer.clear();
  }
}

class InputParser{
  public:
    InputParser (int &argc, char **argv){
      for (int i=1; i < argc; ++i) {
        this->tokens.push_back(std::string(argv[i]));
      }
    }
    void getCmdOption() const {
      std::vector<std::string>::const_iterator itr;
      for (itr = this->tokens.begin(); itr != this->tokens.end(); itr++) {
        std::string current_str = *itr;
        if (std::strncmp(current_str.c_str(), "--nodes:", 8) == 0) {
          std::pair<std::string, std::string> pair_to_insert;
          pair_to_insert.first = std::string(*itr).substr(8);
          itr++;
          pair_to_insert.second = *itr;
          vertex_files.push_back(pair_to_insert);
        } else if (std::strncmp(current_str.c_str(), "--relationships:", 16) == 0) {
          std::pair<std::string, std::string> pair_to_insert;
          pair_to_insert.first = std::string(*itr).substr(16);
          itr++;
          pair_to_insert.second = *itr;
          edge_files.push_back(pair_to_insert);
        }
      }
    }
  private:
    std::vector <std::string> tokens;
};

int main(int argc, char **argv) {
  InputParser input(argc, argv);
  input.getCmdOption();
  fprintf(stdout, "Load Following Nodes\n");
  for (int i = 0; i < vertex_files.size(); i++)
    fprintf(stdout, "%s: %s\n", vertex_files[i].first.c_str(), vertex_files[i].second.c_str());
  fprintf(stdout, "Load Following Relationships\n");
  for (int i = 0; i < edge_files.size(); i++)
    fprintf(stdout, "%s: %s\n", edge_files[i].first.c_str(), edge_files[i].second.c_str());

  // Initialize System Parameters
  DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  DiskAioParameters::WORKSPACE = "/home/tslee/turbograph-v3/tbgpp-graph-store/test/extent/data/";
  
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

  // Initialize ChunkCacheManager
  ChunkCacheManager::ccm = new ChunkCacheManager();

  // Run Catch Test
  int result = Catch::Session().run(argc, argv);

  // Destruct ChunkCacheManager
  delete ChunkCacheManager::ccm;
  return 0;
}
