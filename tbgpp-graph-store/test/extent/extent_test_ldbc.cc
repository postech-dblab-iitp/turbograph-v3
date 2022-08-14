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
#include <boost/functional/hash.hpp>

#include "common/json_reader.hpp"
#include "common/graph_csv_reader.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include "common/types/data_chunk.hpp"
#include "extent/extent_manager.hpp"
#include "extent/extent_iterator.hpp"
#include "index/index.hpp"
#include "index/art/art.hpp"

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
#include "common/robin_hood.h"

using namespace duckdb;
namespace po = boost::program_options;

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

typedef boost::interprocess::managed_shared_memory::const_named_iterator const_named_it;

vector<std::pair<string, string>> vertex_files;
vector<std::pair<string, string>> edge_files;
vector<std::pair<string, string>> edge_files_backward;

bool load_edge;
bool load_backward_edge;

typedef std::pair<idx_t, idx_t> LidPair;

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
  vector<std::pair<string, unordered_map<idx_t, idx_t>>> lid_to_pid_map; // For Forward & Backward AdjList
  vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_pair_to_epid_map; // For Backward AdjList

  // Initialize ClientContext
  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());

  // Initialize Catalog Informations
  // Create Schema, Graph
  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  CreateGraphInfo graph_info("main", "graph1");
  GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);

  int aaa;
  // std::cin >> aaa;
  // Read Vertex CSV File & CreateVertexExtents
  unique_ptr<Index> index; // Temporary..
  for (auto &vertex_file: vertex_files) {
    auto vertex_file_start = std::chrono::high_resolution_clock::now();
    fprintf(stdout, "Start to load %s, %s\n", vertex_file.first.c_str(), vertex_file.second.c_str());
    // Create Partition for each vertex (partitioned by label)
    vector<string> vertex_labels = {vertex_file.first};
    string partition_name = "vpart_" + vertex_file.first;
    CreatePartitionInfo partition_info("main", partition_name.c_str());
    PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
    PartitionID new_pid = graph_cat->GetNewPartitionID();
    graph_cat->AddVertexPartition(*client.get(), new_pid, vertex_labels);
    
    fprintf(stdout, "Init GraphCSVFile\n");
    auto init_csv_start = std::chrono::high_resolution_clock::now();
    // Initialize CSVFileReader
    GraphSIMDCSVFileParser reader;
    size_t approximated_num_rows = reader.InitCSVFile(vertex_file.second.c_str(), GraphComponentType::VERTEX, '|');
    auto init_csv_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> init_csv_duration = init_csv_end - init_csv_start;
    fprintf(stdout, "InitCSV Elapsed: %.3f\n", init_csv_duration.count());

    // Initialize Property Schema Catalog Entry using Schema of the vertex
    vector<string> key_names;
    vector<LogicalType> types;
    if (!reader.GetSchemaFromHeader(key_names, types)) {
      throw InvalidInputException("");
    }
    
    int64_t key_column_idx = reader.GetKeyColumnIndexFromHeader();
    
    string property_schema_name = "vps_" + vertex_file.first;
    fprintf(stdout, "prop_schema_name = %s\n", property_schema_name.c_str());
    CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid);
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
    
    vector<PropertyKeyID> property_key_ids;
    graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
    partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
    property_schema_cat->SetTypes(types);
    property_schema_cat->SetKeys(key_names);
    
    // Initialize DataChunk
    DataChunk data;
    data.Initialize(types);

    // Initialize LID_TO_PID_MAP
    unordered_map<idx_t, idx_t> *lid_to_pid_map_instance;
    if (load_edge) {
      lid_to_pid_map.emplace_back(vertex_file.first, unordered_map<idx_t, idx_t>());
      lid_to_pid_map_instance = &lid_to_pid_map.back().second;
      lid_to_pid_map_instance->reserve(approximated_num_rows);
      vector<column_t> column_ids;
      column_ids.push_back(key_column_idx);
      index = make_unique<ART>(column_ids, IndexConstraintType::NONE);
    }

    // Read CSV File into DataChunk & CreateVertexExtent
    auto read_chunk_start = std::chrono::high_resolution_clock::now();
    while (!reader.ReadCSVFile(key_names, types, data)) {
      auto read_chunk_end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double> chunk_duration = read_chunk_end - read_chunk_start;
      fprintf(stdout, "\tRead CSV File Ongoing.. Elapsed: %.3f\n", chunk_duration.count());
      //continue;
      //fprintf(stderr, "%s\n", data.ToString().c_str());
      // Create Vertex Extent by Extent Manager
      auto create_extent_start = std::chrono::high_resolution_clock::now();
      ExtentID new_eid = ext_mng.CreateExtent(*client.get(), data, *property_schema_cat);
      auto create_extent_end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double> extent_duration = create_extent_end - create_extent_start;
      fprintf(stdout, "\tCreateExtent Elapsed: %.3f\n", extent_duration.count());
      property_schema_cat->AddExtent(new_eid);
      
      if (load_edge) {
        // Initialize pid base
        idx_t pid_base = (idx_t) new_eid;
        pid_base = pid_base << 32;

        // Build Logical id To Physical id Mapping (= LID_TO_PID_MAP)
        auto map_build_start = std::chrono::high_resolution_clock::now();
        if (key_column_idx < 0) continue;
        idx_t* key_column = (idx_t*) data.data[key_column_idx].GetData(); // XXX idx_t type?
        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
          lid_to_pid_map_instance->emplace(key_column[seqno], pid_base + seqno);
          // if (new_eid >= 65536 + 55 && new_eid <= 65536 + 57) {
          //std::cout << key_column[seqno] << " inserted at seqno: " << seqno << ", pid = " << pid_base + seqno << std::endl;
          // }
        }
        auto map_build_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> map_build_duration = map_build_end - map_build_start;
        fprintf(stdout, "Map Build Elapsed: %.3f\n", map_build_duration.count());

        // Build Index
        // auto index_build_start = std::chrono::high_resolution_clock::now();
        // Vector row_ids(LogicalType::UBIGINT, true, false, data.size());
        // for (idx_t seqno = 0; seqno < data.size(); seqno++) {
        //   row_ids.SetValue(seqno, Value::UBIGINT(pid_base + seqno));
        // }
        // DataChunk tmp_chunk;
        // vector<LogicalType> tmp_types = {LogicalType::UBIGINT};
        // tmp_chunk.Initialize(tmp_types);
        // tmp_chunk.data[0].Reference(data.data[key_column_idx]);
        // IndexLock lock;
        // index->Insert(lock, tmp_chunk, row_ids);
        // auto index_build_end = std::chrono::high_resolution_clock::now();
        // std::chrono::duration<double> index_build_duration = index_build_end - index_build_start;
        // fprintf(stdout, "Index Build Elapsed: %.3f\n", index_build_duration.count());
      }
      read_chunk_start = std::chrono::high_resolution_clock::now();
    }
    auto vertex_file_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = vertex_file_end - vertex_file_start;

    fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", vertex_file.first.c_str(), vertex_file.second.c_str(), duration.count());
  }

  fprintf(stdout, "Vertex File Loading Done\n");

  // Read Edge CSV File & CreateEdgeExtents & Append Adj.List to VertexExtents
  for (auto &edge_file: edge_files) {
    auto edge_file_start = std::chrono::high_resolution_clock::now();
    fprintf(stdout, "Start to load %s, %s\n", edge_file.first.c_str(), edge_file.second.c_str());
    // Create Partition for each edge (partitioned by edge type)
    string edge_type = edge_file.first;
    string partition_name = "epart_" + edge_file.first;
    CreatePartitionInfo partition_info("main", partition_name.c_str());
    PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
    PartitionID new_pid = graph_cat->GetNewPartitionID();
    graph_cat->AddEdgePartition(*client.get(), new_pid, edge_type);

    // Initialize CSVFileReader
    GraphSIMDCSVFileParser reader;
    size_t approximated_num_rows = reader.InitCSVFile(edge_file.second.c_str(), GraphComponentType::EDGE, '|');

    // Initialize Property Schema Info using Schema of the edge
    vector<string> key_names;
    vector<LogicalType> types;
    if (!reader.GetSchemaFromHeader(key_names, types)) {
      throw InvalidInputException("A");
    }

    int64_t src_column_idx, dst_column_idx;
    string src_column_name, dst_column_name;
    reader.GetSrcColumnIndexFromHeader(src_column_idx, src_column_name);
    reader.GetDstColumnIndexFromHeader(dst_column_idx, dst_column_name);
    if (src_column_idx < 0 || dst_column_idx < 0) throw InvalidInputException("B");
    fprintf(stdout, "Src column name = %s (idx = %ld), Dst column name = %s (idx = %ld)\n", src_column_name.c_str(), src_column_idx, dst_column_name.c_str(), dst_column_idx);

    auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
      [&src_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == src_column_name; });
    if (src_it == lid_to_pid_map.end()) throw InvalidInputException("C");
    unordered_map<idx_t, idx_t> &src_lid_to_pid_map_instance = src_it->second;

    auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
      [&dst_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == dst_column_name; });
    if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("D");
    unordered_map<idx_t, idx_t> &dst_lid_to_pid_map_instance = dst_it->second;

    string property_schema_name = "eps_" + edge_file.first;
    CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid);
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
    vector<PropertyKeyID> property_key_ids;
    graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
    partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
    property_schema_cat->SetTypes(types);
    property_schema_cat->SetKeys(key_names);

    // Initialize DataChunk
    DataChunk data;
    data.Initialize(types);

    // Initialize LID_PAIR_TO_EPID_MAP
    unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_pair_to_epid_map_instance;
    if (load_backward_edge) {
      lid_pair_to_epid_map.emplace_back(edge_file.first, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
      lid_pair_to_epid_map_instance = &lid_pair_to_epid_map.back().second;
      lid_pair_to_epid_map_instance->reserve(approximated_num_rows);
    }
    LidPair lid_pair;

    // Initialize AdjListBuffer
    vector<idx_t> adj_list_buffer;
    adj_list_buffer.resize(STANDARD_VECTOR_SIZE);

    // Initialize Extent Iterator
    ExtentIterator ext_it;
    PropertySchemaCatalogEntry* vertex_ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, "main", "vps_" + src_column_name);
    vector<idx_t> src_column_idxs = {(idx_t) src_column_idx};
    vector<LogicalType> vertex_id_type = {LogicalType::UBIGINT};
    ext_it.Initialize(*client.get(), vertex_ps_cat_entry, vertex_id_type, src_column_idxs);

    // Initialize variables related to vertex extent
    idx_t cur_src_id, cur_dst_id, cur_src_pid, cur_dst_pid;
    idx_t min_id = ULLONG_MAX, max_id = ULLONG_MAX, vertex_seqno;
    idx_t *vertex_id_column;
    DataChunk vertex_id_chunk;
    ExtentID current_vertex_eid;

    // Read CSV File into DataChunk & CreateEdgeExtent
    while (!reader.ReadCSVFile(key_names, types, data)) {
      fprintf(stdout, "Read Edge CSV File Ongoing..\n");

      // Get New ExtentID for this chunk
      ExtentID new_eid = property_schema_cat->GetNewExtentID();

      // Initialize epid base
      idx_t epid_base = (idx_t) new_eid;
      epid_base = epid_base << 32;

      // Convert lid to pid using LID_TO_PID_MAP
      idx_t *src_key_column = (idx_t*) data.data[src_column_idx].GetData();
      idx_t *dst_key_column = (idx_t*) data.data[dst_column_idx].GetData();

      idx_t src_seqno = 0, dst_seqno = 0;
      idx_t begin_idx, end_idx;
      idx_t max_seqno = data.size();
      idx_t prev_id = ULLONG_MAX;

      // For the first tuple
      begin_idx = src_seqno;
      prev_id = cur_src_id = src_key_column[src_seqno];
      
      D_ASSERT(src_lid_to_pid_map_instance.find(src_key_column[src_seqno]) != src_lid_to_pid_map_instance.end());
      cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
      src_key_column[src_seqno] = cur_src_pid;
      src_seqno++;

      if (min_id == ULLONG_MAX) {
        // Get First Vertex Extent
        while (true) {
          if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, false)) {
            // We do not allow this case
            throw InvalidInputException("E"); 
          }
          
          // Initialize min & max id
          vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
          min_id = vertex_id_column[0];
          max_id = vertex_id_column[vertex_id_chunk.size() - 1];
          if (cur_src_id >= min_id && cur_src_id <= max_id) break;
        }

        // Initialize vertex_seqno
        vertex_seqno = 0;
        vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
        while (vertex_id_column[vertex_seqno] < cur_src_id) {
          adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
        }
        D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
      }
      
      while(src_seqno < max_seqno) {
        cur_src_id = src_key_column[src_seqno];
        cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
        src_key_column[src_seqno] = cur_src_pid;
        if (cur_src_id == prev_id) {
          src_seqno++;
        } else {
          lid_pair.first = prev_id;
          end_idx = src_seqno;
          for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            if (dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) == dst_lid_to_pid_map_instance.end()) {
              fprintf(stdout, "????? dst_seqno %ld, val %ld, src_seqno = %ld, max_seqno = %ld, begin_idx = %ld, end_idx = %ld\n", dst_seqno, dst_key_column[dst_seqno], src_seqno, max_seqno, begin_idx, end_idx);
            }
            D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
            cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
            lid_pair.second = dst_key_column[dst_seqno];
            dst_key_column[dst_seqno] = cur_dst_pid;
            adj_list_buffer.push_back(cur_dst_pid);
            adj_list_buffer.push_back(epid_base + dst_seqno);
            lid_pair_to_epid_map_instance->emplace(lid_pair, epid_base + dst_seqno);
          }
          adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();

          if (cur_src_id > max_id) {
            // Fill offsets
            idx_t last_offset = adj_list_buffer.size();
            for (size_t i = vertex_seqno; i < STANDARD_VECTOR_SIZE; i++)
              adj_list_buffer[i] = last_offset;

            // AddChunk for Adj.List to current Src Vertex Extent
            DataChunk adj_list_chunk;
            vector<LogicalType> adj_list_chunk_types = { LogicalType::ADJLIST };
            vector<data_ptr_t> adj_list_datas(1);
            vector<string> append_keys = { edge_type };
            adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
            adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas);
            ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid, append_keys);
            adj_list_chunk.Destroy();

            // Re-initialize adjlist buffer for next Extent
            adj_list_buffer.resize(STANDARD_VECTOR_SIZE);

            // Read corresponding ID column of Src Vertex Extent
            while (true) {
              if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, false)) {
                // We do not allow this case
                throw InvalidInputException("F");
              }
        
              // Initialize min & max id
              vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
              min_id = vertex_id_column[0];
              max_id = vertex_id_column[vertex_id_chunk.size() - 1];
              if (cur_src_id >= min_id && cur_src_id <= max_id) break;
            }
            
            // Initialize vertex_seqno
            vertex_seqno = 0;
            vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
            while (vertex_id_column[vertex_seqno] < cur_src_id) {
              adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
            }
            D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
          } else {
            while (vertex_id_column[vertex_seqno] < cur_src_id) {
              adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
              D_ASSERT(vertex_seqno < STANDARD_VECTOR_SIZE);
            }
          }

          prev_id = cur_src_id;
          begin_idx = src_seqno;
          src_seqno++;
        }
      }

      // Process remaining dst vertices
      lid_pair.first = prev_id;
      end_idx = src_seqno;
      for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
        D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
        cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
        lid_pair.second = dst_key_column[dst_seqno];
        dst_key_column[dst_seqno] = cur_dst_pid;
        adj_list_buffer.push_back(cur_dst_pid);
        adj_list_buffer.push_back(epid_base + dst_seqno);
        lid_pair_to_epid_map_instance->emplace(lid_pair, epid_base + dst_seqno);
      }
      
      // Create Edge Extent by Extent Manager
      ext_mng.CreateExtent(*client.get(), data, *property_schema_cat, new_eid);
      property_schema_cat->AddExtent(new_eid);
    }
    
    // Process remaining adjlist
    // Fill offsets
    idx_t last_offset = adj_list_buffer.size();
    for (size_t i = vertex_seqno; i < STANDARD_VECTOR_SIZE; i++)
      adj_list_buffer[i] = last_offset;

    // AddChunk for Adj.List to current Src Vertex Extent
    DataChunk adj_list_chunk;
    vector<LogicalType> adj_list_chunk_types = { LogicalType::ADJLIST };
    vector<data_ptr_t> adj_list_datas(1);
    vector<string> append_keys = { edge_type };
    adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
    adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas);
    ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid, append_keys);
    adj_list_chunk.Destroy();

    auto edge_file_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = edge_file_end - edge_file_start;

    fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", edge_file.first.c_str(), edge_file.second.c_str(), duration.count());
  }

  // Read Backward Edge CSV File & Append Adj.List to VertexExtents
  for (auto &edge_file: edge_files_backward) {
    auto edge_file_start = std::chrono::high_resolution_clock::now();
    fprintf(stdout, "Start to load %s, %s\n", edge_file.first.c_str(), edge_file.second.c_str());
    // Get Partition for each edge (partitioned by edge type).
    string edge_type = edge_file.first;
    string partition_name = "epart_" + edge_file.first;
    
    // CreatePartitionInfo partition_info("main", partition_name.c_str());
    // PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
    // PartitionID new_pid = graph_cat->GetNewPartitionID();
    // graph_cat->AddEdgePartition(*client.get(), new_pid, edge_type);

    // Initialize CSVFileReader
    GraphSIMDCSVFileParser reader;
    reader.InitCSVFile(edge_file.second.c_str(), GraphComponentType::EDGE, '|');

    // Initialize LID_PAIR_TO_EPID_MAP
    auto edge_it = std::find_if(lid_pair_to_epid_map.begin(), lid_pair_to_epid_map.end(),
      [&edge_type](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) { return element.first == edge_type; });
    if (edge_it == lid_pair_to_epid_map.end()) throw InvalidInputException("[Error] Lid Pair to EPid Map does not exists");
    unordered_map<LidPair, idx_t, boost::hash<LidPair>> &lid_pair_to_epid_map_instance = edge_it->second;
    LidPair lid_pair;

    // Initialize Property Schema Info using Schema of the edge
    vector<string> key_names;
    vector<LogicalType> types;
    if (!reader.GetSchemaFromHeader(key_names, types)) {
      throw InvalidInputException("[Error] GetSchemaFromHeader");
    }

    int64_t src_column_idx, dst_column_idx;
    string src_column_name, dst_column_name;
    reader.GetDstColumnIndexFromHeader(src_column_idx, src_column_name); // Reverse
    reader.GetSrcColumnIndexFromHeader(dst_column_idx, dst_column_name); // Reverse
    if (src_column_idx < 0 || dst_column_idx < 0) throw InvalidInputException("[Error] GetSrc/DstColumnIndexFromHeader");
    fprintf(stdout, "Src column name = %s (idx = %ld), Dst column name = %s (idx = %ld)\n", src_column_name.c_str(), src_column_idx, dst_column_name.c_str(), dst_column_idx);

    auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
      [&src_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == src_column_name; });
    if (src_it == lid_to_pid_map.end()) throw InvalidInputException("[Error] Src Lid to Pid Map does not exists");
    unordered_map<idx_t, idx_t> &src_lid_to_pid_map_instance = src_it->second;

    auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
      [&dst_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == dst_column_name; });
    if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("[Error] Dst Lid to Pid Map does not exists");
    unordered_map<idx_t, idx_t> &dst_lid_to_pid_map_instance = dst_it->second;

    // string property_schema_name = "eps_" + edge_file.first;
    // CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid);
    // PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
    // vector<PropertyKeyID> property_key_ids;
    // graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
    // partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
    // property_schema_cat->SetTypes(types);
    // property_schema_cat->SetKeys(key_names);

    // Initialize DataChunk
    DataChunk data;
    data.Initialize(types);

    // Initialize AdjListBuffer
    vector<idx_t> adj_list_buffer;
    adj_list_buffer.resize(STANDARD_VECTOR_SIZE);

    // Initialize Extent Iterator
    ExtentIterator ext_it;
    PropertySchemaCatalogEntry* vertex_ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, "main", "vps_" + dst_column_name);
    vector<idx_t> dst_column_idxs = {(idx_t) dst_column_idx};
    vector<LogicalType> vertex_id_type = {LogicalType::UBIGINT};
    ext_it.Initialize(*client.get(), vertex_ps_cat_entry, vertex_id_type, dst_column_idxs);

    // Initialize variables related to vertex extent
    idx_t cur_src_id, cur_dst_id, cur_src_pid, cur_dst_pid;
    idx_t min_id = ULLONG_MAX, max_id = ULLONG_MAX, vertex_seqno;
    idx_t *vertex_id_column;
    DataChunk vertex_id_chunk;
    ExtentID current_vertex_eid;

    // Read CSV File into DataChunk & CreateEdgeExtent
    while (!reader.ReadCSVFile(key_names, types, data)) {
      //fprintf(stdout, "Read Edge CSV File Ongoing..\n");

      // Convert lid to pid using LID_TO_PID_MAP
      idx_t *src_key_column = (idx_t*) data.data[src_column_idx].GetData();
      idx_t *dst_key_column = (idx_t*) data.data[dst_column_idx].GetData();

      idx_t src_seqno = 0, dst_seqno = 0;
      idx_t begin_idx, end_idx;
      idx_t max_seqno = data.size();
      idx_t prev_id = ULLONG_MAX;

      // For the first tuple
      begin_idx = src_seqno;
      prev_id = cur_src_id = src_key_column[src_seqno];
      
      D_ASSERT(src_lid_to_pid_map_instance.find(src_key_column[src_seqno]) != src_lid_to_pid_map_instance.end());
      cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
      // src_key_column[src_seqno] = cur_src_pid;
      src_seqno++;

      if (min_id == ULLONG_MAX) {
        // Get First Vertex Extent
        while (true) {
          if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, false)) {
            // We do not allow this case
            throw InvalidInputException("E"); 
          }
          
          // Initialize min & max id
          vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
          min_id = vertex_id_column[0];
          max_id = vertex_id_column[vertex_id_chunk.size() - 1];
          if (cur_src_id >= min_id && cur_src_id <= max_id) break;
        }

        // Initialize vertex_seqno
        vertex_seqno = 0;
        vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
        while (vertex_id_column[vertex_seqno] < cur_src_id) {
          adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
        }
        D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
      }
      
      while(src_seqno < max_seqno) {
        cur_src_id = src_key_column[src_seqno];
        cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
        // src_key_column[src_seqno] = cur_src_pid;
        if (cur_src_id == prev_id) {
          src_seqno++;
        } else {
          lid_pair.second = prev_id;
          end_idx = src_seqno;
          idx_t peid;
          for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            if (dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) == dst_lid_to_pid_map_instance.end()) {
              fprintf(stdout, "????? dst_seqno %ld, val %ld, src_seqno = %ld, max_seqno = %ld, begin_idx = %ld, end_idx = %ld\n", dst_seqno, dst_key_column[dst_seqno], src_seqno, max_seqno, begin_idx, end_idx);
            }
            D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
            cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
            lid_pair.first = dst_key_column[dst_seqno];
            if (lid_pair_to_epid_map_instance.find(lid_pair) == lid_pair_to_epid_map_instance.end()) {
              fprintf(stdout, "????? cannot find lid_pair %ld %ld\n", lid_pair.first, lid_pair.second);
            }
            D_ASSERT(lid_pair_to_epid_map_instance.find(lid_pair) != lid_pair_to_epid_map_instance.end());
            peid = lid_pair_to_epid_map_instance.at(lid_pair);
            // dst_key_column[dst_seqno] = cur_dst_pid;
            adj_list_buffer.push_back(cur_dst_pid);
            adj_list_buffer.push_back(peid);
          }
          adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();

          if (cur_src_id > max_id) {
            // Fill offsets
            idx_t last_offset = adj_list_buffer.size();
            for (size_t i = vertex_seqno; i < STANDARD_VECTOR_SIZE; i++)
              adj_list_buffer[i] = last_offset;

            // AddChunk for Adj.List to current Src Vertex Extent
            DataChunk adj_list_chunk;
            vector<LogicalType> adj_list_chunk_types = { LogicalType::ADJLIST };
            vector<data_ptr_t> adj_list_datas(1);
            vector<string> append_keys = { edge_type };
            adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
            adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas);
            ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid, append_keys);
            adj_list_chunk.Destroy();

            // Re-initialize adjlist buffer for next Extent
            adj_list_buffer.resize(STANDARD_VECTOR_SIZE);

            // Read corresponding ID column of Src Vertex Extent
            while (true) {
              if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, false)) {
                // We do not allow this case
                throw InvalidInputException("F");
              }
        
              // Initialize min & max id
              vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
              min_id = vertex_id_column[0];
              max_id = vertex_id_column[vertex_id_chunk.size() - 1];
              if (cur_src_id >= min_id && cur_src_id <= max_id) break;
            }
            
            // Initialize vertex_seqno
            vertex_seqno = 0;
            vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
            while (vertex_id_column[vertex_seqno] < cur_src_id) {
              adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
            }
            D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
          } else {
            while (vertex_id_column[vertex_seqno] < cur_src_id) {
              adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
              D_ASSERT(vertex_seqno < STANDARD_VECTOR_SIZE);
            }
          }

          prev_id = cur_src_id;
          begin_idx = src_seqno;
          src_seqno++;
        }
      }

      // Process remaining dst vertices
      lid_pair.second = prev_id;
      end_idx = src_seqno;
      idx_t peid;
      for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
        D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
        cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
        lid_pair.first = dst_key_column[dst_seqno];
        D_ASSERT(lid_pair_to_epid_map_instance.find(lid_pair) != lid_pair_to_epid_map_instance.end());
        peid = lid_pair_to_epid_map_instance.at(lid_pair);
        // dst_key_column[dst_seqno] = cur_dst_pid;
        adj_list_buffer.push_back(cur_dst_pid);
        adj_list_buffer.push_back(peid);
      }
    }
    
    // Process remaining adjlist
    // Fill offsets
    idx_t last_offset = adj_list_buffer.size();
    for (size_t i = vertex_seqno; i < STANDARD_VECTOR_SIZE; i++)
      adj_list_buffer[i] = last_offset;

    // AddChunk for Adj.List to current Src Vertex Extent
    DataChunk adj_list_chunk;
    vector<LogicalType> adj_list_chunk_types = { LogicalType::ADJLIST };
    vector<data_ptr_t> adj_list_datas(1);
    vector<string> append_keys = { edge_type };
    adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
    adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas);
    ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid, append_keys);
    adj_list_chunk.Destroy();

    auto edge_file_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = edge_file_end - edge_file_start;

    fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", edge_file.first.c_str(), edge_file.second.c_str(), duration.count());
  }

  // Loading Done. Iterate vertex & edge files
  fprintf(stdout, "\nLoading Done! Scan Vertex Extents\n");
  for (auto &vertex_file : vertex_files) {
    fprintf(stdout, "Scan Vertex Extents %s\n", vertex_file.first.c_str());
    ExtentIterator ext_it;
    PropertySchemaCatalogEntry* vertex_ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, "main", "vps_" + vertex_file.first);
    vector<LogicalType> column_types = move(vertex_ps_cat_entry->GetTypes());
    vector<idx_t> column_idxs;
    
    column_idxs.resize(column_types.size());
    for (int i = 0; i < column_idxs.size(); i++) column_idxs[i] = i;
    ext_it.Initialize(*client.get(), vertex_ps_cat_entry, column_types, column_idxs);

    // Initialize DataChunk
    DataChunk data;
    ExtentID output_eid;

    ext_it.GetNextExtent(*client.get(), data, output_eid, false);
    
    // Print DataChunk
    fprintf(stdout, "Print Vertex Data %s\n", vertex_file.first.c_str());
    fprintf(stdout, "%s\n", data.ToString(10).c_str());
  }

  for (auto &edge_file : edge_files) {
    fprintf(stdout, "Scan Edge Extents %s\n", edge_file.first.c_str());
    ExtentIterator ext_it;
    PropertySchemaCatalogEntry* edge_ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, "main", "eps_" + edge_file.first);
    vector<LogicalType> column_types = move(edge_ps_cat_entry->GetTypes());
    vector<idx_t> column_idxs;
    column_idxs.resize(column_types.size());
    for (int i = 0; i < column_idxs.size(); i++) column_idxs[i] = i;
    ext_it.Initialize(*client.get(), edge_ps_cat_entry, column_types, column_idxs);

    // Initialize DataChunk
    DataChunk data;
    ExtentID output_eid;

    ext_it.GetNextExtent(*client.get(), data, output_eid, false);

    // Print DataChunk
    fprintf(stdout, "%s\n", data.ToString(10).c_str());
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
          load_edge = true;
        } else if (std::strncmp(current_str.c_str(), "--relationships_backward:", 25) == 0) {
          // TODO check if a corresponding forward edge exists
          std::pair<std::string, std::string> pair_to_insert;
          pair_to_insert.first = std::string(*itr).substr(25);
          itr++;
          pair_to_insert.second = *itr;
          edge_files_backward.push_back(pair_to_insert);
          load_backward_edge = true;
        }
      }
    }
  private:
    std::vector <std::string> tokens;
};

int main(int argc, char **argv) {
  load_edge = load_backward_edge = false;
  InputParser input(argc, argv);
  input.getCmdOption();
  fprintf(stdout, "\nLoad Following Nodes\n");
  for (int i = 0; i < vertex_files.size(); i++)
    fprintf(stdout, "\t%s : %s\n", vertex_files[i].first.c_str(), vertex_files[i].second.c_str());
  fprintf(stdout, "\nLoad Following Relationships\n");
  for (int i = 0; i < edge_files.size(); i++)
    fprintf(stdout, "\t%s : %s\n", edge_files[i].first.c_str(), edge_files[i].second.c_str());
  fprintf(stdout, "\nLoad Following Backward Relationships\n");
  for (int i = 0; i < edge_files_backward.size(); i++)
    fprintf(stdout, "\t%s : %s\n", edge_files_backward[i].first.c_str(), edge_files_backward[i].second.c_str());

  fprintf(stdout, "\nInitialize DiskAioParameters\n");
  // Initialize System Parameters
  DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  DiskAioParameters::WORKSPACE = "/data/data/"; // TODO get this from arguments
  
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

  // Initialize ChunkCacheManager
  ChunkCacheManager::ccm = new ChunkCacheManager();

  // Run Catch Test
  fprintf(stdout, "\nTest Case Start!!\n");
  argc = 1;
  int result = Catch::Session().run(argc, argv);

  // Destruct ChunkCacheManager
  delete ChunkCacheManager::ccm;
  return 0;
}
