#include <iostream>
#include <iterator>
#include <cassert> 
#include <filesystem>
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
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>


#include <nlohmann/json.hpp>	// TODO remove json and use that of boost
using json = nlohmann::json;

#include <icecream.hpp>

//#include "livegraph.hpp"
#include "plans/query_plan_suite.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"

//#include "common/types/chunk_collection.hpp"

//#include "typedef.hpp"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"


#include "main/database.hpp"
#include "main/client_context.hpp"
#include "extent/extent_manager.hpp"
#include "extent/extent_iterator.hpp"
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
#include "common/graph_simdcsv_parser.hpp"

using namespace duckdb;

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

void exportQueryPlanVisualizer(std::vector<CypherPipelineExecutor*>& executors, int exec_time);

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

int main(int argc, char** argv) {
	// Initialize System
	InputParser input(argc, argv);
	input.getCmdOption();
	fprintf(stdout, "\nLoad Following Nodes\n");
	for (int i = 0; i < vertex_files.size(); i++)
		fprintf(stdout, "\t%s : %s\n", vertex_files[i].first.c_str(), vertex_files[i].second.c_str());
	fprintf(stdout, "\nLoad Following Relationships\n");
	for (int i = 0; i < edge_files.size(); i++)
		fprintf(stdout, "\t%s : %s\n", edge_files[i].first.c_str(), edge_files[i].second.c_str());

	fprintf(stdout, "\nInitialize DiskAioParameters\n");
	// Initialize System Parameters
	DiskAioParameters::NUM_THREADS = 1;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
	DiskAioParameters::NUM_CPU_SOCKETS = 1;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = "/data/";
	
	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

	// Initialize ChunkCacheManager
	fprintf(stdout, "\nInitialize ChunkCacheManager\n");
	ChunkCacheManager::ccm = new ChunkCacheManager();

	// Run Catch Test
	fprintf(stdout, "\nTest Case Start!!\n");
	argc = 1;

	// Initialize Database
	IC();
	helper_deallocate_objects_in_shared_memory(); // Initialize shared memory for Catalog
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(nullptr);
	IC();
	Catalog& cat_instance = database->instance->GetCatalog();
	ExtentManager ext_mng; // TODO put this into database
	vector<std::pair<string, unordered_map<idx_t, idx_t>>> lid_to_pid_map;
	
	// Initialize ClientContext
	IC();
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());

	// Initialize Catalog Informations
	// Create Schema, Graph
	CreateSchemaInfo schema_info;
	cat_instance.CreateSchema(*client.get(), &schema_info);

	CreateGraphInfo graph_info("main", "graph1");
	GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);
	IC();
	// Read Vertex CSV File & CreateVertexExtents
	for (auto &vertex_file: vertex_files) {
		fprintf(stderr, "Start to load %s, %s\n", vertex_file.first.c_str(), vertex_file.second.c_str());
		// Create Partition for each vertex (partitioned by label)
		vector<string> vertex_labels = {vertex_file.first};
		string partition_name = "vpart_" + vertex_file.first;
		CreatePartitionInfo partition_info("main", partition_name.c_str());
		PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
		PartitionID new_pid = graph_cat->GetNewPartitionID();
		graph_cat->AddVertexPartition(*client.get(), new_pid, vertex_labels);
		
		fprintf(stderr, "Init GraphCSVFile\n");
		// Initialize GraphCSVFileReader
		GraphSIMDCSVFileParser reader;
		size_t approximated_num_rows = reader.InitCSVFile(vertex_file.second.c_str(), GraphComponentType::VERTEX, '|');

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
		lid_to_pid_map.emplace_back(vertex_file.first, unordered_map<idx_t, idx_t>());
		unordered_map<idx_t, idx_t> &lid_to_pid_map_instance = lid_to_pid_map.back().second;
		lid_to_pid_map_instance.reserve(approximated_num_rows);

		// Read CSV File into DataChunk & CreateVertexExtent
		while (!reader.ReadCSVFile(key_names, types, data)) {
			//fprintf(stderr, "Read CSV File Ongoing..\n");
			//fprintf(stderr, "%s\n", data.ToString().c_str());
			// Create Vertex Extent by Extent Manager
			ExtentID new_eid = ext_mng.CreateExtent(*client.get(), data, *property_schema_cat);
			property_schema_cat->AddExtent(new_eid);
			
			// Initialize pid base
			idx_t pid_base = (idx_t) new_eid;
			pid_base = pid_base << 32;

			// Build Logical id To Physical id Mapping (= LID_TO_PID_MAP)
			if (key_column_idx < 0) continue;
			idx_t* key_column = (idx_t*) data.data[key_column_idx].GetData(); // XXX idx_t type?
			for (idx_t seqno = 0; seqno < data.size(); seqno++)
				lid_to_pid_map_instance.emplace(key_column[seqno], pid_base + seqno);
		}
	}

	fprintf(stderr, "Vertex File Loading Done\n");

	// Read Edge CSV File & CreateEdgeExtents & Append Adj.List to VertexExtents
	for (auto &edge_file: edge_files) {
		fprintf(stderr, "Start to load %s, %s\n", edge_file.first.c_str(), edge_file.second.c_str());
		// Create Partition for each edge (partitioned by edge type)
		string edge_type = edge_file.first;
		string partition_name = "epart_" + edge_file.first;
		CreatePartitionInfo partition_info("main", partition_name.c_str());
		PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
		PartitionID new_pid = graph_cat->GetNewPartitionID();
		graph_cat->AddEdgePartition(*client.get(), new_pid, edge_type);

		// Initialize GraphCSVFileReader
		GraphSIMDCSVFileParser reader;
		reader.InitCSVFile(edge_file.second.c_str(), GraphComponentType::EDGE, '|');

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
		fprintf(stderr, "Src column name = %s (idx = %ld), Dst column name = %s (idx = %ld)\n", src_column_name.c_str(), src_column_idx, dst_column_name.c_str(), dst_column_idx);

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
		//fprintf(stdout, "Read Edge CSV File Ongoing..\n");

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
			end_idx = src_seqno;
			for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
				if (dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) == dst_lid_to_pid_map_instance.end()) {
				fprintf(stdout, "????? dst_seqno %ld, val %ld, src_seqno = %ld, max_seqno = %ld, begin_idx = %ld, end_idx = %ld\n", dst_seqno, dst_key_column[dst_seqno], src_seqno, max_seqno, begin_idx, end_idx);
				}
				D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
				cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
				dst_key_column[dst_seqno] = cur_dst_pid;
				adj_list_buffer.push_back(cur_dst_pid);
				adj_list_buffer.push_back(epid_base + dst_seqno);
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
				ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid);
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
		end_idx = src_seqno;
		for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
			D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
			cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
			dst_key_column[dst_seqno] = cur_dst_pid;
			adj_list_buffer.push_back(cur_dst_pid);
			adj_list_buffer.push_back(epid_base + dst_seqno);
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
		ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid);
		adj_list_chunk.Destroy();
	}
	
	// load plans
	auto suite = QueryPlanSuite(*client.get());

	// execute query
	// IC( *client.get() );

	// run queries by query name
	std::string query_str;
	std::vector<CypherPipelineExecutor*> executors;
	icecream::ic.disable();
	while(true) {
		IC();
		std::cout << ">> "; std::getline(std::cin, query_str);
		
		// if( query_str.compare("t1") == 0 ) {
		// 	executors = suite.Test1();
		// } else if( query_str.compare("t1_1") == 0 ) {
		// 	executors = suite.Test1_1();
		// } else if( query_str.compare("t1_2") == 0 ) {
		// 	executors = suite.Test1_2();
		// } else if( query_str.compare("t1_3") == 0 ) {
		// 	executors = suite.Test1_3();
		// } else if( query_str.compare("t2") == 0 ) {
		// 	executors = suite.Test2();
		// } else if( query_str.compare("t3") == 0 ) {
		// 	executors = suite.Test3();
		// } else if( query_str.compare("t5") == 0 ) {
		// 	executors = suite.Test5();
		// }
		// // else if( query_str.compare("t5") == 0 ) {
		// // 	executors = suite.Test3();
		// // }
		// else if( query_str.compare("s1") == 0 ) {
		// 	executors = suite.LDBCShort1();
		// }
		// else if( query_str.compare("s3") == 0 ) {
		// 	executors = suite.LDBCShort3();
		// } 
		// else if( query_str.compare("s4") == 0 ) {
		// 	executors = suite.LDBCShort4();
		// } 
		//  else if( query_str.compare("s5") == 0 ) {
		// 	executors = suite.LDBCShort5();
		// }
		// else {
		// 	continue;
		// }
		// Execute

		// start_timer
		boost::timer::cpu_timer query_timer;
		int idx=0;
		for( auto exec : executors ) { 
			std::cout << "[Pipeline " << 1 + idx++ << "]" << std::endl;
			//std::cout << exec->pipeline->toString() << std::endl;
			std::cout << "starting!!" << std::endl;
			exec->ExecutePipeline();
			std::cout << "done pipeline execution!!" << std::endl;
		}
		// end_timer
		int query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
		std::cout << "\ndone query exec in: " << query_exec_time_ms << " ms" << std::endl << std::endl;

		// Print result plan
		exportQueryPlanVisualizer(executors, query_exec_time_ms);
	}

	// Destruct ChunkCacheManager
  	delete ChunkCacheManager::ccm;
	return 0;
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root);

void exportQueryPlanVisualizer(std::vector<CypherPipelineExecutor*>& executors, int query_exec_time_ms) {

	// output file
	std::string curtime = boost::posix_time::to_simple_string( boost::posix_time::second_clock::universal_time() );
	std::replace( curtime.begin(), curtime.end(), ' ', '_');
	boost::filesystem::create_directories("execution-log/");
	std::cout << "saving query visualization in : " << "execution-log/" << curtime << ".html" << std::endl;
	std::ofstream file( "execution-log/" + curtime + ".html" );

	// TODO currently supports only linear query plan.
	
	// https://tomeko.net/online_tools/cpp_text_escape.php?lang=en
	std::string html_1 = "<script src=\"https://unpkg.com/vue@3.2.37/dist/vue.global.prod.js\"></script>\n<script src=\"https://unpkg.com/pev2/dist/pev2.umd.js\"></script>\n<link\n  href=\"https://unpkg.com/bootstrap@4.5.0/dist/css/bootstrap.min.css\"\n  rel=\"stylesheet\"\n/>\n<link rel=\"stylesheet\" href=\"https://unpkg.com/pev2/dist/style.css\" />\n\n<div id=\"app\">\n  <pev2 :plan-source=\"plan\" plan-query=\"\" />\n</div>\n\n<script>\n  const { createApp } = Vue\n  \n  const plan = `";
	std::string html_2 = "`\n\n  const app = createApp({\n    data() {\n      return {\n        plan: plan,\n      }\n    },\n  })\n  app.component(\"pev2\", pev2.Plan)\n  app.mount(\"#app\")\n</script>\n";

	json j = json::array( { json({}), } );
	j[0]["Execution Time"] = query_exec_time_ms;

	// reverse-iterate executors
	json* current_root = &(j[0]);
	bool isRootOp = true;
	for (auto it = executors.crbegin() ; it != executors.crend(); ++it) {
  		duckdb::CypherPipeline* pipeline = (*it)->pipeline;
		// sink first
		current_root = operatorToVisualizerJSON( current_root, pipeline->sink, isRootOp );
		isRootOp = false;
		// reverse operator
		for (auto it2 = pipeline->operators.crbegin() ; it2 != pipeline->operators.crend(); ++it2) {
			current_root = operatorToVisualizerJSON( current_root, *it2, false );
		}
		// source
		current_root = operatorToVisualizerJSON( current_root, pipeline->source, false );
	}

	file << html_1;
	file << j.dump(4);
	file << html_2;

	// close file
	file.close();
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root) {

	json* content;
	if( is_root ) {
		(*j)["Plan"] = json({});
		content = &((*j)["Plan"]);
	} else {
		(*j)["Plans"] = json::array( { json({}), } );
		content = &((*j)["Plans"][0]);
	}
	(*content)["Node Type"] = op->ToString();

	// TODO need understanding about the logics of timing in psql
	(*content)["Actual Startup Time"] = 0.0;	
	(*content)["Actual Total Time"] = op->op_timer.elapsed().wall / 1000000.0;
	(*content)["Actual Rows"] = op->processed_tuples; // TODO fix
	(*content)["Actual Loops"] = 1; // meaningless

	// TODO add operator-speciic
	// if( op->ToString() == "NExpand" ) {
	// 	(*content)["AdjFetch Time"] = ((NaiveExpand*) op)->adjfetch_time;
	// 	(*content)["TgtFetch Time"] = ((NaiveExpand*) op)->tgtfetch_time;
	// }


	return content;
}
