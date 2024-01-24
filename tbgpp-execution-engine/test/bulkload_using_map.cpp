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
#include <string>
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>

#include <nlohmann/json.hpp>	// TODO remove json and use that of boost
using json = nlohmann::json;

#include <icecream.hpp>

#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

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
#include "common/range.hpp"
#include "common/graph_csv_reader.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include "common/graph_simdjson_parser.hpp"

#define BULKLOAD_DEBUG_PRINT

using namespace duckdb;

vector<std::pair<string, string>> json_files;
vector<JsonFileType> json_file_types;
vector<vector<std::pair<string, string>>> json_file_vertices;
vector<vector<std::pair<string, string>>> json_file_edges;
vector<std::pair<string, string>> vertex_files;
vector<std::pair<string, string>> edge_files;
vector<std::pair<string, string>> edge_files_backward;
string output_dir;

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

void ParseLabelSet(string &labelset, vector<string> &parsed_labelset) {
	std::istringstream iss(labelset);
	string label;

	while (std::getline(iss, label, ':')) {
		parsed_labelset.push_back(label);
	}
}

void InitializeDiskAio() {
	fprintf(stdout, "\nInitialize Disk Aio Parameters\n"); // TODO use debug options
	// Initialize System Parameters
	DiskAioParameters::NUM_THREADS = 1;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
	DiskAioParameters::NUM_CPU_SOCKETS = 1;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = output_dir;
	
	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
}

void CreateVertexCatalogInfos(Catalog &cat_instance, std::shared_ptr<ClientContext> client, GraphCatalogEntry *graph_cat,
							  std::string &vertex_labelset_name, vector<string> &vertex_labels, vector<string> &key_names,
							  vector<LogicalType> &types, PartitionCatalogEntry *&partition_cat, PropertySchemaCatalogEntry *&property_schema_cat) {
	string partition_name = DEFAULT_VERTEX_PARTITION_PREFIX + vertex_labelset_name;
	string property_schema_name = DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX + vertex_labelset_name;
	vector<PropertyKeyID> property_key_ids;

	// Create Partition Catalog Entry
	CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
	partition_cat = 
		(PartitionCatalogEntry *)cat_instance.CreatePartition(*client.get(), &partition_info);
	PartitionID new_pid = graph_cat->GetNewPartitionID();

	// Create Property Schema Catalog Entry
	CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(),
												 new_pid, partition_cat->GetOid());
	property_schema_cat = (PropertySchemaCatalogEntry *)cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
	
	// Create Physical ID Index Catalog & Add to PartitionCatalogEntry
	CreateIndexInfo idx_info(DEFAULT_SCHEMA, vertex_labelset_name + "_id", IndexType::PHYSICAL_ID, 
							 partition_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
	IndexCatalogEntry *index_cat = (IndexCatalogEntry *)cat_instance.CreateIndex(*client.get(), &idx_info);
	
	// Set up catalog informations
	graph_cat->AddVertexPartition(*client.get(), new_pid, partition_cat->GetOid(), vertex_labels);
	graph_cat->GetPropertyKeyIDs(*client.get(), key_names, types, property_key_ids);

	partition_cat->AddPropertySchema(*client.get(), property_schema_cat->GetOid(), property_key_ids);
	partition_cat->SetKeys(*client.get(), key_names);
	partition_cat->SetTypes(types);
	partition_cat->SetPhysicalIDIndex(index_cat->GetOid());
	partition_cat->SetPartitionID(new_pid);

	property_schema_cat->SetSchema(*client.get(), key_names, types, property_key_ids);
	property_schema_cat->SetPhysicalIDIndex(index_cat->GetOid());
}

void CreateEdgeCatalogInfos(Catalog &cat_instance, std::shared_ptr<ClientContext> client, GraphCatalogEntry *graph_cat,
							  std::string &edge_type, vector<string> &key_names, vector<LogicalType> &types, string &src_column_name,
							  string &dst_column_name, PartitionCatalogEntry *&partition_cat, PropertySchemaCatalogEntry *&property_schema_cat,
							  LogicalType edge_direction_type) {
	string partition_name = DEFAULT_EDGE_PARTITION_PREFIX + edge_type;
	string property_schema_name = DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX + edge_type;
	vector<PropertyKeyID> property_key_ids;
	vector<idx_t> vertex_ps_cat_oids;
	PartitionID new_pid;

	if (edge_direction_type == LogicalType::FORWARD_ADJLIST) {
		// Create Partition Catalog Entry
		CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
		partition_cat = 
			(PartitionCatalogEntry *)cat_instance.CreatePartition(*client.get(), &partition_info);
		PartitionID new_pid = graph_cat->GetNewPartitionID();

		// Create Property Schema Catalog Entry
		CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, partition_cat->GetOid());
		property_schema_cat = (PropertySchemaCatalogEntry *)cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

		// Create Physical ID Index Catalog & Add to PartitionCatalogEntry
		CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, edge_type + "_id", IndexType::PHYSICAL_ID,
									partition_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
		IndexCatalogEntry *id_index_cat = 
			(IndexCatalogEntry *)cat_instance.CreateIndex(*client.get(), &id_idx_info);
		
		// Set up catalog informations
		graph_cat->AddEdgePartition(*client.get(), new_pid, partition_cat->GetOid(), edge_type);
		graph_cat->GetPropertyKeyIDs(*client.get(), key_names, types, property_key_ids);

		partition_cat->AddPropertySchema(*client.get(), property_schema_cat->GetOid(), property_key_ids);
		partition_cat->SetSchema(*client.get(), key_names, types, property_key_ids);
		partition_cat->SetPhysicalIDIndex(id_index_cat->GetOid());
		partition_cat->SetPartitionID(new_pid);

		property_schema_cat->SetSchema(*client.get(), key_names, types, property_key_ids);
		property_schema_cat->SetPhysicalIDIndex(id_index_cat->GetOid());

		// Get Src Vertex PS Catalog Entry
		vector<idx_t> src_vertex_part_cat_oids = 
			graph_cat->LookupPartition(*client.get(), { src_column_name }, GraphComponentType::VERTEX);
		if (src_vertex_part_cat_oids.size() != 1) throw InvalidInputException("The input src key corresponds to multiple vertex partitions.");
		PartitionCatalogEntry *src_vertex_part_cat_entry = 
			(PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, src_vertex_part_cat_oids[0]);

		vector<idx_t> dst_vertex_part_cat_oids = 
			graph_cat->LookupPartition(*client.get(), { dst_column_name }, GraphComponentType::VERTEX);
		if (dst_vertex_part_cat_oids.size() != 1) throw InvalidInputException("The input dst key corresponds to multiple vertex partitions.");
		PartitionCatalogEntry *dst_vertex_part_cat_entry = 
			(PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, dst_vertex_part_cat_oids[0]);
		src_vertex_part_cat_entry->GetPropertySchemaIDs(vertex_ps_cat_oids);
		graph_cat->AddEdgeConnectionInfo(*client.get(), src_vertex_part_cat_entry->GetOid(), partition_cat->GetOid());
		partition_cat->SetSrcDstPartOid(src_vertex_part_cat_entry->GetOid(), dst_vertex_part_cat_entry->GetOid());
	} else if (edge_direction_type == LogicalType::BACKWARD_ADJLIST) {
		partition_cat = 
			(PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, partition_name);
		property_schema_cat = 
			(PropertySchemaCatalogEntry *)cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, property_schema_name);
	} else {
		D_ASSERT(false);
	}
	
	idx_t adj_col_idx; // TODO bug fix
	for (auto i = 0; i < vertex_ps_cat_oids.size(); i++) {
		PropertySchemaCatalogEntry *vertex_ps_cat_entry = 
			(PropertySchemaCatalogEntry*)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, vertex_ps_cat_oids[i]);
		// Add Adjacency Index Info to Vertex PS Catalog Entry
		vertex_ps_cat_entry->AppendAdjListType({ edge_direction_type });
		adj_col_idx = vertex_ps_cat_entry->AppendAdjListKey(*client.get(), { edge_type });
	}

	// Create Adjacency Index Catalog
	duckdb::IndexType index_type = edge_direction_type == LogicalType::FORWARD_ADJLIST ?
		IndexType::FORWARD_CSR : IndexType::BACKWARD_CSR;
	string adj_idx_name = edge_direction_type == LogicalType::FORWARD_ADJLIST ?
		edge_type + "_fwd" : edge_type + "_bwd";
	CreateIndexInfo adj_idx_info(DEFAULT_SCHEMA, adj_idx_name, index_type,
								partition_cat->GetOid(), property_schema_cat->GetOid(), adj_col_idx, {1, 2});
	IndexCatalogEntry *adj_index_cat =
		(IndexCatalogEntry *)cat_instance.CreateIndex(*client.get(), &adj_idx_info);

	partition_cat->AddAdjIndex(adj_index_cat->GetOid());
}

void AppendAdjListChunk(ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, unordered_map<ExtentID, vector<vector<idx_t>>> &adj_list_buffers, 
	LogicalType edge_direction_type) {
	for (auto &it : adj_list_buffers) {
		ExtentID cur_vertex_extentID = it.first;
		vector<vector<idx_t>> &adj_list_buffer = it.second;
		DataChunk adj_list_chunk;
		vector<LogicalType> adj_list_chunk_types = { edge_direction_type };
		vector<data_ptr_t> adj_list_datas(1);
		
		// adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
		// TODO directly copy into buffer in AppendChunk.. to avoid copy
		vector<idx_t> tmp_adj_list_buffer;
		size_t adj_len_total = 0;
		for (size_t i = 0; i < adj_list_buffer.size(); i++) {
			adj_len_total += adj_list_buffer[i].size();
		}
		tmp_adj_list_buffer.resize(STORAGE_STANDARD_VECTOR_SIZE + adj_len_total);
		
		size_t offset = STORAGE_STANDARD_VECTOR_SIZE;
		for (size_t i = 0; i < adj_list_buffer.size(); i++) {
			for (size_t j = 0; j < adj_list_buffer[i].size(); j++) {
				tmp_adj_list_buffer[offset + j] = adj_list_buffer[i][j];
			}
			offset += adj_list_buffer[i].size();
			tmp_adj_list_buffer[i] = offset;
		}
		adj_list_datas[0] = (data_ptr_t) tmp_adj_list_buffer.data();

		adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas, STORAGE_STANDARD_VECTOR_SIZE);
		ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, cur_vertex_extentID);
		adj_list_chunk.Destroy();
	}
}

inline void FillAdjListBuffer(bool load_backward_edge, idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
					   idx_t &vertex_seqno, std::vector<int64_t> &dst_column_idx, vector<idx_t *> dst_key_columns,
					   unordered_map<LidPair, idx_t, boost::hash<LidPair>> &dst_lid_to_pid_map_instance,
					   unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_pair_to_epid_map_instance,
					   unordered_map<ExtentID, vector<vector<idx_t>>> &adj_list_buffers, idx_t epid_base,
					   idx_t src_lid = 0) {
	idx_t cur_src_seqno = GET_SEQNO_FROM_PHYSICAL_ID(cur_src_pid);

	// TODO need to be optimized
	ExtentID cur_vertex_extentID = static_cast<ExtentID>(cur_src_pid >> 32);
	vector<vector<idx_t>> *adj_list_buffer;
	auto it = adj_list_buffers.find(cur_vertex_extentID);
	if (it == adj_list_buffers.end()) {
		adj_list_buffers.emplace(cur_vertex_extentID, vector<vector<idx_t>>(STORAGE_STANDARD_VECTOR_SIZE));
		adj_list_buffer = &(adj_list_buffers.find(cur_vertex_extentID)->second);
	} else {
		adj_list_buffer = &(it->second);
	}

	idx_t dst_seqno, cur_dst_pid;
	LidPair dst_key{0, 0};
	LidPair pid_pair{cur_src_pid, 0};
	end_idx = src_seqno;
	if (load_backward_edge) {
		if (dst_column_idx.size() == 1) {
			for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
				// get dst vertex physical id
				dst_key.first = dst_key_columns[0][dst_seqno];
				cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);

				// change lid -> pid
				dst_key_columns[0][dst_seqno] = cur_dst_pid;

				// update adjlist buffer
				(*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
				(*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);

				// update pid pair to epid map
				pid_pair.second = cur_dst_pid;
				lid_pair_to_epid_map_instance->emplace(pid_pair, epid_base + dst_seqno);
			}
		} else if (dst_column_idx.size() == 2) {
			for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
				// get dst vertex physical id
				dst_key.first = dst_key_columns[0][dst_seqno];
				dst_key.second = dst_key_columns[1][dst_seqno];
				cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);

				// change lid -> pid
				dst_key_columns[0][dst_seqno] = cur_dst_pid; // TODO

				// update adjlist buffer
				(*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
				(*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);

				// update pid pair to epid map
				pid_pair.second = cur_dst_pid;
				lid_pair_to_epid_map_instance->emplace(pid_pair, epid_base + dst_seqno);
			}
		}
	} else {
		if (dst_column_idx.size() == 1) {
			for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
				// get dst vertex physical id
				dst_key.first = dst_key_columns[0][dst_seqno];
				cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);

				// change lid -> pid
				dst_key_columns[0][dst_seqno] = cur_dst_pid;

				// update adjlist buffer
				(*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
				(*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);
			}
		} else if (dst_column_idx.size() == 2) {
			for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
				// get dst vertex physical id
				dst_key.first = dst_key_columns[0][dst_seqno];
				dst_key.second = dst_key_columns[1][dst_seqno];
				cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);

				// change lid -> pid
				dst_key_columns[0][dst_seqno] = cur_dst_pid; // TODO

				// update adjlist buffer
				(*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
				(*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);
			}
		}
	}
}

inline void FillBwdAdjListBuffer(bool load_backward_edge, idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
					   idx_t &vertex_seqno, std::vector<int64_t> &dst_column_idx, vector<idx_t *> dst_key_columns,
					   unordered_map<LidPair, idx_t, boost::hash<LidPair>> &dst_lid_to_pid_map_instance,
					   unordered_map<LidPair, idx_t, boost::hash<LidPair>> &lid_pair_to_epid_map_instance,
					   unordered_map<ExtentID, vector<vector<idx_t>>> &adj_list_buffers) {
	idx_t cur_src_seqno = GET_SEQNO_FROM_PHYSICAL_ID(cur_src_pid);

	// TODO need to be optimized
	ExtentID cur_vertex_extentID = static_cast<ExtentID>(cur_src_pid >> 32);
	vector<vector<idx_t>> *adj_list_buffer;
	auto it = adj_list_buffers.find(cur_vertex_extentID);
	if (it == adj_list_buffers.end()) {
		adj_list_buffers.emplace(cur_vertex_extentID, vector<vector<idx_t>>(STORAGE_STANDARD_VECTOR_SIZE));
		adj_list_buffer = &(adj_list_buffers.find(cur_vertex_extentID)->second);
	} else {
		adj_list_buffer = &(it->second);
	}

	idx_t dst_seqno, cur_dst_pid, peid;
	LidPair dst_key{0, 0};
	LidPair pid_pair {0, cur_src_pid};
	end_idx = src_seqno;
	
	if (dst_column_idx.size() == 1) {
		for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
			// get dst vertex physical id
			dst_key.first = dst_key_columns[0][dst_seqno];
			cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);

			// get edge physical id
			pid_pair.first = cur_dst_pid;
			peid = lid_pair_to_epid_map_instance.at(pid_pair);

			// update adjlist buffer
			(*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
			(*adj_list_buffer)[cur_src_seqno].push_back(peid);
		}
	} else if (dst_column_idx.size() == 2) {
		for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
			// get dst vertex physical id
			dst_key.first = dst_key_columns[0][dst_seqno];
			dst_key.second = dst_key_columns[1][dst_seqno];
			cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);

			// get edge physical id
			pid_pair.first = cur_dst_pid;
			peid = lid_pair_to_epid_map_instance.at(pid_pair);

			// update adjlist buffer
			(*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
			(*adj_list_buffer)[cur_src_seqno].push_back(peid);
		}
	}
}

void CreateAdjListAndAppend(ExtentManager &ext_mng, std::shared_ptr<ClientContext> client) {

}

void InitializeExtentIterator() {

}

void BuildIndex() {
	// auto index_build_start = std::chrono::high_resolution_clock::now();
	// Vector row_ids(LogicalType::ROW_TYPE, true, false, data.size());
	// int64_t *row_ids_data = (int64_t *)row_ids.GetData();
	// for (idx_t seqno = 0; seqno < data.size(); seqno++) {
	// 	row_ids_data[seqno] = (int64_t)(pid_base + seqno);
	// 	// IC(seqno, row_ids_data[seqno]);
	// }
	// DataChunk tmp_chunk;
	// vector<LogicalType> tmp_types;
	// tmp_types.resize(1);
	// if (key_column_idxs.size() == 1) {
	// 	tmp_types[0] = LogicalType::UBIGINT;
	// 	tmp_chunk.Initialize(tmp_types, data.size());
	// 	tmp_chunk.data[0].Reference(data.data[key_column_idxs[0]]);
	// 	tmp_chunk.SetCardinality(data.size());
	// } else if (key_column_idxs.size() == 2) {
	// 	tmp_types[0] = LogicalType::HUGEINT;
	// 	tmp_chunk.Initialize(tmp_types, data.size());
	// 	hugeint_t *tmp_chunk_data = (hugeint_t *)tmp_chunk.data[0].GetData();
	// 	for (idx_t seqno = 0; seqno < data.size(); seqno++) {
	// 		hugeint_t key_val;
	// 		key_val.upper = data.GetValue(key_column_idxs[0], seqno).GetValue<int64_t>();
	// 		key_val.lower = data.GetValue(key_column_idxs[1], seqno).GetValue<uint64_t>();
	// 		tmp_chunk_data[seqno] = key_val;
	// 		// IC(key_val.upper, key_val.lower);
	// 		// tmp_chunk.SetValue(0, seqno, Value::HUGEINT(key_val));
	// 	}
	// 	tmp_chunk.SetCardinality(data.size());
	// } else {
	// 	throw InvalidInputException("Do not support # of compound keys >= 3 currently");
	// }
	// // tmp_types.resize(key_column_idxs.size());
	// // for (size_t i = 0; i < tmp_types.size(); i++) tmp_types[i] = LogicalType::UBIGINT;
	// // tmp_chunk.Initialize(tmp_types);
	// // for (size_t i = 0; i < tmp_types.size(); i++) tmp_chunk.data[i].Reference(data.data[key_column_idxs[i]]);
	// // IC(tmp_chunk.size());
	// IndexLock lock;
	// index->Insert(lock, tmp_chunk, row_ids);
	// auto index_build_end = std::chrono::high_resolution_clock::now();
	// std::chrono::duration<double> index_build_duration = index_build_end - index_build_start;
	// fprintf(stdout, "Index Build Elapsed: %.3f\n", index_build_duration.count());
}

void ReadVertexCSVFileAndCreateVertexExtents(Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *&graph_cat,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_to_pid_map) {
	for (auto &vertex_file: vertex_files) {
		auto vertex_file_start = std::chrono::high_resolution_clock::now();

		string &vertex_labelset = vertex_file.first;
		string &vertex_file_path = vertex_file.second;
		vector<string> vertex_labels;
		vector<string> key_names;
		vector<int64_t> key_column_idxs;
		vector<LogicalType> types;
		GraphSIMDCSVFileParser reader;
		PartitionCatalogEntry *partition_cat;
		PropertySchemaCatalogEntry *property_schema_cat;

		fprintf(stdout, "Start to load %s, %s\n", vertex_labelset.c_str(), vertex_file_path.c_str());

		// Create Partition for each vertex (partitioned by labelset)
		ParseLabelSet(vertex_labelset, vertex_labels);
		
		// Read & Parse Vertex CSV File
		auto init_csv_start = std::chrono::high_resolution_clock::now();
		size_t approximated_num_rows = reader.InitCSVFile(vertex_file_path.c_str(), GraphComponentType::VERTEX, '|');
		auto init_csv_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> init_csv_duration = init_csv_end - init_csv_start;
		fprintf(stdout, "InitCSV Elapsed: %.3f\n", init_csv_duration.count());

		// Get Schema Information From the CSV Header
		if (!reader.GetSchemaFromHeader(key_names, types)) {
			throw InvalidInputException("Invalid Schema Information");
		}
		key_column_idxs = reader.GetKeyColumnIndexFromHeader();

		// Create Catalog Infos for Vertex
		CreateVertexCatalogInfos(cat_instance, client, graph_cat, vertex_labelset, vertex_labels, key_names, types, partition_cat, property_schema_cat);
		
		// Initialize DataChunk
		DataChunk data;
		data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

		// Initialize LID_TO_PID_MAP
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_to_pid_map_instance;
		ART *index;
		if (load_edge) {
			lid_to_pid_map.emplace_back(vertex_labelset, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
			lid_to_pid_map_instance = &lid_to_pid_map.back().second;
			lid_to_pid_map_instance->reserve(approximated_num_rows);
			// vector<column_t> column_ids;
			// for (size_t i = 0; i < key_column_idxs.size(); i++) column_ids.push_back((column_t)key_column_idxs[i]);
			// index = new ART(column_ids, IndexConstraintType::NONE);
			// std::pair<string, ART*> pair_to_insert = {vertex_file.first, index};
			// lid_to_pid_index.push_back(pair_to_insert);
		}

		// Read CSV File into DataChunk & CreateVertexExtent
		auto read_chunk_start = std::chrono::high_resolution_clock::now();
		while (!reader.ReadCSVFile(key_names, types, data)) {
			auto read_chunk_end = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double> read_chunk_duration = read_chunk_end - read_chunk_start;
			fprintf(stdout, "\tRead CSV File Ongoing.. Elapsed: %.3f\n", read_chunk_duration.count());

			// Create Vertex Extent by Extent Manager
			auto create_extent_start = std::chrono::high_resolution_clock::now();
			ExtentID new_eid = ext_mng.CreateExtent(*client.get(), data, *partition_cat, *property_schema_cat);
			auto create_extent_end = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double> extent_duration = create_extent_end - create_extent_start;
			fprintf(stdout, "\tCreateExtent Elapsed: %.3f\n", extent_duration.count());
			property_schema_cat->AddExtent(new_eid, data.size());
			
			if (load_edge) {
				// Initialize pid base
				idx_t pid_base = (idx_t) new_eid;
				pid_base = pid_base << 32;

				// Build Logical id To Physical id Mapping (= LID_TO_PID_MAP)
				if (key_column_idxs.size() == 0) continue;
				auto map_build_start = std::chrono::high_resolution_clock::now();

				LidPair lid_key{0, 0};
				if (key_column_idxs.size() == 1) {
					idx_t *key_column = (idx_t *)data.data[key_column_idxs[0]].GetData();

					for (idx_t seqno = 0; seqno < data.size(); seqno++) {
						lid_key.first = key_column[seqno];
						lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
					}
				} else if (key_column_idxs.size() == 2) {
					idx_t *key_column_1 = (idx_t *)data.data[key_column_idxs[0]].GetData();
					idx_t *key_column_2 = (idx_t *)data.data[key_column_idxs[1]].GetData();

					for (idx_t seqno = 0; seqno < data.size(); seqno++) {
						lid_key.first = key_column_1[seqno];
						lid_key.second = key_column_2[seqno];
						lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
					}
				} else {
					throw InvalidInputException("Do not support # of compound keys >= 3 currently");
				}

				auto map_build_end = std::chrono::high_resolution_clock::now();
				std::chrono::duration<double> map_build_duration = map_build_end - map_build_start;
				fprintf(stdout, "Map Build Elapsed: %.3f\n", map_build_duration.count());

				// Build Index
				// BuildIndex();
			}
			read_chunk_start = std::chrono::high_resolution_clock::now();
		}
		auto vertex_file_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> duration = vertex_file_end - vertex_file_start;

		fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", vertex_file.first.c_str(), vertex_file.second.c_str(), duration.count());
	}
}

void ReadVertexJSONFileAndCreateVertexExtents(Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *&graph_cat,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_to_pid_map) {
	// Read JSON File (Assume Normal JSON File Format)
	for (unsigned idx : util::lang::indices(json_files)) {
		auto json_start = std::chrono::high_resolution_clock::now();
		fprintf(stdout, "\nStart to load %s, %s\n\n", json_files[idx].first.c_str(), json_files[idx].second.c_str());

		// Load & Parse JSON File
		GraphSIMDJSONFileParser reader(client, &ext_mng, &cat_instance);
		if (load_edge) {
			reader.SetLidToPidMap(&lid_to_pid_map);
		}

		// CreateExtent For Each Vertex
		if (json_file_types[idx] == JsonFileType::JSON) {
			D_ASSERT(false); // deactivate temporarily
			reader.InitJsonFile(json_files[idx].second.c_str(), JsonFileType::JSON);
			for (int vertex_idx = 0; vertex_idx < json_file_vertices[idx].size(); vertex_idx++) {
				fprintf(stdout, "\nLoad %s, %s\n", json_file_vertices[idx][vertex_idx].first.c_str(), json_file_vertices[idx][vertex_idx].second.c_str());
				// fflush(stdout);
				
				// Create Partition for each vertex (partitioned by label)
				vector<string> vertex_labels = {json_file_vertices[idx][vertex_idx].first};
				string partition_name = DEFAULT_VERTEX_PARTITION_PREFIX + json_file_vertices[idx][vertex_idx].first;
				PartitionID new_pid = graph_cat->GetNewPartitionID();
				CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str(), new_pid);
				PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
				graph_cat->AddVertexPartition(*client.get(), new_pid, partition_cat->GetOid(), vertex_labels);

				DataChunk data;
				reader.IterateJson(json_file_vertices[idx][vertex_idx].first.c_str(), json_file_vertices[idx][vertex_idx].second.c_str(), data, JsonFileType::JSON, graph_cat, partition_cat);
			}
		} else if (json_file_types[idx] == JsonFileType::JSONL) { // assume Neo4J format
			reader.InitJsonFile(json_files[idx].second.c_str(), JsonFileType::JSONL);
			DataChunk data;
			vector<string> label_set;
			if (json_file_vertices[idx].size() == 1 && json_file_edges[idx].size() == 0) {
				ParseLabelSet(json_file_vertices[idx][0].first, label_set);
				reader.LoadJson(json_file_vertices[idx][0].first, label_set, "", data, JsonFileType::JSONL, 
					graph_cat, nullptr, GraphComponentType::VERTEX);
			} else if (json_file_vertices[idx].size() == 0 && json_file_edges[idx].size() == 1) {
				ParseLabelSet(json_file_vertices[idx][0].first, label_set);
				reader.LoadJson(json_file_edges[idx][0].first, label_set, "", data, JsonFileType::JSONL,
					graph_cat, nullptr, GraphComponentType::EDGE);
			} else {
				D_ASSERT(false);
				// reader.LoadJson("", "", data, JsonFileType::JSONL, graph_cat, nullptr);
			}
		}
		auto json_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> load_json_duration = json_end - json_start;
		fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", json_files[idx].first.c_str(), json_files[idx].second.c_str(), load_json_duration.count());
	}
}

void ReadFwdEdgeCSVFileAndCreateEdgeExtents(Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *&graph_cat,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_to_pid_map,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_pair_to_epid_map) {
	for (auto &edge_file: edge_files) {
		auto edge_file_start = std::chrono::high_resolution_clock::now();

		string &edge_type = edge_file.first;
		string &edge_file_path = edge_file.second;
		string src_column_name;
		string dst_column_name;
		vector<string> key_names;
		vector<int64_t> src_column_idx;
		vector<int64_t> dst_column_idx;
		vector<LogicalType> types;
		GraphSIMDCSVFileParser reader;
		PartitionCatalogEntry *partition_cat;
		PropertySchemaCatalogEntry *property_schema_cat;

#ifdef BULKLOAD_DEBUG_PRINT
		fprintf(stdout, "Start to load %s, %s\n", edge_type.c_str(), edge_file_path.c_str());
#endif

		// Read & Parse Edge CSV File
		size_t approximated_num_rows = reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');

		// Get Schema Information From the CSV Header
		if (!reader.GetSchemaFromHeader(key_names, types)) {
			throw InvalidInputException("Invalid Schema Information");
		}
		reader.GetSrcColumnIndexFromHeader(src_column_idx, src_column_name);
		reader.GetDstColumnIndexFromHeader(dst_column_idx, dst_column_name);
		if (src_column_idx.size() == 0 || dst_column_idx.size() == 0) {
			throw InvalidInputException("Invalid Edge File Format");
		}

#ifdef BULKLOAD_DEBUG_PRINT
		fprintf(stdout, "Src column name = %s (idx =", src_column_name.c_str());
		for (size_t i = 0; i < src_column_idx.size(); i++) fprintf(stdout, " %ld", src_column_idx[i]);
		fprintf(stdout, "), Dst column name = %s (idx =", dst_column_name.c_str());
		for (size_t i = 0; i < dst_column_idx.size(); i++) fprintf(stdout, " %ld", dst_column_idx[i]);
		fprintf(stdout, ")\n");
#endif

		// Initialize LID_TO_PID_MAP
		auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
			[&src_column_name](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) {
				return element.first.find(src_column_name) != string::npos;
			});
		if (src_it == lid_to_pid_map.end()) throw InvalidInputException("Corresponding src vertex file was not loaded");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &src_lid_to_pid_map_instance = src_it->second;

		auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
			[&dst_column_name](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) {
				return element.first.find(dst_column_name) != string::npos;
			});
		if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("Corresponding dst vertex file was not loaded");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &dst_lid_to_pid_map_instance = dst_it->second;

		// Initialize LID_PAIR_TO_EPID_MAP
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_pair_to_epid_map_instance;
		if (load_backward_edge) {
			lid_pair_to_epid_map.emplace_back(edge_file.first, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
			lid_pair_to_epid_map_instance = &lid_pair_to_epid_map.back().second;
			lid_pair_to_epid_map_instance->reserve(approximated_num_rows);
		}
		LidPair lid_pair;

		// Initialize DataChunk
		DataChunk data;
		data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

		// Initialize AdjListBuffer
		unordered_map<ExtentID, vector<vector<idx_t>>> adj_list_buffers;

		// Create Edge Catalog Infos & Get Src vertex Catalog Entry
		CreateEdgeCatalogInfos(cat_instance, client, graph_cat, edge_type, key_names, types, src_column_name, dst_column_name,
			partition_cat, property_schema_cat, LogicalType::FORWARD_ADJLIST);

		// Initialize variables related to vertex extent
		LidPair cur_src_id, cur_dst_id, prev_id;
		idx_t cur_src_pid, prev_src_pid;
		ExtentID cur_vertex_extentID;
		idx_t vertex_seqno;
		bool is_first_tuple_processed = false;

		// Read CSV File into DataChunk & CreateEdgeExtent
		while (!reader.ReadCSVFile(key_names, types, data)) {
#ifdef BULKLOAD_DEBUG_PRINT
			fprintf(stdout, "Read Edge CSV File Ongoing..\n");
#endif

			// Get New ExtentID for this chunk
			ExtentID new_eid = partition_cat->GetNewExtentID();

			// Initialize epid base
			idx_t epid_base = (idx_t) new_eid;
			epid_base = epid_base << 32;

			// Convert lid to pid using LID_TO_PID_MAP
			vector<idx_t *> src_key_columns, dst_key_columns;
			src_key_columns.resize(src_column_idx.size());
			dst_key_columns.resize(dst_column_idx.size());
			for (size_t i = 0; i < src_key_columns.size(); i++) {
				src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
			}
			for (size_t i = 0; i < dst_key_columns.size(); i++) {
				dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();
			}
			
			idx_t src_seqno = 0;
			idx_t begin_idx = 0, end_idx;
			idx_t max_seqno = data.size();
			LidPair src_key{0, 0}, dst_key{0, 0};
			
			// For the first tuple
			if (!is_first_tuple_processed) {
				if (src_column_idx.size() == 1) {
					prev_id.first = src_key_columns[0][src_seqno];
				} else if (src_column_idx.size() == 2) {
					prev_id.first = src_key_columns[0][src_seqno];
					prev_id.second = src_key_columns[1][src_seqno];
				}
				prev_src_pid = src_lid_to_pid_map_instance.at(prev_id);
				cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
				is_first_tuple_processed = true;
				adj_list_buffers.emplace(cur_vertex_extentID, vector<vector<idx_t>>(STORAGE_STANDARD_VECTOR_SIZE));
			}

			if (src_column_idx.size() == 1) {
				while (src_seqno < max_seqno) {
					src_key.first = src_key_columns[0][src_seqno];
					cur_src_pid = src_lid_to_pid_map_instance.at(src_key);
					src_key_columns[0][src_seqno] = cur_src_pid;

					if (src_key == prev_id) {
						src_seqno++;
					} else {
						end_idx = src_seqno;
						FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
										  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
										  lid_pair_to_epid_map_instance, adj_list_buffers, epid_base);

						prev_id = src_key;
						prev_src_pid = cur_src_pid;
						begin_idx = src_seqno;
						src_seqno++;
					}
				}
			} else if (src_column_idx.size() == 2) {
				while (src_seqno < max_seqno) {
					src_key.first = src_key_columns[0][src_seqno];
					src_key.second = src_key_columns[1][src_seqno];
					cur_src_pid = src_lid_to_pid_map_instance.at(src_key);
					src_key_columns[0][src_seqno] = cur_src_pid;

					if (src_key == prev_id) {
						src_seqno++;
					} else {
						end_idx = src_seqno;
						FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
										  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
										  lid_pair_to_epid_map_instance, adj_list_buffers, epid_base);

						prev_id = src_key;
						prev_src_pid = cur_src_pid;
						begin_idx = src_seqno;
						src_seqno++;
					}
				}
			} else {
				throw InvalidInputException("Do not support # of compound keys >= 3 currently");
			}
			
			// Process remaining dst vertices
			end_idx = src_seqno;
			FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
							  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
							  lid_pair_to_epid_map_instance, adj_list_buffers, epid_base);
			
			// Create Edge Extent by Extent Manager
			ext_mng.CreateExtent(*client.get(), data, *partition_cat, *property_schema_cat, new_eid);
			property_schema_cat->AddExtent(new_eid, data.size());
		}
		
		// Process remaining adjlist
		AppendAdjListChunk(ext_mng, client, adj_list_buffers, LogicalType::FORWARD_ADJLIST);
		
		auto edge_file_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> duration = edge_file_end - edge_file_start;
#ifdef BULKLOAD_DEBUG_PRINT
		fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", edge_type.c_str(), edge_file_path.c_str(), duration.count());
#endif
	}
}

void ReadFwdEdgeJSONFileAndCreateEdgeExtents(Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *&graph_cat,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_to_pid_map) {
}

void ReadBwdEdgeCSVFileAndCreateEdgeExtents(Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *&graph_cat,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_to_pid_map,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_pair_to_epid_map) {

	for (auto &edge_file: edge_files_backward) {
		auto edge_file_start = std::chrono::high_resolution_clock::now();

		string &edge_type = edge_file.first;
		string &edge_file_path = edge_file.second;
		string src_column_name;
		string dst_column_name;
		vector<string> key_names;
		vector<int64_t> src_column_idx;
		vector<int64_t> dst_column_idx;
		vector<LogicalType> types;
		GraphSIMDCSVFileParser reader;
		PartitionCatalogEntry *partition_cat;
		PropertySchemaCatalogEntry *property_schema_cat;

#ifdef BULKLOAD_DEBUG_PRINT
		fprintf(stdout, "Start to load %s, %s\n", edge_file.first.c_str(), edge_file.second.c_str());
#endif

		// Read & Parse Edge CSV File
		reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');

		// Get Schema Information From the CSV Header
		if (!reader.GetSchemaFromHeader(key_names, types)) {
			throw InvalidInputException("Invalid Schema Information");
		}
		reader.GetDstColumnIndexFromHeader(src_column_idx, src_column_name); // Reverse
		reader.GetSrcColumnIndexFromHeader(dst_column_idx, dst_column_name); // Reverse
		if (src_column_idx.size() == 0 || dst_column_idx.size() == 0) {
			throw InvalidInputException("Invalid Edge File Format");
		}

#ifdef BULKLOAD_DEBUG_PRINT
		fprintf(stdout, "Src column name = %s (idx =", src_column_name.c_str());
		for (size_t i = 0; i < src_column_idx.size(); i++) fprintf(stdout, " %ld", src_column_idx[i]);
		fprintf(stdout, "), Dst column name = %s (idx =", dst_column_name.c_str());
		for (size_t i = 0; i < dst_column_idx.size(); i++) fprintf(stdout, " %ld", dst_column_idx[i]);
		fprintf(stdout, ")\n");
#endif

		// Initialize LID_TO_PID_MAP
		auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
			[&src_column_name](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) {
				return element.first.find(src_column_name) != string::npos;
			});
		if (src_it == lid_to_pid_map.end()) throw InvalidInputException("Corresponding src vertex file was not loaded");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &src_lid_to_pid_map_instance = src_it->second;

		auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
			[&dst_column_name](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) {
				return element.first.find(dst_column_name) != string::npos;
			});
		if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("Corresponding dst vertex file was not loaded");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &dst_lid_to_pid_map_instance = dst_it->second;

		// Initialize LID_PAIR_TO_EPID_MAP
		auto edge_it = std::find_if(lid_pair_to_epid_map.begin(), lid_pair_to_epid_map.end(),
			[&edge_type](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) { return element.first == edge_type; });
		if (edge_it == lid_pair_to_epid_map.end()) throw InvalidInputException("[Error] Lid Pair to EPid Map does not exists");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &lid_pair_to_epid_map_instance = edge_it->second;
		LidPair lid_pair;

		// Initialize DataChunk
		DataChunk data;
		data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

		// Initialize AdjListBuffer
		unordered_map<ExtentID, vector<vector<idx_t>>> adj_list_buffers;
		vector<idx_t> adj_list_buffer;

		// Create Edge Catalog Infos & Get Src vertex Catalog Entry
		CreateEdgeCatalogInfos(cat_instance, client, graph_cat, edge_type, key_names, types, src_column_name, dst_column_name,
			partition_cat, property_schema_cat, LogicalType::BACKWARD_ADJLIST);
		
		// Initialize variables related to vertex extent
		LidPair cur_src_id, cur_dst_id, prev_id;
		idx_t cur_src_pid, prev_src_pid;
		ExtentID cur_vertex_extentID;
		idx_t vertex_seqno;
		bool is_first_tuple_processed = false;

		// Read CSV File into DataChunk & CreateEdgeExtent
		while (!reader.ReadCSVFile(key_names, types, data)) {
#ifdef BULKLOAD_DEBUG_PRINT
			fprintf(stdout, "Read Edge CSV File Ongoing..\n");
#endif

			// Convert lid to pid using LID_TO_PID_MAP
			vector<idx_t *> src_key_columns, dst_key_columns;
			src_key_columns.resize(src_column_idx.size());
			dst_key_columns.resize(dst_column_idx.size());
			for (size_t i = 0; i < src_key_columns.size(); i++) src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
			for (size_t i = 0; i < dst_key_columns.size(); i++) dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();

			idx_t src_seqno = 0;
			idx_t begin_idx = 0, end_idx;
			idx_t max_seqno = data.size();
			LidPair src_key{0, 0}, dst_key{0, 0};

			// For the first tuple
			if (!is_first_tuple_processed) {
				if (src_column_idx.size() == 1) {
					prev_id.first = src_key_columns[0][src_seqno];
				} else if (src_column_idx.size() == 2) {
					prev_id.first = src_key_columns[0][src_seqno];
					prev_id.second = src_key_columns[1][src_seqno];
				}
				prev_src_pid = src_lid_to_pid_map_instance.at(prev_id);
				cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
				is_first_tuple_processed = true;
				adj_list_buffers.emplace(cur_vertex_extentID, vector<vector<idx_t>>(STORAGE_STANDARD_VECTOR_SIZE));
			}

			if (src_column_idx.size() == 1) {
				while (src_seqno < max_seqno) {
					src_key.first = src_key_columns[0][src_seqno];
					cur_src_pid = src_lid_to_pid_map_instance.at(src_key);

					if (src_key == prev_id) {
						src_seqno++;
					} else {
						end_idx = src_seqno;
						FillBwdAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
										  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
										  lid_pair_to_epid_map_instance, adj_list_buffers);

						prev_id = src_key;
						prev_src_pid = cur_src_pid;
						begin_idx = src_seqno;
						src_seqno++;
					}
				}
			} else if (src_column_idx.size() == 2) {
				while (src_seqno < max_seqno) {
					src_key.first = src_key_columns[0][src_seqno];
					src_key.second = src_key_columns[1][src_seqno];
					cur_src_pid = src_lid_to_pid_map_instance.at(src_key);

					if (src_key == prev_id) {
						src_seqno++;
					} else {
						end_idx = src_seqno;
						FillBwdAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
										  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
										  lid_pair_to_epid_map_instance, adj_list_buffers);

						prev_id = src_key;
						prev_src_pid = cur_src_pid;
						begin_idx = src_seqno;
						src_seqno++;
					}
				}
			} else {
				throw InvalidInputException("Do not support # of compound keys >= 3 currently");
			}

			// Process remaining dst vertices
			FillBwdAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
										  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
										  lid_pair_to_epid_map_instance, adj_list_buffers);
		}

		// Process remaining adjlist
		AppendAdjListChunk(ext_mng, client, adj_list_buffers, LogicalType::BACKWARD_ADJLIST);
		
		auto edge_file_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> duration = edge_file_end - edge_file_start;
#ifdef BULKLOAD_DEBUG_PRINT
		fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", edge_file.first.c_str(), edge_file.second.c_str(), duration.count());
#endif
	}
}

void ReadBwdEdgeJSONFileAndCreateEdgeExtents(Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *&graph_cat,
											 vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> &lid_to_pid_map) {
}

class InputParser{ // TODO use boost options
  public:
    InputParser (int &argc, char **argv){
    	for (int i=1; i < argc; ++i) {
			this->tokens.push_back(std::string(argv[i]));
    	}
    }
    void ParseCmdOption() const {
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
			} else if (std::strncmp(current_str.c_str(), "--json:", 7) == 0) {
				bool is_file_path_exist = false;
				std::pair<std::string, std::string> pair_to_insert;
				pair_to_insert.first = "JSON";
				std::string json_file_path_and_input_parameters = std::string(*itr).substr(7);

				size_t pos;
				std::string token;
				vector<std::string> input_parameters;

				while ((pos = json_file_path_and_input_parameters.find(' ')) != std::string::npos) {
					token = json_file_path_and_input_parameters.substr(0, pos);
					input_parameters.push_back(token);
					json_file_path_and_input_parameters.erase(0, pos + 1);
				}
				input_parameters.push_back(json_file_path_and_input_parameters);

				std::vector<std::string>::const_iterator parameter_itr;
				vector<std::pair<std::string, std::string>> vertex_label_json_expression_pairs;
				vector<std::pair<std::string, std::string>> edge_label_json_expression_pairs;
				for (parameter_itr = input_parameters.begin(); parameter_itr != input_parameters.end(); parameter_itr++) {
					std::string current_parameter = *parameter_itr;
					if (std::strncmp(current_parameter.c_str(), "--nodes:", 8) == 0) {
						std::pair<std::string, std::string> node_info;
						node_info.first = std::string(*parameter_itr).substr(8);
						parameter_itr++;
						node_info.second = *parameter_itr;
						vertex_label_json_expression_pairs.push_back(node_info);
					} else if (std::strncmp(current_parameter.c_str(), "--file_path:", 12) == 0) {
						std::string json_file_path = std::string(*parameter_itr).substr(12);
						pair_to_insert.second = json_file_path;
						json_files.push_back(pair_to_insert);
						is_file_path_exist = true;
					} else if (std::strncmp(current_parameter.c_str(), "--relationships:", 16) == 0) {
						// std::pair<std::string, std::string> pair_to_insert;
						// pair_to_insert.first = std::string(*label_itr).substr(16);
						// label_itr++;
						// pair_to_insert.second = *label_itr;
						// edge_label_json_expression_pairs.push_back(pair_to_insert);
						// load_edge = true;
					}
				}

				if (!is_file_path_exist) {
					throw InvalidInputException("There is no given input json file path");
				}

				json_file_types.push_back(JsonFileType::JSON);
				json_file_vertices.push_back(vertex_label_json_expression_pairs);
				json_file_edges.push_back(edge_label_json_expression_pairs);

				// size_t space_pos = json_file_path_and_input_parameters.find(' ');
				// if (space_pos != std::string::npos) {
				// 	// The input parameter contains label_info
				// 	std::string label_info = json_file_path_and_label_info.substr(space_pos + 1);
					
				// 	size_t pos;
				// 	std::string token;
				// 	vector<std::string> labels;
					
					
				// 	while ((pos = label_info.find(' ')) != std::string::npos) {
				// 		token = label_info.substr(0, pos);
				// 		labels.push_back(token);
				// 		label_info.erase(0, pos + 1);
				// 	}
				// 	labels.push_back(label_info);

				// 	std::vector<std::string>::const_iterator label_itr;
				// 	for (label_itr = labels.begin(); label_itr != labels.end(); label_itr++) {
				// 		std::string current_label = *label_itr;
				// 		if (std::strncmp(current_label.c_str(), "--nodes:", 8) == 0) {
				// 			std::pair<std::string, std::string> pair_to_insert;
				// 			pair_to_insert.first = std::string(*label_itr).substr(8);
				// 			label_itr++;
				// 			pair_to_insert.second = *label_itr;
				// 			vertex_label_json_expression_pairs.push_back(pair_to_insert);
				// 		} else if (std::strncmp(current_label.c_str(), "--relationships:", 16) == 0) {
				// 			std::pair<std::string, std::string> pair_to_insert;
				// 			pair_to_insert.first = std::string(*label_itr).substr(16);
				// 			label_itr++;
				// 			pair_to_insert.second = *label_itr;
				// 			edge_label_json_expression_pairs.push_back(pair_to_insert);
				// 			load_edge = true;
				// 		}
				// 	}
				// 	json_file_vertices.push_back(vertex_label_json_expression_pairs);
				// 	json_file_edges.push_back(edge_label_json_expression_pairs);
				// } else {
				// 	vector<std::pair<std::string, std::string>> vertex_label_json_expression_pairs;
				// 	vector<std::pair<std::string, std::string>> edge_label_json_expression_pairs;
				// 	json_file_vertices.push_back(vertex_label_json_expression_pairs);
				// 	json_file_edges.push_back(edge_label_json_expression_pairs);
				// }
				// std::string json_file_path = json_file_path_and_label_info.substr(0, space_pos);
				// pair_to_insert.second = json_file_path;
				// json_files.push_back(pair_to_insert);
			} else if (std::strncmp(current_str.c_str(), "--jsonl:", 8) == 0) {
				bool is_file_path_exist = false;
				std::pair<std::string, std::string> pair_to_insert;
				pair_to_insert.first = "JSONL";
				std::string json_file_path_and_input_parameters = std::string(*itr).substr(8);

				size_t pos;
				std::string token;
				vector<std::string> input_parameters;

				while ((pos = json_file_path_and_input_parameters.find(' ')) != std::string::npos) {
					token = json_file_path_and_input_parameters.substr(0, pos);
					input_parameters.push_back(token);
					json_file_path_and_input_parameters.erase(0, pos + 1);
				}
				input_parameters.push_back(json_file_path_and_input_parameters);

				std::vector<std::string>::const_iterator parameter_itr;
				vector<std::pair<std::string, std::string>> vertex_label_json_expression_pairs;
				vector<std::pair<std::string, std::string>> edge_label_json_expression_pairs;
				for (parameter_itr = input_parameters.begin(); parameter_itr != input_parameters.end(); parameter_itr++) {
					std::string current_parameter = *parameter_itr;
					if (std::strncmp(current_parameter.c_str(), "--nodes:", 8) == 0) {
						std::pair<std::string, std::string> node_info;
						node_info.first = std::string(*parameter_itr).substr(8);
						vertex_label_json_expression_pairs.push_back(node_info);
					} else if (std::strncmp(current_parameter.c_str(), "--file_path:", 12) == 0) {
						std::string json_file_path = std::string(*parameter_itr).substr(12);
						pair_to_insert.second = json_file_path;
						json_files.push_back(pair_to_insert);
						is_file_path_exist = true;
					} else if (std::strncmp(current_parameter.c_str(), "--relationships:", 16) == 0) {
						std::pair<std::string, std::string> pair_to_insert;
						pair_to_insert.first = std::string(*parameter_itr).substr(16);
						edge_label_json_expression_pairs.push_back(pair_to_insert);
						load_edge = true;
					}
				}

				if (!is_file_path_exist) {
					throw InvalidInputException("There is no given input json file path");
				}
				json_file_types.push_back(JsonFileType::JSONL);
				json_file_vertices.push_back(vertex_label_json_expression_pairs);
				json_file_edges.push_back(edge_label_json_expression_pairs);
			} else if (std::strncmp(current_str.c_str(), "--output_dir:", 13) == 0) {
				output_dir = std::string(*itr).substr(13);
			}
    	}

		// Print Bulkloading Informations
		fprintf(stdout, "\nLoad Following Nodes\n");
		for (int i = 0; i < vertex_files.size(); i++) {
			fprintf(stdout, "\t%s : %s\n", vertex_files[i].first.c_str(), vertex_files[i].second.c_str());
		}
		for (unsigned idx : util::lang::indices(json_files)) {
			for (int vertex_idx = 0; vertex_idx < json_file_vertices[idx].size(); vertex_idx++) {
				fprintf(stdout, "\t%s : %s\n", json_file_vertices[idx][vertex_idx].first.c_str(), json_files[idx].second.c_str());
			}
		}
		fprintf(stdout, "\nLoad Following Relationships\n");
		for (int i = 0; i < edge_files.size(); i++)
			fprintf(stdout, "\t%s : %s\n", edge_files[i].first.c_str(), edge_files[i].second.c_str());
		fprintf(stdout, "\nLoad Following Backward Relationships\n");
		for (int i = 0; i < edge_files_backward.size(); i++)
			fprintf(stdout, "\t%s : %s\n", edge_files_backward[i].first.c_str(), edge_files_backward[i].second.c_str());
    }
  private:
    std::vector <std::string> tokens;
};

int main(int argc, char** argv) {
	// Parse Command Option
	InputParser input(argc, argv);
	input.ParseCmdOption();

	// Initialize DiskAio Parameters
	InitializeDiskAio();

	// Initialize ChunkCacheManager
	fprintf(stdout, "\nInitialize ChunkCacheManager\n");
	ChunkCacheManager::ccm = new ChunkCacheManager(DiskAioParameters::WORKSPACE.c_str());

	// Initialize Database
	helper_deallocate_objects_in_shared_memory(); // Initialize shared memory for Catalog
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(DiskAioParameters::WORKSPACE.c_str());
	
	// Initialize ClientContext
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());

	Catalog& cat_instance = database->instance->GetCatalog();
	ExtentManager ext_mng; // TODO put this into database
	vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_to_pid_map; // For Forward & Backward AdjList
	vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_pair_to_epid_map; // For Backward AdjList
	vector<std::pair<string, ART*>> lid_to_pid_index; // For Forward & Backward AdjList

	// Initialize Graph Catalog Informations
	CreateGraphInfo graph_info(DEFAULT_SCHEMA, DEFAULT_GRAPH);
	GraphCatalogEntry *graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);

	// Read Vertex CSV File & CreateVertexExtents
	ReadVertexCSVFileAndCreateVertexExtents(cat_instance, ext_mng, client, graph_cat, lid_to_pid_map);
	// Read Vertex JSON File & CreateVertexExtents
	ReadVertexJSONFileAndCreateVertexExtents(cat_instance, ext_mng, client, graph_cat, lid_to_pid_map);
	fprintf(stdout, "Vertex File Loading Done\n\n");

	// Read Fwd Edge CSV File & CreateEdgeExtents & Append Adj.List to VertexExtents
	ReadFwdEdgeCSVFileAndCreateEdgeExtents(cat_instance, ext_mng, client, graph_cat, lid_to_pid_map, lid_pair_to_epid_map);
	// Read Fwd Edge JSON File & CreateEdgeExtents & Append Adj.List to VertexExtents
	ReadFwdEdgeJSONFileAndCreateEdgeExtents(cat_instance, ext_mng, client, graph_cat, lid_to_pid_map);
	fprintf(stdout, "Fwd Edge File Loading Done\n\n");

	// Read Bwd Edge CSV File & Append Adj.List to VertexExtents
	ReadBwdEdgeCSVFileAndCreateEdgeExtents(cat_instance, ext_mng, client, graph_cat, lid_to_pid_map, lid_pair_to_epid_map);
	// Read Bwd Edge JSON File & Append Adj.List to VertexExtents
	ReadBwdEdgeJSONFileAndCreateEdgeExtents(cat_instance, ext_mng, client, graph_cat, lid_to_pid_map);
	fprintf(stdout, "Bwd Edge File Loading Done\n\n");
	

	// Destruct ChunkCacheManager
  	delete ChunkCacheManager::ccm;
	return 0;
}