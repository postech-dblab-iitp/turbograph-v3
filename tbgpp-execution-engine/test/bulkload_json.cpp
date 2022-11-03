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
#include "common/graph_simdcsv_parser.hpp"
#include "common/graph_simdjson_parser.hpp"
#include "common/range.hpp"

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

template <typename T, int K>
class MapKey {
	using type = MapKey<T, K>;
	using value_type = T;

	T k[K];

	template<typename... Args>
		MapKey(T first, Args... args) {
			std::vector<T> vec = {first, args...};
			for (auto i = 0; i < K; i++) k[i] = vec[i];
		}

	template <typename U>
	operator MapKey<U, K>() const {
		MapKey<U, K> tmp;
		for (auto i = 0; i < K; i++) tmp[i] = k[i];
		return tmp;
	}

	MapKey() {
	}

	MapKey(T init_value_) {
		for (auto i = 0; i < K; i++) k[i] = init_value_;
	}

	~MapKey() {
	}

	void SetValue(T value_) {
		for (auto i = 0; i < K; i++) k[i] = value_;
	}

	void SetValue(idx_t idx, T value_) {
		k[idx] = value_;
	}

	T& operator[] (idx_t idx) {
		return k[idx];
	}

	const T& operator[] (idx_t idx) const {
		return k[idx];
	}

	MapKey<T, K>& operator=(const MapKey<T, K> &l) {
		for (auto i = 0; i < K; i++) 
            k[i] = l[i];
        return *this;
	}

	//template <typename t_>
	//	friend bool operator== (MapKey<t_> &l, MapKey<t_> &r);
	template <typename t_, int k_>
		friend bool operator== (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	//template <typename t_>
	//	friend bool operator!= (MapKey<t_> &l, MapKey<t_> &r);
	template <typename t_, int k_>
		friend bool operator!= (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	template <typename t_, int k_>
		friend MapKey<t_, k_> operator* (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	template <typename t_, int k_>
		friend MapKey<t_, k_> operator/ (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	template <typename t_, int k_>
		friend t_ operator* (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	template <typename t, typename t_, int k_>
		friend MapKey<t_, k_> operator* (const t &l, const MapKey<t_, k_> &r);
	template <typename t, typename t_, int k_>
		friend MapKey<t_, k_> operator* (const MapKey<t_, k_> &l, const t &r);
	template <typename t_, int k_>
		friend MapKey<t_, k_> operator+ (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	template <typename t_, int k_>
		friend MapKey<t_, k_> operator- (const MapKey<t_, k_> &l, const MapKey<t_, k_> &r);
	template <typename t_, int k_>
		friend std::ostream& operator<< (std::ostream& out, const MapKey<t_, k_> &l);
};

/*template <typename T>
  bool operator== (MapKey<T> &l, MapKey<T> &r) {
  ALWAYS_ASSERT(l.latent_factor == r.latent_factor);
  for (auto i = 0; i < l.latent_factor; i++)
  if (l[i] != r[i]) return false;
  return true;
  }*/
template <typename T, int K>
bool operator== (const MapKey<T, K> &l, const MapKey<T, K> &r) {
    // return checkEquality(l, r);
	for (auto i = 0; i < K; i++)
		if (l[i] != r[i]) return false;
	return true;
}
/*template <typename T>
  bool operator!= (MapKey<T> &l, MapKey<T> &r) {
  ALWAYS_ASSERT(l.latent_factor == r.latent_factor);
  for (auto i = 0; i < l.latent_factor; i++)
  if (l[i] != r[i]) return true;
  return false;
  }*/
template <typename T, int K>
bool operator!= (const MapKey<T, K> &l, const MapKey<T, K> &r) {
    // return !checkEquality(l, r);
	for (auto i = 0; i < K; i++)
		if (l[i] != r[i]) return true;
	return false;
}
template <typename T, int K>
MapKey<T, K> operator* (const MapKey<T, K> &l, const MapKey<T, K> &r) {
	MapKey<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] * r[i];
	return result;
}
template <typename T, int K>
MapKey<T, K> operator/ (const MapKey<T, K> &l, const MapKey<T, K> &r) {
	MapKey<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] / r[i];
	return result;
}
template <typename T, int K>
T operator* (const MapKey<T, K> &l, const MapKey<T, K> &r) {
	T result = (T)0;
	for (auto i = 0; i < K; i++)
		result += (l[i] * r[i]);
	return result;
}
template <typename S, typename T, int K>
MapKey<T, K> operator* (const S &l, const MapKey<T, K> &r) {
	MapKey<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l * r[i];
	return result;
}
template <typename S, typename T, int K>
MapKey<T, K> operator* (const MapKey<T, K> &l, const S &r) {
	MapKey<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = r * l[i];
	return result;
}
template <typename T, int K>
MapKey<T, K> operator+ (const MapKey<T, K> &l, const MapKey<T, K> &r) {
	MapKey<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] + r[i];
	return result;
}
template <typename T, int K>
MapKey<T, K> operator- (const MapKey<T, K> &l, const MapKey<T, K> &r) {
	MapKey<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] - r[i];
	return result;
}
template <typename T, int K>
std::ostream& operator<< (std::ostream& out, const MapKey<T, K> &l) {
    for (auto i = 0; i < K; i++) {
    	out << l[i] << " ";
    }
    return out;
}

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

void exportQueryPlanVisualizer(std::vector<CypherPipelineExecutor*>& executors, std::string start_time, int exec_time=0, bool is_debug=false);

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
    }
  private:
    std::vector <std::string> tokens;
};

int main(int argc, char** argv) {
icecream::ic.disable();
	// Initialize System
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
	DiskAioParameters::WORKSPACE = output_dir;
	
	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

	// Initialize ChunkCacheManager
	fprintf(stdout, "\nInitialize ChunkCacheManager\n");
	auto ccm_start = std::chrono::high_resolution_clock::now();
	ChunkCacheManager::ccm = new ChunkCacheManager(DiskAioParameters::WORKSPACE.c_str());
	auto ccm_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> init_ccm_duration = ccm_end - ccm_start;
	fprintf(stdout, "Init ChunkCacheManager Elapsed: %.3f\n", init_ccm_duration.count());

	// Run BulkLoad
	fprintf(stdout, "\nBulk Load Start!!\n");
	auto bulkload_start = std::chrono::high_resolution_clock::now();
	argc = 1;

	// Initialize Database
	helper_deallocate_objects_in_shared_memory(); // Initialize shared memory for Catalog
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(DiskAioParameters::WORKSPACE.c_str());
	
	// Initialize ClientContext
icecream::ic.disable();
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());

	Catalog& cat_instance = database->instance->GetCatalog();
	ExtentManager ext_mng; // TODO put this into database
	vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_to_pid_map; // For Forward & Backward AdjList
	vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> lid_pair_to_epid_map; // For Backward AdjList
	vector<std::pair<string, ART*>> lid_to_pid_index; // For Forward & Backward AdjList

	// Initialize Catalog Informations
	// Create Schema, Graph
	CreateSchemaInfo schema_info;
	cat_instance.CreateSchema(*client.get(), &schema_info);

	CreateGraphInfo graph_info("main", "graph1");
	GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);

	// Read JSON File (Assume Normal JSON File Format)
	for (unsigned idx : util::lang::indices(json_files)) {
		auto json_start = std::chrono::high_resolution_clock::now();
		fprintf(stdout, "\nStart to load %s, %s\n\n", json_files[idx].first.c_str(), json_files[idx].second.c_str());

		// Load & Parse JSON File
		GraphSIMDJSONFileParser reader(client, &ext_mng, &cat_instance);

		// CreateExtent For Each Vertex
		if (json_file_types[idx] == JsonFileType::JSON) {
			reader.InitJsonFile(json_files[idx].second.c_str(), JsonFileType::JSON);
			for (int vertex_idx = 0; vertex_idx < json_file_vertices[idx].size(); vertex_idx++) {
				fprintf(stdout, "\nLoad %s, %s\n", json_file_vertices[idx][vertex_idx].first.c_str(), json_file_vertices[idx][vertex_idx].second.c_str());
				fflush(stdout);
				
				// Create Partition for each vertex (partitioned by label)
				vector<string> vertex_labels = {json_file_vertices[idx][vertex_idx].first};
				string partition_name = "vpart_" + json_file_vertices[idx][vertex_idx].first;
				PartitionID new_pid = graph_cat->GetNewPartitionID();
				CreatePartitionInfo partition_info("main", partition_name.c_str(), new_pid);
				PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
				graph_cat->AddVertexPartition(*client.get(), new_pid, vertex_labels);

				DataChunk data;
				reader.IterateJson(json_file_vertices[idx][vertex_idx].first.c_str(), json_file_vertices[idx][vertex_idx].second.c_str(), data, JsonFileType::JSON, graph_cat, partition_cat);
			}
		} else if (json_file_types[idx] == JsonFileType::JSONL) {
			reader.InitJsonFile(json_files[idx].second.c_str(), JsonFileType::JSONL);
			DataChunk data;
			if (json_file_vertices[idx].size() == 1 && json_file_edges[idx].size() == 0) {
				reader.IterateJson("", "", data, JsonFileType::JSONL, graph_cat, nullptr, GraphComponentType::VERTEX);
			} else if (json_file_vertices[idx].size() == 0 && json_file_edges[idx].size() == 1) {
				reader.IterateJson("", "", data, JsonFileType::JSONL, graph_cat, nullptr, GraphComponentType::EDGE);
			} else {
				reader.IterateJson("", "", data, JsonFileType::JSONL, graph_cat, nullptr);
			}
		}
		auto json_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> load_json_duration = json_end - json_start;
		fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", json_files[idx].first.c_str(), json_files[idx].second.c_str(), load_json_duration.count());
	}

	// Destruct ChunkCacheManager
  	delete ChunkCacheManager::ccm;

	return 0;

	// Read Vertex CSV File & CreateVertexExtents
	// unique_ptr<Index> index; // Temporary..
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
		
		fprintf(stdout, "Load & Parse GraphCSVFile\n");
		auto init_csv_start = std::chrono::high_resolution_clock::now();
		// Initialize CSVFileReader
		GraphSIMDCSVFileParser reader;
		size_t approximated_num_rows = reader.InitCSVFile(vertex_file.second.c_str(), GraphComponentType::VERTEX, '|');
		auto init_csv_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> init_csv_duration = init_csv_end - init_csv_start;
		fprintf(stdout, "Load/ParseCSV Elapsed: %.3f\n", init_csv_duration.count());

		// Initialize Property Schema Catalog Entry using Schema of the vertex
		vector<string> key_names;
		vector<LogicalType> types;
		if (!reader.GetSchemaFromHeader(key_names, types)) {
			throw InvalidInputException("");
		}
		
		vector<int64_t> key_column_idxs_ = reader.GetKeyColumnIndexFromHeader();
		vector<idx_t> key_column_idxs;
		for (size_t i = 0; i < key_column_idxs_.size(); i++) key_column_idxs.push_back((idx_t) key_column_idxs_[i]);
		
		string property_schema_name = "vps_" + vertex_file.first;
		fprintf(stdout, "prop_schema_name = %s\n", property_schema_name.c_str());
		CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid);
		PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
		
		vector<PropertyKeyID> property_key_ids;
		graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
		partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
		property_schema_cat->SetTypes(types);
		property_schema_cat->SetKeys(*client.get(), key_names);
		property_schema_cat->SetKeyColumnIdxs(key_column_idxs);
// IC();
		
		// Initialize DataChunk
		DataChunk data;
		data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);
// IC();

		// Initialize LID_TO_PID_MAP
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_to_pid_map_instance;
		ART *index;
		if (load_edge) {
			lid_to_pid_map.emplace_back(vertex_file.first, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
			lid_to_pid_map_instance = &lid_to_pid_map.back().second;
			lid_to_pid_map_instance->reserve(approximated_num_rows * 2);
			// vector<column_t> column_ids;
			// for (size_t i = 0; i < key_column_idxs.size(); i++) column_ids.push_back((column_t)key_column_idxs[i]);
			// index = new ART(column_ids, IndexConstraintType::NONE);
			// std::pair<string, ART*> pair_to_insert = {vertex_file.first, index};
			// lid_to_pid_index.push_back(pair_to_insert);
		}
// IC();

		// Read CSV File into DataChunk & CreateVertexExtent
		auto read_chunk_start = std::chrono::high_resolution_clock::now();
		while (!reader.ReadCSVFile(key_names, types, data)) {
			// IC();
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
				if (key_column_idxs.size() == 0) {
					continue;
				} else if (key_column_idxs.size() == 1) {
					LidPair lid_key;
					lid_key.second = 0;
					idx_t* key_column = (idx_t*) data.data[key_column_idxs[0]].GetData();
					
					for (idx_t seqno = 0; seqno < data.size(); seqno++) {
						lid_key.first = key_column[seqno];
						lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
					}
				} else if (key_column_idxs.size() == 2) {
					LidPair lid_key;
					idx_t* key_column_1 = (idx_t*) data.data[key_column_idxs[0]].GetData();
					idx_t* key_column_2 = (idx_t*) data.data[key_column_idxs[1]].GetData();
					
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
// 				auto index_build_start = std::chrono::high_resolution_clock::now();
// 				Vector row_ids(LogicalType::ROW_TYPE, true, false, data.size());
// 				int64_t *row_ids_data = (int64_t *)row_ids.GetData();
// 				for (idx_t seqno = 0; seqno < data.size(); seqno++) {
// 					row_ids_data[seqno] = (int64_t)(pid_base + seqno);
// 					// IC(seqno, row_ids_data[seqno]);
// 				}

// 				DataChunk tmp_chunk;
// 				vector<LogicalType> tmp_types;
// 				tmp_types.resize(1);
// 				if (key_column_idxs.size() == 1) {
// 					tmp_types[0] = LogicalType::UBIGINT;
// 					tmp_chunk.Initialize(tmp_types, data.size());
// 					tmp_chunk.data[0].Reference(data.data[key_column_idxs[0]]);
// 					tmp_chunk.SetCardinality(data.size());
// 				} else if (key_column_idxs.size() == 2) {
// 					tmp_types[0] = LogicalType::HUGEINT;
// 					tmp_chunk.Initialize(tmp_types, data.size());
// 					hugeint_t *tmp_chunk_data = (hugeint_t *)tmp_chunk.data[0].GetData();
// 					for (idx_t seqno = 0; seqno < data.size(); seqno++) {
// 						hugeint_t key_val;
// 						key_val.upper = data.GetValue(key_column_idxs[0], seqno).GetValue<int64_t>();
// 						key_val.lower = data.GetValue(key_column_idxs[1], seqno).GetValue<uint64_t>();
// 						tmp_chunk_data[seqno] = key_val;
// 						// IC(key_val.upper, key_val.lower);
// 						// tmp_chunk.SetValue(0, seqno, Value::HUGEINT(key_val));
// 					}
// 					tmp_chunk.SetCardinality(data.size());
// 				} else {
// 					throw InvalidInputException("Do not support # of compound keys >= 3 currently");
// 				}
// // IC();
// 				// tmp_types.resize(key_column_idxs.size());
// 				// for (size_t i = 0; i < tmp_types.size(); i++) tmp_types[i] = LogicalType::UBIGINT;
// 				// tmp_chunk.Initialize(tmp_types);
// 				// for (size_t i = 0; i < tmp_types.size(); i++) tmp_chunk.data[i].Reference(data.data[key_column_idxs[i]]);
// 				// IC(tmp_chunk.size());
// 				IndexLock lock;
// 				index->Insert(lock, tmp_chunk, row_ids);
// 				auto index_build_end = std::chrono::high_resolution_clock::now();
// 				std::chrono::duration<double> index_build_duration = index_build_end - index_build_start;
// 				fprintf(stdout, "Index Build Elapsed: %.3f\n", index_build_duration.count());
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
		// for (size_t i = 0; i < key_names.size(); i++) IC(key_names[i]);

		vector<int64_t> src_column_idx, dst_column_idx;
		string src_column_name, dst_column_name;
		reader.GetSrcColumnIndexFromHeader(src_column_idx, src_column_name);
		reader.GetDstColumnIndexFromHeader(dst_column_idx, dst_column_name);
		if (src_column_idx.size() == 0 || dst_column_idx.size() == 0) throw InvalidInputException("B");
		fprintf(stdout, "Src column name = %s (idx =", src_column_name.c_str());
		for (size_t i = 0; i < src_column_idx.size(); i++) fprintf(stdout, " %ld", src_column_idx[i]);
		fprintf(stdout, "), Dst column name = %s (idx =", dst_column_name.c_str());
		for (size_t i = 0; i < dst_column_idx.size(); i++) fprintf(stdout, " %ld", dst_column_idx[i]);
		fprintf(stdout, ")\n");

		auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
			[&src_column_name](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) { return element.first == src_column_name; });
		if (src_it == lid_to_pid_map.end()) throw InvalidInputException("Corresponding src vertex file not loaded");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &src_lid_to_pid_map_instance = src_it->second;

		auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
			[&dst_column_name](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) { return element.first == dst_column_name; });
		if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("Corresponding dst vertex file not loaded");
		unordered_map<LidPair, idx_t, boost::hash<LidPair>> &dst_lid_to_pid_map_instance = dst_it->second;

		string property_schema_name = "eps_" + edge_file.first;
		CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid);
		PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
		vector<PropertyKeyID> property_key_ids;
		graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
		partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
		property_schema_cat->SetTypes(types);
		property_schema_cat->SetKeys(*client.get(), key_names);

		// for (size_t i = 0; i < property_key_ids.size(); i++) IC(property_key_ids[i]);

		// Initialize DataChunk
		DataChunk data;
		data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

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
		adj_list_buffer.resize(STORAGE_STANDARD_VECTOR_SIZE);

		// Initialize Extent Iterator for Vertex Extents
		ExtentIterator ext_it;
		PropertySchemaCatalogEntry* vertex_ps_cat_entry = 
			(PropertySchemaCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, "main", "vps_" + src_column_name);
		vector<idx_t> src_column_idxs = move(vertex_ps_cat_entry->GetKeyColumnIdxs());
// icecream::ic.enable(); IC(); IC(src_column_idxs.size()); for (size_t i = 0; i < src_column_idxs.size(); i++) IC(src_column_idxs[i]); icecream::ic.disable();
		vector<LogicalType> vertex_id_type;
		for (size_t i = 0; i < src_column_idxs.size(); i++) vertex_id_type.push_back(LogicalType::UBIGINT);
		ext_it.Initialize(*client.get(), vertex_ps_cat_entry, vertex_id_type, src_column_idxs);
		vertex_ps_cat_entry->AppendType({ LogicalType::FORWARD_ADJLIST });
		vertex_ps_cat_entry->AppendKey(*client.get(), { edge_type });

		// Initialize variables related to vertex extent
		LidPair cur_src_id, cur_dst_id, min_id, max_id, prev_id; // Logical ID
		idx_t cur_src_pid, cur_dst_pid; // Physical ID
		idx_t vertex_seqno;
		idx_t *vertex_id_column_1, *vertex_id_column_2;
		DataChunk vertex_id_chunk;
		ExtentID current_vertex_eid;
		bool is_first_tuple_processed = false;
		vertex_id_chunk.Initialize(vertex_id_type, STORAGE_STANDARD_VECTOR_SIZE);
		cur_src_id.second = cur_dst_id.second = min_id.second = max_id.second = 0;

		// Read CSV File into DataChunk & CreateEdgeExtent
		while (!reader.ReadCSVFile(key_names, types, data)) {
			fprintf(stdout, "Read Edge CSV File Ongoing..\n");
// IC(data.ToString(10));
			// Get New ExtentID for this chunk
			ExtentID new_eid = property_schema_cat->GetNewExtentID();

			// Initialize epid base
			idx_t epid_base = (idx_t) new_eid;
			epid_base = epid_base << 32;
// 			IC(new_eid, epid_base);
// IC();
			// Convert lid to pid using LID_TO_PID_MAP
			vector<idx_t *> src_key_columns, dst_key_columns;
			src_key_columns.resize(src_column_idx.size());
			dst_key_columns.resize(dst_column_idx.size());
			for (size_t i = 0; i < src_key_columns.size(); i++) src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
			for (size_t i = 0; i < dst_key_columns.size(); i++) dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();
			// idx_t *src_key_column = (idx_t*) data.data[src_column_idx].GetData();
			// idx_t *dst_key_column = (idx_t*) data.data[dst_column_idx].GetData();
// IC();
			idx_t src_seqno = 0, dst_seqno = 0;
			idx_t begin_idx, end_idx;
			idx_t max_seqno = data.size();
			// idx_t prev_id = ULLONG_MAX;

			// For the first tuple
			begin_idx = src_seqno;
			// D_ASSERT(src_lid_to_pid_map_instance.find(src_key_column[src_seqno]) != src_lid_to_pid_map_instance.end());
			LidPair key, dst_key;
			// if (src_column_idx.size() == 1) {
			// 	key.first = src_key_columns[0][src_seqno];
			// 	key.second = 0;
			// 	cur_src_id = key;
			// 	if (src_column_idx.size() == 1 && dst_column_idx.size() == 1 && (cur_src_id.first > 4194300 && cur_src_id.first < 4194310)) {
			// 		icecream::ic.enable(); IC(); IC(src_seqno, cur_src_id.first); icecream::ic.disable();
			// 	}
			// } else if (src_column_idx.size() == 2) {
			// 	key.first = src_key_columns[0][src_seqno];
			// 	key.second = src_key_columns[1][src_seqno];
			// 	cur_src_id = key;
			// } else throw InvalidInputException("Do not support # of compound keys >= 3 currently");
			// cur_src_pid = src_lid_to_pid_map_instance.at(key);
			
			// src_key_columns[0][src_seqno] = cur_src_pid; // TODO.. compound key columns -> one physical id column ?
			// src_seqno++;

			while(src_seqno < max_seqno) {
				// Get Cur_Src_ID
				if (src_column_idx.size() == 1) {
					key.first = src_key_columns[0][src_seqno];
					key.second = 0;
					cur_src_id = key;
					// if (src_column_idx.size() == 1 && dst_column_idx.size() == 1 && (cur_src_id.first > 4194300 && cur_src_id.first < 4194310)) {
					// 	icecream::ic.enable(); IC(); IC(src_seqno, cur_src_id.first); icecream::ic.disable();
					// }
				} else if (src_column_idx.size() == 2) {
					key.first = src_key_columns[0][src_seqno];
					key.second = src_key_columns[1][src_seqno];
					cur_src_id = key;
				} else throw InvalidInputException("Do not support # of compound keys >= 3 currently");

				if (!is_first_tuple_processed) {
					// Get First Vertex Extent
					while (true) {
	// IC();
						if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, STORAGE_STANDARD_VECTOR_SIZE)) {
							// We do not allow this case
							throw InvalidInputException("GetNextExtent Fail - The vertex chunk containing the first vertex does not exist");
						}
	// IC();
						
						// Initialize min & max id. We assume that the vertex data is sorted by id
						if (src_column_idxs.size() == 1) {
							vertex_id_column_1 = (idx_t*) vertex_id_chunk.data[0].GetData();
							min_id.first = vertex_id_column_1[0];
							max_id.first = vertex_id_column_1[vertex_id_chunk.size() - 1];
						} else if (src_column_idxs.size() == 2) {
							vertex_id_column_1 = (idx_t*) vertex_id_chunk.data[0].GetData();
							vertex_id_column_2 = (idx_t*) vertex_id_chunk.data[1].GetData();
							min_id.first = vertex_id_column_1[0];
							min_id.second = vertex_id_column_2[0];
							max_id.first = vertex_id_column_1[vertex_id_chunk.size() - 1];
							max_id.second = vertex_id_column_2[vertex_id_chunk.size() - 1];
						}
						if (cur_src_id >= min_id && cur_src_id <= max_id) break;
					}

					// Initialize vertex_seqno
					vertex_seqno = 0;
					if (src_column_idxs.size() == 1) {
						while (vertex_id_column_1[vertex_seqno] < cur_src_id.first) {
							adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
						}
					} else if (src_column_idxs.size() == 2) {
						while ((vertex_id_column_1[vertex_seqno] < cur_src_id.first) ||
							((vertex_id_column_1[vertex_seqno] == cur_src_id.first) && (vertex_id_column_2[vertex_seqno] < cur_src_id.second))) {
							adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
						}
					}
					is_first_tuple_processed = true;
					prev_id = cur_src_id;
				}

				cur_src_pid = src_lid_to_pid_map_instance.at(key);
				src_key_columns[0][src_seqno] = cur_src_pid;
				if (cur_src_id == prev_id) {
					src_seqno++;
				} else {
					// lid_pair.first = prev_id;
					end_idx = src_seqno;
					if (load_backward_edge) {
						D_ASSERT(false);
						// for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
						// 	// if (dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) == dst_lid_to_pid_map_instance.end()) {
						// 	// 	fprintf(stdout, "????? dst_seqno %ld, val %ld, src_seqno = %ld, max_seqno = %ld, begin_idx = %ld, end_idx = %ld\n", dst_seqno, dst_key_column[dst_seqno], src_seqno, max_seqno, begin_idx, end_idx);
						// 	// }
						// 	D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
						// 	cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
						// 	lid_pair.second = dst_key_column[dst_seqno];
						// 	dst_key_column[dst_seqno] = cur_dst_pid;
						// 	adj_list_buffer.push_back(cur_dst_pid);
						// 	adj_list_buffer.push_back(epid_base + dst_seqno);
						// 	lid_pair_to_epid_map_instance->emplace(lid_pair, epid_base + dst_seqno);
						// }
					} else {
						if (dst_column_idx.size() == 1) {
							for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
								dst_key.first = dst_key_columns[0][dst_seqno];
								dst_key.second = 0;
								cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
								dst_key_columns[0][dst_seqno] = cur_dst_pid;
								adj_list_buffer.push_back(cur_dst_pid);
								adj_list_buffer.push_back(epid_base + dst_seqno);
							}
						} else if (dst_column_idx.size() == 2) {
							for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
								dst_key.first = dst_key_columns[0][dst_seqno];
								dst_key.second = dst_key_columns[1][dst_seqno];
								cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
								dst_key_columns[0][dst_seqno] = cur_dst_pid; // TODO
								adj_list_buffer.push_back(cur_dst_pid);
								adj_list_buffer.push_back(epid_base + dst_seqno);
							}
						} else throw InvalidInputException("Do not support # of compound keys >= 3 currently");
					}
					
					adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();

					if (cur_src_id > max_id) {
						// Fill offsets
						idx_t last_offset = adj_list_buffer.size();
						for (size_t i = vertex_seqno; i < STORAGE_STANDARD_VECTOR_SIZE; i++)
							adj_list_buffer[i] = last_offset;

						// AddChunk for Adj.List to current Src Vertex Extent
						DataChunk adj_list_chunk;
						vector<LogicalType> adj_list_chunk_types = { LogicalType::FORWARD_ADJLIST };
						vector<data_ptr_t> adj_list_datas(1);
						adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
						adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas, STORAGE_STANDARD_VECTOR_SIZE);
						ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid);
						adj_list_chunk.Destroy();

						// Re-initialize adjlist buffer for next Extent
						adj_list_buffer.resize(STORAGE_STANDARD_VECTOR_SIZE);

						// Read corresponding ID column of Src Vertex Extent
						while (true) {
							if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, STORAGE_STANDARD_VECTOR_SIZE)) {
								// We do not allow this case
								throw InvalidInputException("F");
							}
						
							// Initialize min & max id

							if (src_column_idxs.size() == 1) {
								vertex_id_column_1 = (idx_t*) vertex_id_chunk.data[0].GetData();
								min_id.first = vertex_id_column_1[0];
								max_id.first = vertex_id_column_1[vertex_id_chunk.size() - 1];
							} else if (src_column_idxs.size() == 2) {
								vertex_id_column_1 = (idx_t*) vertex_id_chunk.data[0].GetData();
								vertex_id_column_2 = (idx_t*) vertex_id_chunk.data[1].GetData();
								min_id.first = vertex_id_column_1[0];
								min_id.second = vertex_id_column_2[0];
								max_id.first = vertex_id_column_1[vertex_id_chunk.size() - 1];
								max_id.second = vertex_id_column_2[vertex_id_chunk.size() - 1];
							}
							if (cur_src_id >= min_id && cur_src_id <= max_id) break;
						}
						
						// Initialize vertex_seqno
						vertex_seqno = 0;
						if (src_column_idxs.size() == 1) {
							while (vertex_id_column_1[vertex_seqno] < cur_src_id.first) {
								// icecream::ic.enable(); IC(current_vertex_eid, vertex_seqno, vertex_id_column_1[vertex_seqno], cur_src_id.first); icecream::ic.disable();
								adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
							}
						} else if (src_column_idxs.size() == 2) {
							while ((vertex_id_column_1[vertex_seqno] < cur_src_id.first) ||
								((vertex_id_column_1[vertex_seqno] == cur_src_id.first) && (vertex_id_column_2[vertex_seqno] < cur_src_id.second))) {
								adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
							}
						}
					} else {
						if (src_column_idxs.size() == 1) {
							while (vertex_id_column_1[vertex_seqno] < cur_src_id.first) {
								adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
							}
						} else if (src_column_idxs.size() == 2) {
							while ((vertex_id_column_1[vertex_seqno] < cur_src_id.first) ||
								((vertex_id_column_1[vertex_seqno] == cur_src_id.first) && (vertex_id_column_2[vertex_seqno] < cur_src_id.second))) {
								adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
							}
						}
					}

					prev_id = cur_src_id;
					begin_idx = src_seqno;
					src_seqno++;
				}
			}
// IC();
			// Process remaining dst vertices
			// lid_pair.first = prev_id;
			end_idx = src_seqno;
			if (load_backward_edge) {
				D_ASSERT(false);
				// for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
				// 	D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
				// 	cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
				// 	lid_pair.second = dst_key_column[dst_seqno];
				// 	dst_key_column[dst_seqno] = cur_dst_pid;
				// 	adj_list_buffer.push_back(cur_dst_pid);
				// 	adj_list_buffer.push_back(epid_base + dst_seqno);
				// 	lid_pair_to_epid_map_instance->emplace(lid_pair, epid_base + dst_seqno);
				// }
			} else {
				if (dst_column_idx.size() == 1) {
					for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
						dst_key.first = dst_key_columns[0][dst_seqno];
						dst_key.second = 0;
						cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
						dst_key_columns[0][dst_seqno] = cur_dst_pid;
						adj_list_buffer.push_back(cur_dst_pid);
						adj_list_buffer.push_back(epid_base + dst_seqno);
					}
				} else if (dst_column_idx.size() == 2) {
					for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
						dst_key.first = dst_key_columns[0][dst_seqno];
						dst_key.second = dst_key_columns[1][dst_seqno];
						cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
						dst_key_columns[0][dst_seqno] = cur_dst_pid; // TODO
						adj_list_buffer.push_back(cur_dst_pid);
						adj_list_buffer.push_back(epid_base + dst_seqno);
						// IC(src_seqno, dst_seqno, epid_base, cur_dst_pid, epid_base + dst_seqno);
					}
				} else throw InvalidInputException("Do not support # of compound keys >= 3 currently");
			}

			// Create Edge Extent by Extent Manager
			ext_mng.CreateExtent(*client.get(), data, *property_schema_cat, new_eid);
			property_schema_cat->AddExtent(new_eid);
		}
		
		// Process remaining adjlist
		// Fill offsets
		idx_t last_offset = adj_list_buffer.size();
		for (size_t i = vertex_seqno; i < STORAGE_STANDARD_VECTOR_SIZE; i++)
			adj_list_buffer[i] = last_offset;

		// AddChunk for Adj.List to current Src Vertex Extent
		DataChunk adj_list_chunk;
		vector<LogicalType> adj_list_chunk_types = { LogicalType::FORWARD_ADJLIST };
		vector<data_ptr_t> adj_list_datas(1);
		adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
		adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas, STORAGE_STANDARD_VECTOR_SIZE);
		ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid);
		adj_list_chunk.Destroy();

		auto edge_file_end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> duration = edge_file_end - edge_file_start;
// icecream::ic.disable();
		fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", edge_file.first.c_str(), edge_file.second.c_str(), duration.count());
	}

	// Read Backward Edge CSV File & Append Adj.List to VertexExtents
	for (auto &edge_file: edge_files_backward) {
		D_ASSERT(false);
		// auto edge_file_start = std::chrono::high_resolution_clock::now();
		// fprintf(stdout, "Start to load %s, %s\n", edge_file.first.c_str(), edge_file.second.c_str());
		// // Get Partition for each edge (partitioned by edge type).
		// string edge_type = edge_file.first;
		// string partition_name = "epart_" + edge_file.first;
		
		// // CreatePartitionInfo partition_info("main", partition_name.c_str());
		// // PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
		// // PartitionID new_pid = graph_cat->GetNewPartitionID();
		// // graph_cat->AddEdgePartition(*client.get(), new_pid, edge_type);

		// // Initialize CSVFileReader
		// GraphSIMDCSVFileParser reader;
		// reader.InitCSVFile(edge_file.second.c_str(), GraphComponentType::EDGE, '|');

		// // Initialize LID_PAIR_TO_EPID_MAP
		// auto edge_it = std::find_if(lid_pair_to_epid_map.begin(), lid_pair_to_epid_map.end(),
		// 	[&edge_type](const std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>> &element) { return element.first == edge_type; });
		// if (edge_it == lid_pair_to_epid_map.end()) throw InvalidInputException("[Error] Lid Pair to EPid Map does not exists");
		// unordered_map<LidPair, idx_t, boost::hash<LidPair>> &lid_pair_to_epid_map_instance = edge_it->second;
		// LidPair lid_pair;

		// // Initialize Property Schema Info using Schema of the edge
		// vector<string> key_names;
		// vector<LogicalType> types;
		// if (!reader.GetSchemaFromHeader(key_names, types)) {
		// 	throw InvalidInputException("[Error] GetSchemaFromHeader");
		// }

		// int64_t src_column_idx, dst_column_idx;
		// string src_column_name, dst_column_name;
		// reader.GetDstColumnIndexFromHeader(src_column_idx, src_column_name); // Reverse
		// reader.GetSrcColumnIndexFromHeader(dst_column_idx, dst_column_name); // Reverse
		// if (src_column_idx < 0 || dst_column_idx < 0) throw InvalidInputException("[Error] GetSrc/DstColumnIndexFromHeader");
		// fprintf(stdout, "Src column name = %s (idx = %ld), Dst column name = %s (idx = %ld)\n", src_column_name.c_str(), src_column_idx, dst_column_name.c_str(), dst_column_idx);

		// auto src_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
		// 	[&src_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == src_column_name; });
		// if (src_it == lid_to_pid_map.end()) throw InvalidInputException("[Error] Src Lid to Pid Map does not exists");
		// unordered_map<idx_t, idx_t> &src_lid_to_pid_map_instance = src_it->second;

		// auto dst_it = std::find_if(lid_to_pid_map.begin(), lid_to_pid_map.end(),
		// 	[&dst_column_name](const std::pair<string, unordered_map<idx_t, idx_t>> &element) { return element.first == dst_column_name; });
		// if (dst_it == lid_to_pid_map.end()) throw InvalidInputException("[Error] Dst Lid to Pid Map does not exists");
		// unordered_map<idx_t, idx_t> &dst_lid_to_pid_map_instance = dst_it->second;

		// // string property_schema_name = "eps_" + edge_file.first;
		// // CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid);
		// // PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
		// // vector<PropertyKeyID> property_key_ids;
		// // graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
		// // partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
		// // property_schema_cat->SetTypes(types);
		// // property_schema_cat->SetKeys(key_names);

		// // Initialize DataChunk
		// DataChunk data;
		// data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

		// // Initialize AdjListBuffer
		// vector<idx_t> adj_list_buffer;
		// adj_list_buffer.resize(STORAGE_STANDARD_VECTOR_SIZE);

		// // Initialize Extent Iterator for Vertex Extents
		// ExtentIterator ext_it;
		// PropertySchemaCatalogEntry* vertex_ps_cat_entry = 
		// (PropertySchemaCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, "main", "vps_" + src_column_name);
		// vector<string> key_column_name = { "id" };
		// vector<idx_t> src_column_idxs = move(vertex_ps_cat_entry->GetColumnIdxs(key_column_name));
		// vector<LogicalType> vertex_id_type = { LogicalType::UBIGINT };
		// ext_it.Initialize(*client.get(), vertex_ps_cat_entry, vertex_id_type, src_column_idxs);
		// vertex_ps_cat_entry->AppendType({ LogicalType::BACKWARD_ADJLIST });
		// vertex_ps_cat_entry->AppendKey({ edge_type });

		// // Initialize variables related to vertex extent
		// idx_t cur_src_id, cur_dst_id, cur_src_pid, cur_dst_pid;
		// idx_t min_id = ULLONG_MAX, max_id = ULLONG_MAX, vertex_seqno;
		// idx_t *vertex_id_column;
		// DataChunk vertex_id_chunk;
		// ExtentID current_vertex_eid;
		// vertex_id_chunk.Initialize({ LogicalType::UBIGINT }, STORAGE_STANDARD_VECTOR_SIZE);

		// // Read CSV File into DataChunk & CreateEdgeExtent
		// while (!reader.ReadCSVFile(key_names, types, data)) {
		// 	//fprintf(stdout, "Read Edge CSV File Ongoing..\n");

		// 	// Convert lid to pid using LID_TO_PID_MAP
		// 	idx_t *src_key_column = (idx_t*) data.data[src_column_idx].GetData();
		// 	idx_t *dst_key_column = (idx_t*) data.data[dst_column_idx].GetData();

		// 	idx_t src_seqno = 0, dst_seqno = 0;
		// 	idx_t begin_idx, end_idx;
		// 	idx_t max_seqno = data.size();
		// 	idx_t prev_id = ULLONG_MAX;

		// 	// For the first tuple
		// 	begin_idx = src_seqno;
		// 	prev_id = cur_src_id = src_key_column[src_seqno];
			
		// 	D_ASSERT(src_lid_to_pid_map_instance.find(src_key_column[src_seqno]) != src_lid_to_pid_map_instance.end());
		// 	cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
		// 	// src_key_column[src_seqno] = cur_src_pid;
		// 	src_seqno++;

		// 	if (min_id == ULLONG_MAX) {
		// 		// Get First Vertex Extent
		// 		while (true) {
		// 			if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, STORAGE_STANDARD_VECTOR_SIZE)) {
		// 				// We do not allow this case
		// 				throw InvalidInputException("E"); 
		// 			}
					
		// 			// Initialize min & max id
		// 			vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
		// 			min_id = vertex_id_column[0];
		// 			max_id = vertex_id_column[vertex_id_chunk.size() - 1];
		// 			fprintf(stdout, "min_id = %ld, cur_src_id = %ld, max_id = %ld\n", min_id, cur_src_id, max_id);
		// 			if (cur_src_id >= min_id && cur_src_id <= max_id) break;
		// 		}

		// 		// Initialize vertex_seqno
		// 		vertex_seqno = 0;
		// 		vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
		// 		while (vertex_id_column[vertex_seqno] < cur_src_id) {
		// 			adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
		// 		}
		// 		if (vertex_id_column[vertex_seqno] != cur_src_id) {
		// 			fprintf(stdout, "Error at %ld, %ld != %ld\n", vertex_seqno, vertex_id_column[vertex_seqno], cur_src_id);
		// 		}
		// 		D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
		// 	}
			
		// 	while(src_seqno < max_seqno) {
		// 		cur_src_id = src_key_column[src_seqno];
		// 		cur_src_pid = src_lid_to_pid_map_instance.at(src_key_column[src_seqno]);
		// 		// src_key_column[src_seqno] = cur_src_pid;
		// 		if (cur_src_id == prev_id) {
		// 			src_seqno++;
		// 		} else {
		// 			lid_pair.second = prev_id;
		// 			end_idx = src_seqno;
		// 			idx_t peid;
		// 			for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
		// 				if (dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) == dst_lid_to_pid_map_instance.end()) {
		// 					fprintf(stdout, "????? dst_seqno %ld, val %ld, src_seqno = %ld, max_seqno = %ld, begin_idx = %ld, end_idx = %ld\n", dst_seqno, dst_key_column[dst_seqno], src_seqno, max_seqno, begin_idx, end_idx);
		// 				}
		// 				D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
		// 				cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
		// 				lid_pair.first = dst_key_column[dst_seqno];
		// 				if (lid_pair_to_epid_map_instance.find(lid_pair) == lid_pair_to_epid_map_instance.end()) {
		// 					fprintf(stdout, "????? cannot find lid_pair %ld %ld\n", lid_pair.first, lid_pair.second);
		// 				}
		// 				D_ASSERT(lid_pair_to_epid_map_instance.find(lid_pair) != lid_pair_to_epid_map_instance.end());
		// 				peid = lid_pair_to_epid_map_instance.at(lid_pair);
		// 				// dst_key_column[dst_seqno] = cur_dst_pid;
		// 				adj_list_buffer.push_back(cur_dst_pid);
		// 				adj_list_buffer.push_back(peid);
		// 			}
		// 			adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();

		// 			if (cur_src_id > max_id) {
		// 				// Fill offsets
		// 				idx_t last_offset = adj_list_buffer.size();
		// 				for (size_t i = vertex_seqno; i < STORAGE_STANDARD_VECTOR_SIZE; i++)
		// 					adj_list_buffer[i] = last_offset;

		// 				// AddChunk for Adj.List to current Src Vertex Extent
		// 				DataChunk adj_list_chunk;
		// 				vector<LogicalType> adj_list_chunk_types = { LogicalType::BACKWARD_ADJLIST };
		// 				vector<data_ptr_t> adj_list_datas(1);
		// 				adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
		// 				adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas, STORAGE_STANDARD_VECTOR_SIZE);
		// 				ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid);
		// 				adj_list_chunk.Destroy();

		// 				// Re-initialize adjlist buffer for next Extent
		// 				adj_list_buffer.resize(STORAGE_STANDARD_VECTOR_SIZE);

		// 				// Read corresponding ID column of Src Vertex Extent
		// 				while (true) {
		// 					if (!ext_it.GetNextExtent(*client.get(), vertex_id_chunk, current_vertex_eid, STORAGE_STANDARD_VECTOR_SIZE)) {
		// 						// We do not allow this case
		// 						throw InvalidInputException("F");
		// 					}
						
		// 					// Initialize min & max id
		// 					vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
		// 					min_id = vertex_id_column[0];
		// 					max_id = vertex_id_column[vertex_id_chunk.size() - 1];
		// 					if (cur_src_id >= min_id && cur_src_id <= max_id) break;
		// 				}
						
		// 				// Initialize vertex_seqno
		// 				vertex_seqno = 0;
		// 				vertex_id_column = (idx_t*) vertex_id_chunk.data[0].GetData();
		// 				while (vertex_id_column[vertex_seqno] < cur_src_id) {
		// 					adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
		// 				}
		// 				D_ASSERT(vertex_id_column[vertex_seqno] == cur_src_id);
		// 			} else {
		// 				while (vertex_id_column[vertex_seqno] < cur_src_id) {
		// 					adj_list_buffer[vertex_seqno++] = adj_list_buffer.size();
		// 					D_ASSERT(vertex_seqno < STORAGE_STANDARD_VECTOR_SIZE);
		// 				}
		// 			}

		// 			prev_id = cur_src_id;
		// 			begin_idx = src_seqno;
		// 			src_seqno++;
		// 		}
		// 	}

		// 	// Process remaining dst vertices
		// 	lid_pair.second = prev_id;
		// 	end_idx = src_seqno;
		// 	idx_t peid;
		// 	for(dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
		// 		D_ASSERT(dst_lid_to_pid_map_instance.find(dst_key_column[dst_seqno]) != dst_lid_to_pid_map_instance.end());
		// 		cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key_column[dst_seqno]);
		// 		lid_pair.first = dst_key_column[dst_seqno];
		// 		D_ASSERT(lid_pair_to_epid_map_instance.find(lid_pair) != lid_pair_to_epid_map_instance.end());
		// 		peid = lid_pair_to_epid_map_instance.at(lid_pair);
		// 		// dst_key_column[dst_seqno] = cur_dst_pid;
		// 		adj_list_buffer.push_back(cur_dst_pid);
		// 		adj_list_buffer.push_back(peid);
		// 	}
		// }
		
		// // Process remaining adjlist
		// // Fill offsets
		// idx_t last_offset = adj_list_buffer.size();
		// for (size_t i = vertex_seqno; i < STORAGE_STANDARD_VECTOR_SIZE; i++)
		// 	adj_list_buffer[i] = last_offset;

		// // AddChunk for Adj.List to current Src Vertex Extent
		// DataChunk adj_list_chunk;
		// vector<LogicalType> adj_list_chunk_types = { LogicalType::BACKWARD_ADJLIST };
		// vector<data_ptr_t> adj_list_datas(1);
		// adj_list_datas[0] = (data_ptr_t) adj_list_buffer.data();
		// adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas, STORAGE_STANDARD_VECTOR_SIZE);
		// ext_mng.AppendChunkToExistingExtent(*client.get(), adj_list_chunk, *vertex_ps_cat_entry, current_vertex_eid);
		// adj_list_chunk.Destroy();

		// auto edge_file_end = std::chrono::high_resolution_clock::now();
		// std::chrono::duration<double> duration = edge_file_end - edge_file_start;

		// fprintf(stdout, "Load %s, %s Done! Elapsed: %.3f\n", edge_file.first.c_str(), edge_file.second.c_str(), duration.count());
	}

	auto bulkload_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> bulkload_duration = bulkload_end - bulkload_start;
	fprintf(stdout, "Bulk Load Elapsed: %.3f\n", bulkload_duration.count());

	// Destruct ChunkCacheManager
  	delete ChunkCacheManager::ccm;
	return 0;
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root, bool is_debug);

void exportQueryPlanVisualizer(std::vector<CypherPipelineExecutor*>& executors, std::string start_time, int query_exec_time_ms, bool is_debug) {	// default = 0, false

	// output file
	
	std::replace( start_time.begin(), start_time.end(), ' ', '_');
	boost::filesystem::create_directories("execution-log/");

	std::string filename = "execution-log/" + start_time;
	if( is_debug ) filename += "_debug";
	std::cout << "saving query visualization in : " << "build/execution-log/" << filename << ".html" << std::endl;
	std::ofstream file( filename + ".html" );

	// https://tomeko.net/online_tools/cpp_text_escape.php?lang=en
	std::string html_1 = "<script src=\"https://code.jquery.com/jquery-3.4.1.js\" integrity=\"sha256-WpOohJOqMqqyKL9FccASB9O0KwACQJpFTUBLTYOVvVU=\" crossorigin=\"anonymous\"></script>\n<script src=\"https://unpkg.com/vue@3.2.37/dist/vue.global.prod.js\"></script>\n<script src=\"https://unpkg.com/pev2/dist/pev2.umd.js\"></script>\n<link\n  href=\"https://unpkg.com/bootstrap@4.5.0/dist/css/bootstrap.min.css\"\n  rel=\"stylesheet\"\n/>\n<link rel=\"stylesheet\" href=\"https://unpkg.com/pev2/dist/style.css\" />\n\n<div id=\"app\">\n  <pev2 :plan-source=\"plan\" plan-query=\"\" />\n</div>\n\n<script>\n  const { createApp } = Vue\n  \n  const plan = `";
	std::string html_2 = "`\n\n  const app = createApp({\n    data() {\n      return {\n        plan: plan,\n      }\n    },\n  })\n  app.component(\"pev2\", pev2.Plan)\n  app.mount(\"#app\")\n$(\".plan-container\").css('height','100%')\n  </script>\n";

	json j = json::array( { json({}), } );
	if(!is_debug) {
		j[0]["Execution Time"] = query_exec_time_ms;
	}
	
	// reverse-iterate executors
	json* current_root = &(j[0]);
	bool isRootOp = true;	// is true for only one operator
	for (auto it = executors.crbegin() ; it != executors.crend(); ++it) {
  		duckdb::CypherPipeline* pipeline = (*it)->pipeline;
		// reverse operator
		for (auto it2 = pipeline->operators.crbegin() ; it2 != pipeline->operators.crend(); ++it2) {
			current_root = operatorToVisualizerJSON( current_root, *it2, isRootOp, is_debug );
			if( isRootOp ) { isRootOp = false; }
		}
		// source
		current_root = operatorToVisualizerJSON( current_root, pipeline->source, isRootOp, is_debug );
		if( isRootOp ) { isRootOp = false; }
	}

	// fix execution time
	vector<CypherPhysicalOperator*> stack;
	json& tmp_root = j[0];
	// while(true) {
		
	// }


	file << html_1;
	file << j.dump(4);
	file << html_2;

	// close file
	file.close();
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root, bool is_debug) {
	json* content;
	if( is_root ) {
		(*j)["Plan"] = json({});
		content = &((*j)["Plan"]);
	} else {
		if( (*j)["Plans"].is_null() ) {
			// single child
			(*j)["Plans"] = json::array( { json({}), } );
		} else {
			// already made child with two childs. so pass
		}
		content = &((*j)["Plans"][0]);
	}
	(*content)["Node Type"] = op->ToString();

	if(!is_debug) {
		(*content)["*Duration (exclusive)"] = op->op_timer.elapsed().wall / 1000000.0;
		(*content)["Actual Rows"] = op->processed_tuples;
		(*content)["Actual Loops"] = 1; // meaningless
	}
	// output shcma
	(*content)["Output Schema"] = op->schema.toString();

	// add child when operator is 
	if( op->ToString().compare("AdjIdxJoin") == 0 ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "AdjIdxJoinBuild";
	} else if( op->ToString().compare("NodeIdSeek") == 0  ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "NodeIdSeekBuild";
	} else if( op->ToString().compare("EdgeIdSeek") == 0  ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "EdgeIdSeekBuild";
	}

	return content;
}
