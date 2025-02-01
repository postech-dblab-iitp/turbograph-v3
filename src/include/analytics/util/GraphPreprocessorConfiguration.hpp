#pragma once

/*
 * Design of the GraphPreprocessorConfiguration
 *
 * This class is implemented to parse the options of the configuration file 
 * given as input to the GraphPreprocessor (BBP). It reads the configuration 
 * file line by line, inserting property keys and its values into the map data 
 * structure. After that, each key is searched one by one and values are put 
 * into each member variable of the GraphPreprocessArgs data structure. The
 * GraphPreprocessArgs is used in the preprocessing step.
 */

#include <fstream>
#include <string>
#include "analytics/util/ConfigurationProperties.hpp"

enum class InputFileType {
	BinaryVertexPair, PlainTextVertexPair,
	BinaryAdjList, PlainTextAdjList
};

struct GraphPreprocessArgs {
	std::string source_path;
	std::string destination_path;
	std::string temp_path;
	std::string temp_path2;
	std::string partitioned_range_path;
	std::string mapping_info_path;
	std::string subchunk_vid_range_path;
	std::string given_partition_range_path;
	int machine_num;
	int machine_count;
	int num_threads;
	int phase_from;
	int phase_to;
	int data_mode;
	InputFileType input_type;
	bool remove_mode;
	bool bidirect_mode;
	bool input_sorted;
	bool random_mode;
	bool uniqueness_test_mode;
	bool diff_test_mode;
	bool inout_test_mode;
	bool redist_validation;
	bool sort_validation;
	bool build_db;
    bool storing_inedges;
	int64_t total_vertices;
	int64_t edge_num;
	int64_t max_vid;
	int64_t degree;
	int64_t partitioning_scheme;
	int64_t mem_size;
	size_t buffer_size;
};

class GraphPreprocessConfigurationReader {
  public:
	void read_configuration(int argc, char* argv[], GraphPreprocessArgs& args) {
		if (argc == 2)
			read_config_from_file(argv[1]);
		else
			throw "No Given Configuration or Wrong arguments";

		args.total_vertices = total_vertices_;
		args.max_vid = max_vid_;
		args.num_threads = num_threads_;
		args.source_path = source_;
		args.partitioned_range_path = partitioned_range_;
		args.mapping_info_path = mapping_info_;
		args.subchunk_vid_range_path = subchunk_vid_range_;
		args.given_partition_range_path = given_partition_range_;
		args.destination_path = destination_;
		args.temp_path = temp_;
		args.temp_path2 = temp2_;
		args.input_type = input_file_type_;
		args.remove_mode = remove_mode_;
		args.bidirect_mode = bidirect_mode_;
		args.random_mode = random_mode_;
		args.edge_num = edge_num_;
		args.mem_size = mem_size_;
		args.uniqueness_test_mode = uniqueness_test_mode_;
		args.diff_test_mode = diff_test_mode_;
		args.inout_test_mode = inout_test_mode_;
		args.redist_validation = redist_validation_;
		args.sort_validation = sort_validation_;
		args.partitioning_scheme = partitioning_scheme_;
		args.build_db = build_db_;
        args.storing_inedges = storing_inedges_;
		args.phase_from = phase_from_;
		args.phase_to = phase_to_;
		if(random_mode_) {
			args.data_mode = data_mode_;
			args.degree = random_degree_;
		}
	}
private:
	void read_config_from_file(char* file_path) {
		ConfigProperties properties;
		std::ifstream fin;
		std::string tmp;
		bool success = true;
		int temp = 0;
		fin.open(file_path);
		while (!fin.eof()) {
			std::getline(fin, tmp);
			if (!tmp.empty())
				properties.AddProperty(tmp);
		}

		success = success && properties.ReadProperty(PROPERTY_KEY_SOURCE, source_);
		success = success && properties.ReadProperty(PROPERTY_KEY_PARTITIONED_RANGE_PATH, partitioned_range_);
		success = success && properties.ReadProperty(PROPERTY_KEY_MAPPING_INFO_PATH, mapping_info_);
		success = success && properties.ReadProperty(PROPERTY_KEY_SUBCHUNK_VID_RANGE_PATH, subchunk_vid_range_);
		success = success && properties.ReadProperty(PROPERTY_KEY_GIVEN_PARTITION_RANGE_PATH, given_partition_range_);
		success = success && properties.ReadProperty(PROPERTY_KEY_DESTINATION, destination_);
		success = success && properties.ReadProperty(PROPERTY_KEY_TEMP_PATH, temp_);
		success = success && properties.ReadProperty(PROPERTY_KEY_TEMP_PATH2, temp2_);
		success = success && properties.ReadPropertyInt(PROPERTY_KEY_INPUT_TYPE, temp);
		success = success && properties.ReadPropertyInt(PROPERTY_KEY_PHASE_FROM, phase_from_);
		success = success && properties.ReadPropertyInt(PROPERTY_KEY_PHASE_TO, phase_to_);
		success = success && properties.ReadPropertyInt(PROPERTY_KEY_THREADS, num_threads_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_REMOVE, remove_mode_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_RANDOM, random_mode_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_BIDIRECT, bidirect_mode_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_UNIQUENESS, uniqueness_test_mode_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_REDIST_VALIDATION, redist_validation_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_SORT_VALIDATION, sort_validation_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_DIFF, diff_test_mode_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_INOUT, inout_test_mode_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_BUILD_DB, build_db_);
		success = success && properties.ReadPropertyBool(PROPERTY_KEY_STORING_INEDGES, storing_inedges_);
		success = success && properties.ReadPropertyLong(PROPERTY_KEY_PARTITIONING_SCHEME, partitioning_scheme_);
		success = success && properties.ReadPropertyLong(PROPERTY_KEY_EDGES, edge_num_);
		success = success && properties.ReadPropertyLong(PROPERTY_KEY_MAX_VID, max_vid_);
		success = success && properties.ReadPropertyLong(PROPERTY_KEY_VERTICES, total_vertices_);
		success = success && properties.ReadPropertyLong(PROPERTY_KEY_MEM_SIZE, mem_size_);
		if (success && random_mode_) {
			success = success && properties.ReadPropertyInt(PROPERTY_KEY_DATA_MODE, data_mode_);
			success = success && properties.ReadPropertyLong(PROPERTY_KEY_DEGREE, random_degree_);
		}
		input_file_type_ = (InputFileType) temp;
		if (!success)
			throw "Bad Configuration File";
	}
	std::string source_;
	std::string destination_;
	std::string temp_;
	std::string temp2_;
	std::string partitioned_range_;
	std::string mapping_info_;
	std::string subchunk_vid_range_;
	std::string given_partition_range_;
	int num_threads_;
	int phase_from_;
	int phase_to_;
	int data_mode_;
	InputFileType input_file_type_;
	bool remove_mode_;
	bool bidirect_mode_;
	bool random_mode_;
	bool uniqueness_test_mode_;
	bool diff_test_mode_;
	bool inout_test_mode_;
	bool redist_validation_;
	bool sort_validation_;
	bool build_db_;
	bool storing_inedges_;
	int64_t total_vertices_;
	int64_t edge_num_;
	int64_t max_vid_;
	int64_t random_degree_;
	int64_t mem_size_;
	int64_t partitioning_scheme_;

	const char* PROPERTY_KEY_SOURCE = "source_path";
	const char* PROPERTY_KEY_DESTINATION= "destination_path";
	const char* PROPERTY_KEY_TEMP_PATH = "temp_path";
	const char* PROPERTY_KEY_TEMP_PATH2 = "temp_path2";
	const char* PROPERTY_KEY_PARTITIONED_RANGE_PATH = "partitioned_range_path";
	const char* PROPERTY_KEY_MAPPING_INFO_PATH = "mapping_info_path";
	const char* PROPERTY_KEY_SUBCHUNK_VID_RANGE_PATH = "subchunk_vid_range_path";
	const char* PROPERTY_KEY_GIVEN_PARTITION_RANGE_PATH = "given_partition_range_path";
	const char* PROPERTY_KEY_VERTICES = "num_vertices";
	const char* PROPERTY_KEY_REMOVE = "remove_mode";
	const char* PROPERTY_KEY_BIDIRECT = "bidirect_mode";
	const char* PROPERTY_KEY_THREADS = "num_threads";
	const char* PROPERTY_KEY_INPUT_TYPE = "input_type";
	const char* PROPERTY_KEY_MEM_SIZE = "mem_size";
	const char* PROPERTY_KEY_RANDOM = "random_mode";
	const char* PROPERTY_KEY_EDGES = "num_edges";
	const char* PROPERTY_KEY_MAX_VID = "max_vertex_id";
	const char* PROPERTY_KEY_DATA_MODE = "data_mode";
	const char* PROPERTY_KEY_DEGREE = "degree";
	const char* PROPERTY_KEY_UNIQUENESS = "uniqueness_test_mode";
	const char* PROPERTY_KEY_DIFF = "diff_test_mode";
	const char* PROPERTY_KEY_INOUT = "inout_test_mode";
	const char* PROPERTY_KEY_REDIST_VALIDATION = "redist_validation";
	const char* PROPERTY_KEY_SORT_VALIDATION= "sort_validation";
	const char* PROPERTY_KEY_PARTITIONING_SCHEME= "partitioning_scheme";
	const char* PROPERTY_KEY_BUILD_DB = "build_db";
	const char* PROPERTY_KEY_STORING_INEDGES = "storing_inedges";
	const char* PROPERTY_KEY_PHASE_FROM= "phase_from";
	const char* PROPERTY_KEY_PHASE_TO= "phase_to";
};
