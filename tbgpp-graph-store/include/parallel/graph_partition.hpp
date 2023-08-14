#include "common/common.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include <mpi.h>


enum class DistributionPolicy {
    DIST_RANDOM = 0,
    DIST_HASH = 1
};


template <class HashFuncType>
int partitioning(char* dir, DistributionPolicy policy, std::vector<std::string> keys, HashFuncType hash_function)
{
    // int process_count, process_rank;
    // MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    // MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

    // std::string input_file_name(dir);
    // input_file_name += "/";
    // input_file_name + "in";
    // input_file_name += std::to_string(process_rank); //Assume that there is in0, in1, ... in the base directory.

    // duckdb::GraphSIMDCSVFileParser reader;
	// size_t approximated_num_rows = reader.InitCSVFile(input_file_name.c_str(), duckdb::GraphComponentType::VERTEX, '|');
    
    // vector<std::string> key_names;
    // vector<duckdb::LogicalType> types;
    // if (!reader.GetSchemaFromHeader(key_names, types)) {
    //     throw InvalidInputException("");
    // }

    

    return 0;
}