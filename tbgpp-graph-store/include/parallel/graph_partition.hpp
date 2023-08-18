#pragma once
#include "common/common.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include <mpi.h>
#include <vector>
#include "extent/extent_manager.hpp"
#include "common/graph_simdcsv_parser.hpp"


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

class GraphPartitioner {
    //Intended partitioning sequence: MPI_Init() -> InitializePartitioner() -> ProcessPartitioning() 
    //-> DistributePartitionedFile() -> ClearPartitioner() -> MPI_Finalize()

    public:
    void InitializePartitioner(char* dir);
    int32_t ProcessPartitioning(DistributionPolicy policy, std::vector<std::string> keys);
    void DistributePartitionedFile();
    void ClearPartitioner();

    duckdb::DataChunk* AllocateNewChunk(int32_t dest_process_rank);

    //Temporarily use very simple hash functions. Maybe need to change this if necessary.
    template <typename T>
    int32_t PartitionHash(T value) {
        static_assert(std::is_integral<T>::value, "Hash function for integers");
        return (int32_t)(value%process_count-1) + 1; //1, 2, ..., process_count-1. Master node stores no grpah.
    }

    private:
    Role role;
    int32_t process_count;
    int32_t process_rank;
    std::string output_path; //Temporarily store output to "dir/process_rank" to test in single machine. In distributed system, this should be changed.
    std::vector<int64_t> buffer_count;              //use: int64_t buffer_count = buffer_count[proc_rank]

    //Followings are only for master!
    std::vector<std::string> key_names;
    std::vector<duckdb::LogicalType> types; //This is better to be here since this is used for chunk allocation.
    std::vector<int32_t> allocated_chunk_count;
    std::unordered_map<int32_t, std::vector<duckdb::DataChunk *>> datachunks; //chunkcollection 라는 자료구조가 있으므로 바꿔도 됨.
    duckdb::ExtentManager ext_mng;
    duckdb::GraphSIMDCSVFileParser reader;

    std::unordered_map<int32_t, std::vector<char*>> buffers;        //use: char* buffer = buffers[proc_rank][buffer_idx]
    std::unordered_map<int32_t, std::vector<int64_t>> buffer_sizes; //use: int64_t buffer_size = buffer_sizes[proc_rank][buffer_idx]
};
