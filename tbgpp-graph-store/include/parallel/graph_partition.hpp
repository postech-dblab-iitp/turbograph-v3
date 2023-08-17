#pragma once
#include "common/common.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include <mpi.h>
#include <vector>
#include "extent/extent_manager.hpp"


enum class DistributionPolicy {
    DIST_RANDOM = 0,
    DIST_HASH = 1
};

class GraphPartitioner {
    public:
    void InitializePartitioner(char* dir);
    int32_t ProcessPartitioning(DistributionPolicy policy, std::vector<std::string> keys);
    duckdb::DataChunk* AllocateNewChunk(int32_t dest_process_rank);

    //Temporarily use very simple hash functions.
    template <typename T>
    int32_t PartitionHash(T value) {
        static_assert(std::is_integral<T>::value, "Hash function for integers");
        return (int32_t)(value%process_count);
    }

    void ClearPartitioner();

    private:
    int32_t process_count;
    int32_t process_rank;
    std::vector<std::string> key_names;
    std::vector<duckdb::LogicalType> types; //This is better to be here since this is used for chunk allocation.
    std::vector<int32_t> allocated_chunk_count;
    std::unordered_map<int32_t, std::vector<duckdb::DataChunk *>> datachunks;
    duckdb::ExtentManager ext_mng;
};
