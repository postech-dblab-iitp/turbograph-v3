#include "parallel/graph_partition.hpp"
#include <iostream>

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    std::vector<std::string> keynames;
    std::function<bool(int)> hash_function;               //temporary type
    partitioning(argv[1], DistributionPolicy::DIST_HASH, keynames, hash_function);

    MPI_Finalize();
    return 0;
}