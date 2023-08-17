#include "parallel/graph_partition.hpp"
#include <iostream>
#include <mpi.h>

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    GraphPartitioner graphpartitioner;
    graphpartitioner.InitializePartitioner(argv[1]);
    DistributionPolicy policy = DistributionPolicy::DIST_HASH;

    std::vector<std::string> keynames;
    graphpartitioner.ProcessPartitioning(policy, keynames);

    MPI_Finalize();
    return 0;
}