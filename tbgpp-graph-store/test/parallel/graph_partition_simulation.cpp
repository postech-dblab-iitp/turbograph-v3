#include "parallel/graph_partition.hpp"
#include <iostream>
#include <mpi.h>

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    GraphPartitioner graphpartitioner;
    graphpartitioner.InitializePartitioner(argv[1], argv[2]);//path, filename

    if(graphpartitioner.process_rank == 0) {
        printf("Initialized partitioner.\n");
    }
    DistributionPolicy policy = DistributionPolicy::DIST_HASH; //TODO: implement random dist.

    std::vector<std::string> keynames; //TODO: currently creating keynames manually. Need to change this to be given as argument.
    keynames.push_back("id");

    graphpartitioner.ProcessPartitioning(policy, keynames);
    if(graphpartitioner.process_rank == 0) {
        printf("Process partitioning done.\n");
    }

    graphpartitioner.DistributePartitionedFile();
    if(graphpartitioner.process_rank == 0) {
        printf("Distributing process done..\n");
    }

    graphpartitioner.ClearPartitioner();
    if(graphpartitioner.process_rank == 0) {
        printf("Partitioner cleaning done.\n");
    }
    MPI_Finalize();
    return 0;
}