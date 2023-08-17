#include "common/graph_simdcsv_parser.hpp"
#include "graph_partition.hpp"


void GraphPartitioner::InitializePartitioner(char* dir)
{
    MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
    std::string input_file_name(dir);
    input_file_name += "/";
    input_file_name + "in";
    input_file_name += std::to_string(process_rank); //Assume that there is in0, in1, ... in the base directory.
    
    duckdb::GraphSIMDCSVFileParser reader;
	size_t approximated_num_rows = reader.InitCSVFile(input_file_name.c_str(), duckdb::GraphComponentType::VERTEX, '|');

    if (!reader.GetSchemaFromHeader(key_names, types)) {
        throw InvalidInputException("");
    }

    allocated_chunk_count.resize(process_count);
    for(int32_t i = 0; i <process_count; i++) allocated_chunk_count[i] = 0;

    return;
}

int GraphPartitioner::ProcessPartitioning(std::vector<std::string> hash_columns)
{
    // duckdb::ReadVertexCSVFileUsingHash(this, ...);

    // GenerateExtentFromChunkInBuffer();

    // MPI_Allgather();
    // for(int32_t ...) {
    //     MPI_Alltoall();
    // }
    

    // ClearPartitioner();
    // return 0;
}

duckdb::DataChunk* GraphPartitioner::AllocateNewChunk(int32_t dest_process_rank) 
{
    duckdb::DataChunk* allocated_chunk = new duckdb::DataChunk;
    allocated_chunk->Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);
    allocated_chunk_count[dest_process_rank]++;
    datachunks[dest_process_rank].push_back(allocated_chunk);
    return allocated_chunk;
}

void GraphPartitioner::ClearPartitioner()
{
    //clear allocated chunk
    for(int32_t i = 0; i < process_count; i++) {
        for(int32_t j = 0; j < allocated_chunk_count[i]; j++)
            delete datachunks[i][j];
    }
}
