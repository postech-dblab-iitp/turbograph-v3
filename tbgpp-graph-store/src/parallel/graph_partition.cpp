#include "parallel/graph_partition.hpp"


void GraphPartitioner::InitializePartitioner(char* dir)
{
    MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

    if(process_rank == 0) role = Role::MASTER;
    else role = Role::SEGMENT;

    std::string file_path_for_this_process(dir);
    file_path_for_this_process += "/";
    file_path_for_this_process += std::to_string(process_rank); 
    output_path = file_path_for_this_process;

    buffer_count.resize(process_count, 0); //This will be used later for segments too.
    if(role == Role::MASTER) {
        size_t approximated_num_rows = reader.InitCSVFile(file_path_for_this_process.c_str(), duckdb::GraphComponentType::VERTEX, '|');

        if (!reader.GetSchemaFromHeader(key_names, types)) {
            throw duckdb::InvalidInputException("");
        }

        allocated_chunk_count.resize(process_count);
        for(int32_t i = 0; i <process_count; i++) allocated_chunk_count[i] = 0;
    }
    return;
}

int GraphPartitioner::ProcessPartitioning(DistributionPolicy policy, std::vector<std::string> hash_columns)
{
    //If segment, just skip.
    if(role == Role::SEGMENT) return 0;

    reader.ReadVertexCSVFileUsingHash(this, process_count, hash_columns);

    for(int32_t proc_idx = 1; proc_idx < process_count; proc_idx++) { //skip 0, since master itself store no data.
        for(int32_t chunk_idx = 0; chunk_idx < allocated_chunk_count[proc_idx]; chunk_idx++) {
            auto buffer_allocated_ptr_list_ptr = new std::vector<char*>;
            auto buffer_allocated_size_list_ptr = new std::vector<int64_t>;
            auto buffer_allocated_count_ptr = new int64_t;
            ext_mng.GenerateExtentFromChunkInBuffer(*datachunks[proc_idx][chunk_idx], *buffer_allocated_ptr_list_ptr, *buffer_allocated_size_list_ptr, *buffer_allocated_count_ptr);
            for(int64_t buffer_idx = 0; buffer_idx < *buffer_allocated_count_ptr; buffer_idx ++) {
                buffers[proc_idx]; //explicitly insert a mapping.
                buffers[proc_idx].insert(buffers[proc_idx].end(), buffer_allocated_ptr_list_ptr->begin(), buffer_allocated_ptr_list_ptr->end());
                buffer_sizes[proc_idx];
                buffer_sizes[proc_idx].insert(buffer_sizes[proc_idx].end(), buffer_allocated_size_list_ptr->begin(), buffer_allocated_size_list_ptr->end());
                buffer_count[proc_idx] += *buffer_allocated_count_ptr;
            }
            delete buffer_allocated_ptr_list_ptr;
            delete buffer_allocated_size_list_ptr;
            delete buffer_allocated_count_ptr;
        }
    }
    return 0;
}

duckdb::DataChunk* GraphPartitioner::AllocateNewChunk(int32_t dest_process_rank) 
{
    if(dest_process_rank == 0) return NULL; //root node stores nothing.
    duckdb::DataChunk* allocated_chunk = new duckdb::DataChunk;
    allocated_chunk->Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);
    allocated_chunk_count[dest_process_rank]++;
    datachunks[dest_process_rank].push_back(allocated_chunk);
    return allocated_chunk;
}

void GraphPartitioner::DistributePartitionedFile() {
    //1. let each segments know how many receive buffers are necessary. (how many times of buffer receive)
    //2. let each segments know the size of each receive buffers.
    //3. send buffer using MPI_Scatterv(). Buffer count for each process may be different. Therefore sendcount, recvcound may be 0.

    //For Segments

    int64_t buffer_count_seg = 0;
    std::vector<int64_t> buffer_sizes;

    MPI_Bcast(&buffer_count[0], process_count, MPI_INT64_T, 0, MPI_COMM_WORLD); //For simplicity send the full vector since size is small.
    buffer_count_seg = buffer_count[process_rank]; 
    buffer_sizes.resize(buffer_count_seg, 0);
    
    if(role == Role::MASTER) {
        //To use MPI_Scatterv(), set displacement values.

    }
    MPI_Scatterv(**); //buffer size


    for(...)
    {
        MPI_Scatterv() //send buffer. use I.
    }

    //store received buffer in disk.
}

//Correctness
    //naive: chech hash column
    //GPDB export per segment (into SCV), compare.
    //implement extent->csv.
//Performance check, does MPI provide sufficient performance? Network bandwidth?
//If not, then need to use requestrespond, TCP,..

//Catalog design. ->later.

void GraphPartitioner::ClearPartitioner()
{
    //clear allocated chunk
    for(int32_t i = 0; i < process_count; i++) {
        for(int32_t j = 0; j < allocated_chunk_count[i]; j++)
            delete datachunks[i][j];
    }

    //clear allocated send buffers (master only)
    if(role == Role::MASTER) {
        for(int32_t proc_idx = 1; proc_idx < process_count; proc_idx++) {
            for(int64_t buffer_idx = 0; buffer_idx < buffer_count[proc_idx], buffer_idx++) {
                free(buffers[proc_idx][buffer_idx]);
            }
        }
    }

    //clear receive buffers (segment only)
    if(role == Role::SEGMENT) {
        ;
    }
}
