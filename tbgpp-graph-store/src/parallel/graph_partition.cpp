#include "parallel/graph_partition.hpp"

using namespace duckdb;

duckdb::ProcessID GraphPartitioner::process_count = 0;
std::vector<tbb::concurrent_queue<ExtentWithMetaData>> GraphPartitioner::per_segment_buffer_queue;
std::atomic<int> GraphPartitioner::finished_segment_count;

void GraphPartitioner::SendExtentFromQueue() {
    return;
}

void GraphPartitioner::InitializePartitioner(std::pair<std::string, std::string> fileinfo)
{
    MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

    if(process_rank == 0) role = Role::MASTER;
    else role = Role::SEGMENT;


    // buffer_count.resize(process_count, 0); //This will be used later for segments too.
    if(role == Role::MASTER) {
        spawn_sender_thread_cnt = 1;//TODO change this if necessary.

        // std::string filename(name);
        // file_path_for_this_process += "/" + filename;
        size_t approximated_num_rows = reader.InitCSVFile(fileinfo.first.c_str(), duckdb::GraphComponentType::VERTEX, '|');

        if (!reader.GetSchemaFromHeader(key_names, types)) {
            throw duckdb::InvalidInputException("");
        }
        finished_segment_count = 0;
        allocated_chunk_count.resize(process_count);
        for(duckdb::ProcessID i = 0; i <process_count; i++) allocated_chunk_count[i] = 0;
        for(duckdb::ProcessID i = 0; i <process_count; i++) {
            per_segment_buffer_queue.push_back(tbb::concurrent_queue<ExtentWithMetaData>());
        }
        D_ASSERT(per_segment_buffer_queue.size() == process_count);
        
        for(int32_t i = 0; i < spawn_sender_thread_cnt; i++) {
            send_request_to_wait.push(Aio_Helper::async_pool.enqueue(GraphPartitioner::SendExtentFromQueue));
        }
    }
    return;
}

void GraphPartitioner::ParseLabelSet(string &labelset, vector<string> &parsed_labelset) {
	std::istringstream iss(labelset);
	std::string label;

	while (std::getline(iss, label, ':')) {
		parsed_labelset.push_back(label);
	}
}


int GraphPartitioner::ProcessPartitioning(DistributionPolicy policy, std::vector<std::string> hash_columns, Catalog &cat_instance, ExtentManager &ext_mng, std::shared_ptr<ClientContext> client, GraphCatalogEntry *graph_cat, std::pair<std::string, std::string> fileinfo)
{
    //If segment, just skip.
    if(role == Role::SEGMENT) return 0;

    //csv file -> DataChunk
    std::vector<std::string> vertex_labels;
	ParseLabelSet(fileinfo.first, vertex_labels);

    string partition_name = "vpart_" + fileinfo.first;
    CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
    PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
    PartitionID new_pid = graph_cat->GetNewPartitionID();
    graph_cat->AddVertexPartition(*client.get(), new_pid, partition_cat->GetOid(), vertex_labels);

    std::string property_schema_name = "vps_" + fileinfo.first;
    fprintf(stdout, "prop_schema_name = %s\n", property_schema_name.c_str());
    CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, partition_cat->GetOid());
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);
    
    vector<PropertyKeyID> property_key_ids;
    graph_cat->GetPropertyKeyIDs(*client.get(), key_names, property_key_ids);
    partition_cat->AddPropertySchema(*client.get(), property_schema_cat->GetOid(), property_key_ids);
    property_schema_cat->SetTypes(types);
    property_schema_cat->SetKeys(*client.get(), key_names);

    // TODO need to merge below two functions into one function call
    partition_cat->SetKeys(*client.get(), key_names);
    partition_cat->SetTypes(types);

    // Create Physical ID Index Catalog & Add to PartitionCatalogEntry
    CreateIndexInfo idx_info(DEFAULT_SCHEMA, fileinfo.first + "_id", IndexType::PHYSICAL_ID, partition_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
    IndexCatalogEntry *index_cat = (IndexCatalogEntry *)cat_instance.CreateIndex(*client.get(), &idx_info);
    partition_cat->SetPhysicalIDIndex(index_cat->GetOid());


    reader.ReadVertexCSVFileUsingHash(this, process_count, hash_columns);

    for(duckdb::ProcessID proc_idx = 1; proc_idx < process_count; proc_idx++) { //skip 0, since master itself store no data.
        for(int32_t chunk_idx = 0; chunk_idx < allocated_chunk_count[proc_idx]; chunk_idx++) {
            // auto buffer_allocated_ptr_list_ptr = new std::vector<char*>;
            // buffer_allocated_ptr_list_ptr->resize(0, NULL);
            // auto buffer_allocated_size_list_ptr = new std::vector<int64_t>;
            // buffer_allocated_size_list_ptr->resize(0, 0);
            // auto buffer_allocated_count_ptr = new int32_t;
            // *buffer_allocated_count_ptr = 0;
            // ext_mng.GenerateExtentFromChunkInBuffer(*datachunks[proc_idx][chunk_idx], *buffer_allocated_ptr_list_ptr, *buffer_allocated_size_list_ptr, *buffer_allocated_count_ptr);
            // // for(int64_t buffer_idx = 0; buffer_idx < *buffer_allocated_count_ptr; buffer_idx ++) {
            // buffers[proc_idx]; //explicitly insert a mapping.
            // buffers[proc_idx].insert(buffers[proc_idx].end(), buffer_allocated_ptr_list_ptr->begin(), buffer_allocated_ptr_list_ptr->end());
            // buffer_sizes[proc_idx];
            // buffer_sizes[proc_idx].insert(buffer_sizes[proc_idx].end(), buffer_allocated_size_list_ptr->begin(), buffer_allocated_size_list_ptr->end());
            // // }
            // buffer_count[proc_idx] += *buffer_allocated_count_ptr;
            // delete buffer_allocated_ptr_list_ptr;
            // delete buffer_allocated_size_list_ptr;
            // delete buffer_allocated_count_ptr;
        }
        // printf("For process %d: allocated %d buffers\n", proc_idx, buffer_count[proc_idx]);
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

// void GraphPartitioner::DistributePartitionedFile() {
//     //1. let each segments know how many receive buffers are necessary. (how many times of buffer receive)
//     //2. let each segments know the size of each receive buffers.
//     //3. send buffer using MPI_Scatterv(). Buffer count for each process may be different. Therefore sendcount, recvcound may be 0.

//     const bool debugging = true;

//     //For Segments
//     int32_t buffer_count_max = 0; //Repeat MPI_Scatterv as much as this value.
//     std::vector<int64_t> buffer_sizes_seg;

//     //For this no need to distinguish master and segment.
//     MPI_Bcast(&buffer_count[0], process_count, MPI_INT32_T, 0, MPI_COMM_WORLD); //For simplicity send the full vector since size is small.

//     buffer_count_max = *std::max_element(buffer_count.begin(), buffer_count.end());
//     if(role == Role::SEGMENT) {
//         buffer_count_seg = buffer_count[process_rank]; 
//         buffer_sizes_seg.resize(buffer_count_max, 0);
//     }
    
//     //Following variables are used to send separated (not contiguous) data using MPI_Scatterv().
//     std::vector<int32_t> displs(process_count, 0);

    

//     if(role == Role::MASTER) {
//         //To use MPI_Scatterv(), set displs.
//         buffer_sizes[0];
//         buffer_sizes[0].resize(1, 0);//Dummy.

//         for(duckdb::ProcessID i = 1; i<process_count; i++) {
//             displs[i] = ((char*)&(buffer_sizes[i][0]) - (char*)&(buffer_sizes[0][0])) / sizeof(int64_t);
            
//             if(debugging)
//                 printf("ptr1 = %p, ptr2 = %p, diff is %ld (INT_MAX = %d)\n", &buffer_sizes[i][0], &buffer_sizes[0][0], &buffer_sizes[i][0] - &buffer_sizes[0][0], __INT32_MAX__);
            
//             // if ((int64_t)((char*)&(buffer_sizes[i][0]) - (char*)&(buffer_sizes[0][0])) / sizeof(int64_t) > std::numeric_limits<int32_t>::max()
//             // || (int64_t)((char*)&(buffer_sizes[i][0]) - (char*)&(buffer_sizes[0][0])) / sizeof(int64_t) <  std::numeric_limits<int32_t>::min()) {
//             //     printf("value1 = %ld\n", ((char*)&(buffer_sizes[i][0]) - (char*)&(buffer_sizes[0][0])) / sizeof(int64_t));
//             //     printf("Error: buffer pointer gap is too large to be sent using MPI_Scatterv(). Need to change buffer displs policy.\n");
//             //     D_ASSERT(false);
//             // }
//         }

//         if(debugging) {
//             for(duckdb::ProcessID i = 1; i<process_count; i++) {
//                 D_ASSERT((displs[i] + (int64_t*)&buffer_sizes[0][0])== (int64_t*)&buffer_sizes[i][0]);
//             }
//         }
//     }

//     if(role == Role::MASTER)
//         MPI_Scatterv((int64_t*)&buffer_sizes[0][0], (int*)&buffer_count[0], (int*)&displs[0], MPI_INT64_T, 
//     NULL, 0, MPI_INT64_T, 0, MPI_COMM_WORLD); //send buffer size
//     else 
//         MPI_Scatterv(NULL, NULL, NULL, MPI_INT64_T, 
//     (int64_t*)&buffer_sizes_seg[0], buffer_count_seg, MPI_INT64_T, 0, MPI_COMM_WORLD);

//     if(role == Role::SEGMENT) {
//         recv_buffers.resize(buffer_count_max, NULL);
//         for(int32_t buf_idx = 0; buf_idx < buffer_count_seg; buf_idx++) {
//             recv_buffers[buf_idx] = (char*)malloc(buffer_sizes_seg[buf_idx]);
//         }
//     }

//     buffers[0].resize(buffer_count_max, NULL);
//     buffer_count[0] = buffer_count_max;
//     for(int32_t buf_idx = 0; buf_idx < buffer_count_max; buf_idx++) {
//         buffers[0][buf_idx] = (char*) malloc(1); //Dummy. This is necessary for master to let dipls work.
//     }

//     for(int32_t buf_idx = 0; buf_idx < buffer_count_max; buf_idx ++) {
//         if(role == Role::MASTER) {
//             std::vector<int32_t> buffer_sizes_this_run(process_count, 0);
//             for(duckdb::ProcessID proc_idx = 1; proc_idx<process_count; proc_idx++) {
//                 if(buffer_count[proc_idx] <= buf_idx) {
//                     displs[proc_idx] = 0;
//                     buffer_sizes_this_run[proc_idx] = 0;
//                     continue;
//                 }
//                 if(buffer_sizes[proc_idx][buf_idx] > __INT32_MAX__) D_ASSERT(false);
//                 buffer_sizes_this_run[proc_idx] = (int32_t)buffer_sizes[proc_idx][buf_idx];
//                 displs[proc_idx] = buffers[proc_idx][buf_idx] - buffers[0][buf_idx];
//             }

//             if(debugging)   //For debugging
//             {
//                 D_ASSERT(buffer_sizes_this_run[0] == 0);
//                 for(duckdb::ProcessID proc_idx = 1; proc_idx<process_count; proc_idx++) {
//                     D_ASSERT(displs[proc_idx] + buffers[0][buf_idx]== buffers[proc_idx][buf_idx]);
//                 } //check if displs values are correct.
//             }
            
//             MPI_Scatterv((char*) buffers[0][buf_idx], (int32_t*) &buffer_sizes_this_run[0], (int*)&displs[0], MPI_CHAR, 
//             NULL, 0, MPI_CHAR, 0, MPI_COMM_WORLD);
//         }
//         else 
//             MPI_Scatterv(NULL, NULL, NULL, MPI_CHAR, 
//             recv_buffers[buf_idx], buffer_sizes_seg[buf_idx], MPI_CHAR, 0, MPI_COMM_WORLD);
//     }


//     //store received buffer in disk.
//     if(role == Role::SEGMENT) {
//         for(int32_t buf_idx = 0; buf_idx < buffer_count_seg; buf_idx++) {
//             std::string filename = output_path + "/" + std::to_string(buf_idx);

//             std::ofstream file(filename, std::ios::binary);
//             if(file.is_open()) {
//                 file.write(recv_buffers[buf_idx], buffer_sizes_seg[buf_idx]);
//                 file.close();
//             }
//             else {
//                 printf("Segment final file open error!\n");
//             }
//         }
//     }
// }

//Correctness
    //naive: chech hash column
    //GPDB export per segment (into SCV), compare.
    //implement extent->csv.
//Performance check, does MPI provide sufficient performance? Network bandwidth?
//If not, then need to use requestrespond, TCP,..

//Catalog design. ->later.

void GraphPartitioner::ClearPartitioner()
{

    //clear allocated send buffers (master only)
    if(role == Role::MASTER) {
        //clear allocated chunk
        for(duckdb::ProcessID i = 0; i < process_count; i++) {
            for(int32_t j = 0; j < allocated_chunk_count[i]; j++)
                delete datachunks[i][j];
        }
        // for(duckdb::ProcessID proc_idx = 0; proc_idx < process_count; proc_idx++) {
        //     for(int64_t buffer_idx = 0; buffer_idx < buffer_count[proc_idx]; buffer_idx++) {
        //         free(buffers[proc_idx][buffer_idx]);
        //         buffers[proc_idx][buffer_idx] = NULL;
        //     }
        // }
        while(!send_request_to_wait.empty()) {
            send_request_to_wait.front().get();
            send_request_to_wait.pop();
        }
        D_ASSERT(finished_segment_count.load() == process_count-1);
    }

    // //clear receive buffers (segment only)
    // if(role == Role::SEGMENT) {
    //     for(int32_t buf_idx = 0; buf_idx < buffer_count_seg; buf_idx++) {
    //         free(recv_buffers[buf_idx]);
    //     }
    // }
    //TODO: implement new clear.
}
