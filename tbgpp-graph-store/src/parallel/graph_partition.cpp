#include "parallel/graph_partition.hpp"

using namespace duckdb;

duckdb::ProcessID GraphPartitioner::process_rank;
std::string GraphPartitioner::output_path; //Temporarily store output to "dir/process_rank" to test in single machine. In distributed system, this should be changed.
duckdb::ProcessID GraphPartitioner::process_count;
Role GraphPartitioner::role;

//Followings are set at InputParser::getCmdOption()
std::vector<std::pair<string, string>> GraphPartitioner::vertex_files;
std::vector<std::pair<string, string>> GraphPartitioner::edge_files;
std::vector<std::pair<string, string>> GraphPartitioner::edge_files_backward;
std::string GraphPartitioner::output_dir;
bool GraphPartitioner::load_edge;
bool GraphPartitioner::load_backward_edge;

std::vector<FileMetaInfo> GraphPartitioner::file_meta_infos;   //indexed by file_seq_number. Used by GenerateExtentFromChunkQueue, ...

//Followings are set at InitializePartitioner()
std::shared_ptr<ClientContext> GraphPartitioner::client;
Catalog *GraphPartitioner::cat_instance;
duckdb::ExtentManager *GraphPartitioner::ext_mng;
GraphCatalogEntry *GraphPartitioner::graph_cat;

turbo_tcp GraphPartitioner::server_sockets;
turbo_tcp GraphPartitioner::client_sockets;

//Followings are only for master!
std::vector<tbb::concurrent_queue<std::pair<std::pair<int32_t, int32_t>, duckdb::DataChunk *>>> GraphPartitioner::per_generator_datachunk_queue; //here int32_t is file seq number.
std::vector<tbb::concurrent_queue<::SimpleContainer>> GraphPartitioner::per_sender_buffer_queue; //use: extent_queues[proc_rank].push(extent_with_meta_data)
std::queue<std::future<void>> GraphPartitioner::extent_generator_futures;
std::queue<std::future<void>> GraphPartitioner::extent_sender_futures;
std::atomic<bool> GraphPartitioner::file_reading_finished;
std::atomic<bool> GraphPartitioner::extent_generating_finished; //TODO set and use this in main / sender thread
std::vector<std::pair<std::string, std::unordered_map<idx_t, idx_t>>> GraphPartitioner::lid_to_pid_map;
std::vector<atom> GraphPartitioner::lid_to_pid_map_locks;
std::vector<std::pair<std::string, std::unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> GraphPartitioner::lid_pair_to_epid_map; // For Backward AdjList. No need to consider while processing vertices
std::vector<atom> GraphPartitioner::lid_pair_to_epid_map_locks;
// static std::vector<std::pair<std::string, ART*>> lid_to_pid_index; // Not being used now?
std::atomic<int> GraphPartitioner::local_file_seq_number = 0;


void GraphPartitioner::SendExtentFromQueue(int32_t my_sender_id) { //This function could be executed by many threads paralelly.
    int64_t send_size; //Size of extent + metainfo
    int32_t dest_process_id;
    tbb::concurrent_queue<::SimpleContainer> my_queue = per_sender_buffer_queue[my_sender_id];

    while(!extent_generating_finished.load()) {
        ::SimpleContainer container;
        if(my_queue.try_pop(container)) { 
            send_size = ((ExtentWithMetaData*)container.data)->size_extent + offsetof(ExtentWithMetaData, data[0]);
            if(send_size == 0) continue; //This may happen if file reading is finished just after one chunk for a segment is finished and allocated new chunk.
            dest_process_id = ((ExtentWithMetaData*)container.data)->dest_process_id;
            server_sockets.send_to_client((char*) container.data, 0, send_size, dest_process_id);
            PartitionedExtentReadRequest request;
            request.from = PartitionStatistics::my_machine_id();
            request.rt = PartitionedExtentRead;
            request.to = dest_process_id;
            request.size = send_size;
            RequestRespond::SendRequest(dest_process_id, request);  //Send signal.
            RequestRespond::ReturnTempDataBuffer(container);    
            // sending_to_process[proc_idx].store(false);
        }
        else if (extent_generating_finished.load()) {
            break;
        }
        else std::this_thread::yield();
    }
    return;
}

void GraphPartitioner::SpawnGeneratorAndSenderThreads() {
    for(int32_t i = 0; i <EXTENT_GENERATOR_THREAD_COUNT; i++)
        extent_generator_futures.push(Aio_Helper::async_pool.enqueue(GraphPartitioner::GenerateExtentFromChunkQueue, i));
    for(int32_t i = 0; i <SENDER_THREAD_COUNT; i++)
        extent_sender_futures.push(Aio_Helper::async_pool.enqueue(GraphPartitioner::SendExtentFromQueue, i));
}

void GraphPartitioner::WaitForGeneratorThreads() {
    D_ASSERT(file_reading_finished.load());
    for(int32_t i = 0; i <EXTENT_GENERATOR_THREAD_COUNT; i++) {
        extent_generator_futures.front().get();
        extent_generator_futures.pop();
    }
    extent_generating_finished.store(true);
}

void GraphPartitioner::WaitForSenderThreads() {
    D_ASSERT(extent_generating_finished.load());
    for(int32_t i = 0; i <SENDER_THREAD_COUNT; i++) {
        extent_sender_futures.front().get();
        extent_sender_futures.pop();
    }
}

void GraphPartitioner::ReceiveAndStoreExtent() {
    //design: while not finished, receive extent from tcp, and allocate buffer, and store. And swizzle.
    //If a dummy extent is received, then finish.
    D_ASSERT(role == Role::SEGMENT);
    static int called_count = 0;
    called_count++;
    printf("ReceiveAndStoreExtent %dth segment, %dth call\n", GraphPartitioner::process_rank, called_count);
    return;    
}

void GraphPartitioner::GenerateExtentFromChunkQueue(int32_t my_generator_id) {
    std::unordered_map <idx_t, idx_t> local_lid_to_pid_map_instance;
    std::pair<std::pair<int32_t, int32_t>, duckdb::DataChunk*> datachunk_with_file_seq_number;
    while(1) {
        if(per_generator_datachunk_queue[my_generator_id].try_pop(datachunk_with_file_seq_number)) { 
        int file_seq_number = datachunk_with_file_seq_number.first.first;
            ExtentID new_eid = ext_mng->GenerateExtentFromChunkToSend(*(datachunk_with_file_seq_number.second), datachunk_with_file_seq_number.first.first, datachunk_with_file_seq_number.first.second);
            file_meta_infos[file_seq_number].property_schema_cat->AddExtent(new_eid, datachunk_with_file_seq_number.second->size());
            if(load_edge && file_meta_infos[file_seq_number].file_type == GraphFileType::Vertex) { //TODO: check file type and add only if vertex file. 
				idx_t pid_base = (idx_t) new_eid;
				pid_base = pid_base << 32;
                idx_t dest_process_id = static_cast<idx_t>(datachunk_with_file_seq_number.first.second);
                dest_process_id = dest_process_id << 20; //Use 12bit of pid for dest_process_id.
                pid_base += dest_process_id;
				if (file_meta_infos[file_seq_number].key_column_idxs[0] < 0) continue;
				idx_t* key_column = (idx_t*) datachunk_with_file_seq_number.second->data[file_meta_infos[file_seq_number].key_column_idxs[0]].GetData();
				for (idx_t seqno = 0; seqno < datachunk_with_file_seq_number.second->size(); seqno++) {
					local_lid_to_pid_map_instance.emplace(key_column[seqno], pid_base + seqno);
				}

                //Merge local lid to pid mapping instance to a global one. Hold lock for thread safeness.
                lid_to_pid_map_locks[file_seq_number].lock();
                std::unordered_map<idx_t, idx_t> *global_lid_to_pid_map_instance = &lid_to_pid_map[file_seq_number].second; 
                global_lid_to_pid_map_instance->merge(local_lid_to_pid_map_instance);
                lid_to_pid_map_locks[file_seq_number].unlock();
            }
            delete datachunk_with_file_seq_number.second;
        }
        else if(file_reading_finished.load()) {
            if(per_generator_datachunk_queue[my_generator_id].empty()) { 
                break;
            }
        }
    }
    // for(duckdb::ProcessID i = my_generator_id; i < process_count; i += EXTENT_GENERATOR_THREAD_COUNT) {
    //     ::SimpleContainer container = RequestRespond::GetTempDataBuffer(offsetof(::ExtentWithMetaData, data[0]));
    //     ((::ExtentWithMetaData*)container.data)->size_extent = 0;
    //     ((::ExtentWithMetaData*)container.data)->dest_process_id = i;
    //     per_sender_buffer_queue[i%SENDER_THREAD_COUNT].push(container);
    // }
}



void GraphPartitioner::InitializePartitioner(std::shared_ptr<ClientContext> client, Catalog* cat_instance, ExtentManager* ext_mng, GraphCatalogEntry* graph_cat)
{
    MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

    Aio_Helper::Initialize(DEFAULT_AIO_THREAD); 
    std::thread* rr_receiver_ = new std::thread(RequestRespond::ReceiveRequest);
    PartitionStatistics::init();
    turbo_tcp::init_host();
    RequestRespond::Initialize(4 * 1024 * 1024L, 128, 2 * 1024 * 1024 * 1024L, DEFAULT_NIO_BUFFER_SIZE, DEFAULT_NIO_THREADS, 0);
    RequestRespond::SetMaxDynamicBufferSize(1 * (1024 * 1024 * 1024L));

    if(AmIMaster(process_rank)) role = Role::MASTER;
    else role = Role::SEGMENT;
    turbo_tcp::establish_all_connections(&server_sockets, &client_sockets);

    printf("Turbo tcp connection established\n");

    GraphPartitioner::client = client;
    GraphPartitioner::cat_instance = cat_instance;
    GraphPartitioner::ext_mng = ext_mng;
    GraphPartitioner::graph_cat = graph_cat;

    if(role == Role::MASTER) { //Initialize data structures for master.
        for(int32_t i = 0; i <EXTENT_GENERATOR_THREAD_COUNT; i++) {
            per_generator_datachunk_queue.push_back(tbb::concurrent_queue<std::pair<std::pair<int32_t, int32_t>, duckdb::DataChunk *>>());
        }

        for(int32_t i = 0; i <SENDER_THREAD_COUNT; i++) {
            per_sender_buffer_queue.push_back(tbb::concurrent_queue<::SimpleContainer>());
        }
        D_ASSERT(per_sender_buffer_queue.size() == SENDER_THREAD_COUNT);

        file_reading_finished.store(false);
        extent_generating_finished.store(false);
        file_meta_infos.resize(vertex_files.size() + edge_files.size() + edge_files_backward.size());
    }
    return;
}

void GraphPartitioner::ParseLabelSet(string &labelset, vector<string> &parsed_labelset) {
	std::istringstream iss(labelset);
	string label;

	while (std::getline(iss, label, ':')) {
		parsed_labelset.push_back(label);
	}
}

void GraphPartitioner::ReadVertexFileAndCreateDataChunk(DistributionPolicy policy, std::vector<std::string> hash_columns, std::pair<std::string, std::string> fileinfo)
{
    //Algorithm
    //Get local file seq number. (Consider concurrent Vertex file read in the future..)
    //Generate lid_to_pid_map instance and insert it into both lid_to_pid_map. Set FileMetaInfo.
    //Do catalog-repated work.
    //Initialize CSV reader.
    //Read CSV file and generate datachunk.

    D_ASSERT(role == Role::MASTER);
    int file_seq_number = local_file_seq_number.fetch_add(1); //After this local_file_seq_number is increased by 1.
    D_ASSERT(file_meta_infos[file_seq_number].file_seq_number == -1); //This file_seq_number should be empty. Otherwise error.

    std::vector<std::string> vertex_labels;
	ParseLabelSet(fileinfo.first, vertex_labels);

    string partition_name = "vpart_" + fileinfo.first;
    CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
    PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance->CreatePartition(*client.get(), &partition_info);
    PartitionID new_pid = graph_cat->GetNewPartitionID();
    graph_cat->AddVertexPartition(*client.get(), new_pid, partition_cat->GetOid(), vertex_labels);

    fprintf(stdout, "Init GraphCSVFile\n");
    auto init_csv_start = std::chrono::high_resolution_clock::now();
    // Initialize CSVFileReader
    GraphSIMDCSVFileParser reader;
    size_t approximated_num_rows = reader.InitCSVFile(fileinfo.second.c_str(), GraphComponentType::VERTEX, '|');
    auto init_csv_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> init_csv_duration = init_csv_end - init_csv_start;
    fprintf(stdout, "InitCSV Elapsed: %.3f\n", init_csv_duration.count());

    std::vector<std::string> key_names;
    std::vector<duckdb::LogicalType> types;

    if (!reader.GetSchemaFromHeader(key_names, types)) {
        throw InvalidInputException("");
    }

	vector<int64_t> key_column_idxs = reader.GetKeyColumnIndexFromHeader();

    std::string property_schema_name = "vps_" + fileinfo.first;
    fprintf(stdout, "prop_schema_name = %s\n", property_schema_name.c_str());
    CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, partition_cat->GetOid());
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance->CreatePropertySchema(*client.get(), &propertyschema_info);
    
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
    IndexCatalogEntry *index_cat = (IndexCatalogEntry *)cat_instance->CreateIndex(*client.get(), &idx_info);
    partition_cat->SetPhysicalIDIndex(index_cat->GetOid());

    std::unordered_map<idx_t, idx_t> *lid_to_pid_map_instance = nullptr;
    ART *index;
    if (GraphPartitioner::load_edge) {
        lid_to_pid_map_locks.emplace_back(atom());
        lid_to_pid_map.emplace_back(fileinfo.first, unordered_map<idx_t, idx_t>());
        lid_to_pid_map_instance = &lid_to_pid_map.back().second;
        lid_to_pid_map_instance->reserve(approximated_num_rows);
    }

    //Set FileMetaInfo
    FileMetaInfo &file_meta_info = file_meta_infos[file_seq_number]; //This is already resized.
    file_meta_info.file_seq_number = file_seq_number;
    file_meta_info.file_type = GraphFileType::Vertex;
    file_meta_info.key_names = key_names;
    file_meta_info.fileinfo = fileinfo;
    file_meta_info.types = types;
    file_meta_info.vertex_labels = vertex_labels;
    file_meta_info.property_schema_cat = property_schema_cat;
    file_meta_info.hash_columns = hash_columns;
    file_meta_info.key_column_idxs = key_column_idxs;

    reader.ReadVertexCSVFileUsingHash(file_seq_number);
    return;
}

duckdb::DataChunk* GraphPartitioner::AllocateNewChunk(int32_t file_seq_number)  //No need to be parallel for this.
{
    duckdb::DataChunk* allocated_chunk = new duckdb::DataChunk;
    allocated_chunk->Initialize(file_meta_infos[file_seq_number].types, STORAGE_STANDARD_VECTOR_SIZE);
    // datachunks[dest_process_rank].push(allocated_chunk); //This chunk will be handled by csvreader. 
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

bool GraphPartitioner::AmIMaster(int32_t process_rank){
    return process_rank == 0; //May be changed.
}

void GraphPartitioner::ClearPartitioner()
{

    //clear allocated send buffers (master only)
    if(role == Role::MASTER) {
        // //clear allocated chunk
        // for(duckdb::ProcessID i = 0; i < process_count; i++) {
        //     for(int32_t j = 0; j < allocated_chunk_count[i]; j++)
        //         ;// delete datachunks[i][j]; TODO: do this.
        // }
        // for(duckdb::ProcessID proc_idx = 0; proc_idx < process_count; proc_idx++) {
        //     for(int64_t buffer_idx = 0; buffer_idx < buffer_count[proc_idx]; buffer_idx++) {
        //         free(buffers[proc_idx][buffer_idx]);
        //         buffers[proc_idx][buffer_idx] = NULL;
        //     }
        // }
        // while(!send_request_to_wait.empty()) {
        //     send_request_to_wait.front().get();
        //     send_request_to_wait.pop();
        // }
        // D_ASSERT(finished_segment_count.load() == process_count-1);
    }

    // //clear receive buffers (segment only)
    // if(role == Role::SEGMENT) {
    //     for(int32_t buf_idx = 0; buf_idx < buffer_count_seg; buf_idx++) {
    //         free(recv_buffers[buf_idx]);
    //     }
    // }
    //TODO: implement new clear.
}
