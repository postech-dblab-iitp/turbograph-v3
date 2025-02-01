#pragma once

/*
 * Design of the GraphRenumberer
 *
 * This class implements vertex id renumbering step of BBP. The detailed vertex 
 * id renumbering process is as follows:
 *     1) Count the degree of each vertex
 *     2) Build the local histogram of vertex degree on each machine
 *     3) Build the global histogram of vertex degree by allreducing local
 *        histograms
 *     4) Build the mapping table that maps old vertex IDs to newly assigned
 *        vertex IDs. For vertices in each machine, new vertex IDs are assigned 
 *        in descending order of their degrees. For example,
 *        --------------------------------------
 *        Old Vertex ID | Degree | New Vertex ID
 *        --------------------------------------
 *              0       |   1    |       2
 *              1       |   10   |       0
 *              2       |   5    |       1
 *        --------------------------------------
 *     5) Renumber old vertex IDs to new vertex IDs according to the mapping
 *        table
 */

#include <atomic>
#include <vector>
#include <random>
#include <algorithm>
#include <math.h>
#include <sys/time.h>

#include "analytics/core/turbograph_implementation.hpp"
#include "analytics/core/turbo_tcp.hpp"
#include "analytics/io/PartitionedEdges.hpp"
#include "analytics/util/timer.hpp"

template <typename FileNodeType, typename degree_t>
class GraphRenumberer : public TurbographImplementation {
	public:
		GraphRenumberer() {}

		GraphRenumberer(PartitionedEdges<FileNodeType, degree_t>* edge_partitioned_, int num_chunks_, int num_threads_, int64_t total_vertices_) : num_chunks(num_chunks_)
                      , num_threads(num_threads_)
                      , total_vertices(total_vertices_)
                      , max_vid(total_vertices_) { 
            edge_partitioned = edge_partitioned_;
		}

		~GraphRenumberer() {}

		virtual void Initialize() {}
		
        virtual void OpenVectors() {
			vector_for_push.Open(RDONLY, WRONLY, 1);
			vector_for_push.OpenWindow(RDONLY, WRONLY, 1);
			register_vector(vector_for_push);
            vector_for_push.PinRandVectorWriteBufferInMemory();
            vector_for_push.AllocateMemory();
            LocalStatistics::print_mem_alloc_info();
		}

		virtual void run_(int argc, char** argv, int64_t* total_vertices_updated_, int partitioning_scheme = 0) {
            total_vertices_updated = *total_vertices_updated_; 
            switch(partitioning_scheme) {
                case 0: renumberByBBP(); break;
                case 2: renumberByRandomPartitioning(); break;
                case 3: renumberByHashPartitioning(); break;
                case 4: renumberByGraphXHashPartitioning(); break;
                default: 
                    if(PartitionStatistics::my_machine_id() == 0)  {
                        fprintf(stdout, "Invalid Partitioning Scheme!!\n");
                    }
                    throw std::runtime_error("[GraphRenumberer::run] Invalid partitioning scheme");
            }
            *total_vertices_updated_ = total_vertices_updated;
		}

		void renumberByBBP() {
            //Open and Initialize vectors
			timer.start_timer(0);
            OpenVectors();
			Initialize();
			
            PartitionStatistics::wait_for_all();
			timer.stop_timer(0);
#ifdef PRINT_LOG
			system_fprintf(0, stdout, "\tInitialization vector elapsed = %.3f\n", timer.get_timer(0));
#endif

            //Counting degree of vertices
			timer.start_timer(6);
			PartitionStatistics::wait_for_all();
#ifdef PRINT_LOG
			system_fprintf(0, stdout, "\tDegree counting Phase Start\n");
#endif 
            CountingDegree();
			PartitionStatistics::wait_for_all();

			timer.start_timer(4);
			wait_for_vector_io();
			timer.stop_timer(4);
			PartitionStatistics::wait_for_all();
#ifdef PRINT_LOG
			fprintf(stdout, "\t[%lld] [Degree counting Phase] Processed edge = %lld, elapsed time = %.3f\n", PartitionStatistics::my_machine_id(), count_processed_edge, timer.get_timer(1) + timer.get_timer(2) + timer.get_timer(3));
#endif

            //Renumbering graph
			timer.start_timer(7);
#ifdef PRINT_LOG
			if(PartitionStatistics::my_machine_id() == 0)
				fprintf(stdout, "\tRenumbering Graph Phase Start\n");
#endif

			RenumberingGraph();

			timer.stop_timer(7);
			timer.stop_timer(6);
			PartitionStatistics::wait_for_all();
			
            print_elapsed_time();
			CloseVectors();
		}
		
        void renumberByRandomPartitioning() {
			std::uniform_int_distribution<int> distribution(0, PartitionStatistics::num_machines() - 1);
			std::random_device rd;
			std::mt19937 engine(rd());

            RenumberingGraphWrapper([&](node_t vid){
                return distribution(engine) % PartitionStatistics::num_machines();
            });
		}
		
        void renumberByHashPartitioning() {
            RenumberingGraphWrapper([&](node_t vid){
                return vid % PartitionStatistics::num_machines();
            });
        }
		
        void renumberByGraphXHashPartitioning() {
			int64_t mixing_prime = 1125899906842597L;
            RenumberingGraphWrapper([&](node_t vid){
                return std::abs(vid * mixing_prime) % PartitionStatistics::num_machines();
            });
        }
		
        void CountingDegree() {
            for (auto i = 0; i < num_chunks; i++) {
                system_fprintf(0, stdout, "\n\t[%lldth chunk] Count degree and writeback\n", i);
                int64_t start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS;
                int part_id = (start_part_id + i) % num_chunks;
                int next_part_id = (part_id + 1) % num_chunks;
                int64_t src_total_edge = 0;
                int64_t dst_total_edge = 0;
                int64_t overflow = 0;

                if(next_part_id == start_part_id) next_part_id = -1;

                timer.start_timer(1);
                vector_for_push.ReadSequentialVector(start_part_id, part_id, next_part_id); 
                vector_for_push.InitializeRandomVector(start_part_id, part_id, next_part_id);
                vector_for_push.ReadRandomVector(start_part_id, part_id, next_part_id); 
                timer.stop_timer(1);

                timer.start_timer(2);
                for(auto j = 0; j < num_chunks; j++) {
                    src_total_edge += edge_partitioned->countOutDegreeAndProcess(part_id, (j + part_id) % num_chunks, vector_for_push);
                    dst_total_edge += edge_partitioned->countInDegreeAndProcess((j + part_id) % num_chunks, part_id, vector_for_push);
                }
                timer.stop_timer(2);

                count_processed_edge += src_total_edge;
                count_processed_edge += dst_total_edge;

                timer.start_timer(3);

#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
                {
                    bool of = vector_for_push.FlushUpdateBufferPerThread(start_part_id, part_id, next_part_id, -1);
                    //bool of = vector_for_push.FlushUpdateBufferPerThread(start_part_id, part_id, next_part_id, -1, -1);
                    if (of) std::atomic_fetch_add((std::atomic<int64_t>*) &overflow, 1L);
                }
                vector_for_push.FlushUpdateBuffer(start_part_id, part_id, next_part_id, 0, std::atomic_load((std::atomic<int64_t>*) &overflow));
                vector_for_push.FlushLocalGatherBuffer(start_part_id, part_id, next_part_id, src_total_edge + dst_total_edge);
                timer.stop_timer(3);
            }
		}
        
        void RenumberingGraph() {
            BuildLocalHistogramOfDegree();
            BuildOldVidtoNewVidMappingTable();
            RenumberingGraphUsingMappingTable();
        }
		
        void Update(node_t src_vid) {
			vector_for_push.Read<1>(src_vid) = vector_for_push.AccumulatedUpdate(src_vid);
			vector_for_push.ResetWriteBuffer(src_vid);
		}

        void BuildLocalHistogramOfDegree() {
			int64_t num_updated_vertices = 0;
            int64_t max_degree_ = 0;
			int64_t start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS;
            int part_id, next_part_id;
			int temp_num_threads = MIN(4, num_threads);
			std::vector<std::vector<int64_t>> per_thread_local_histogram;
            per_thread_local_histogram.resize(temp_num_threads);
            PartitionStatistics::wait_for_all();
            timer.start_timer(8);
#ifdef PRINT_LOG
            if(PartitionStatistics::my_machine_id() == 0)
                fprintf(stdout, "\tBuild local histogram of degree phase\n");
#endif

            timer.start_timer(13);
            if(!TG_DistributedVectorBase::update_delta_buffer_overflow){
                TG_DistributedVectorBase::SortAndMergeUpdatedListOfVertices();
            }

            for (int64_t chunk_id = 0; chunk_id < UserArguments::VECTOR_PARTITIONS; chunk_id++) {
                for (int64_t j = 0 ; j <  Vectors().size() ; j++) {
                    Vectors()[j]->ReadAndMergeSpilledWritesFromDisk(chunk_id);
                }
                part_id = (start_part_id + chunk_id) % num_chunks;
                next_part_id = (part_id + 1) % num_chunks;
                if(next_part_id == start_part_id) next_part_id = -1;
                vector_for_push.ReadSequentialVector(start_part_id, part_id, next_part_id);
                Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(start_part_id + chunk_id);
#pragma omp parallel for reduction(max: max_degree_) num_threads(temp_num_threads)
                for (node_t vid = vid_range.GetBegin();  vid <= vid_range.GetEnd(); vid++) {
                    Update(vid);
                    if(max_degree_ < vector_for_push.Read<1>(vid))
                        max_degree_ = vector_for_push.Read<1>(vid);
                }
            }

            max_degree = max_degree_;
            MPI_Allreduce(MPI_IN_PLACE, &max_degree, 1, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);
            for(int i = 0; i < temp_num_threads; i++) {
                per_thread_local_histogram[i].resize(max_degree + 1, 0);
            }

            for (int64_t chunk_id = 0; chunk_id < UserArguments::VECTOR_PARTITIONS; chunk_id++) {
                part_id = (start_part_id + chunk_id) % num_chunks;
                next_part_id = (part_id + 1) % num_chunks;
                if(next_part_id == start_part_id) next_part_id = -1;
                vector_for_push.ReadSequentialVector(start_part_id, part_id, next_part_id);
                Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(start_part_id + chunk_id);
#pragma omp parallel num_threads(temp_num_threads)
                {
                    int tid = omp_get_thread_num();
#pragma omp for
                    for (node_t vid = vid_range.GetBegin();  vid <= vid_range.GetEnd(); vid++) {
                        per_thread_local_histogram[tid][vector_for_push.Read<1>(vid)]++;
                    }
                }
            }
            local_histogram.resize(max_degree + 1, 0);
            for(auto i = 0; i < temp_num_threads; i++) {
                for(auto j = 0; j < max_degree + 1; j++) {
                    local_histogram[j] += per_thread_local_histogram[i][j];
                }
                per_thread_local_histogram[i].clear();
            }
            per_thread_local_histogram.clear();

            timer.stop_timer(13);
            
            vector_for_push.ReleaseGlobalGatherBuffer();
            PartitionStatistics::wait_for_all();
#ifdef PRINT_LOG
            fprintf(stdout, "\t[%lld] Max degree = %lld\n", PartitionStatistics::my_machine_id(), max_degree);
            if(PartitionStatistics::my_machine_id() == 0) {
                fprintf(stdout, "\tBuild local histogram of degree phase END, elapsed = %.3f\n", timer.get_timer(13));
            }
#endif
        }

        void BuildOldVidtoNewVidMappingTable() {
			int64_t col_size;
            MPI_Status stat;
            
#ifdef PRINT_LOG
            system_fprintf(0, stdout, "\tBuild OldVid to NewVid mapping table phase\n");
#endif
            timer.start_timer(14);
            accumulated_histogram.resize(max_degree + 1, 0);
            newvid_offset_table.resize(max_degree + 1, 0);

            PartitionStatistics::wait_for_all();
#ifdef PRINT_LOG
            system_fprintf(0, stdout, "\tBuild global histogram\n");
#endif
            //Build global histogram
            if (PartitionStatistics::num_machines() > 1) {
                if (PartitionStatistics::my_machine_id() == 0) {
                    MPI_Send((void*) &local_histogram[0], sizeof(int64_t) * (max_degree + 1), MPI_BYTE, PartitionStatistics::my_machine_id() + 1, 0, MPI_COMM_WORLD);
                    //fprintf(stdout, "\tSend from %lld to %lld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id() + 1);

                } else if (PartitionStatistics::my_machine_id() == PartitionStatistics::num_machines() - 1) {
                    MPI_Recv((void*) &accumulated_histogram[0], sizeof(int64_t) * (max_degree + 1), MPI_BYTE, PartitionStatistics::my_machine_id() - 1, 0, MPI_COMM_WORLD, &stat);
                    //fprintf(stdout, "\tRecv from %lld\n", PartitionStatistics::my_machine_id() - 1);
                    for(auto i = 0; i < max_degree + 1; i++) {
                        local_histogram[i] += accumulated_histogram[i];
                    }
                    int64_t temp = 0;
                    for(auto i = max_degree; i>= 0; i--) {
                        newvid_offset_table[i] = temp;
                        temp += local_histogram[i];
                    }
                } else {
                    MPI_Recv((void*) &accumulated_histogram[0], sizeof(int64_t) * (max_degree + 1), MPI_BYTE, PartitionStatistics::my_machine_id() - 1, 0, MPI_COMM_WORLD, &stat);
                    //fprintf(stdout, "\tRecv from %lld\n", PartitionStatistics::my_machine_id() - 1);
                    for(auto i = 0; i < max_degree + 1; i++) {
                        local_histogram[i] += accumulated_histogram[i];
                    }
                    MPI_Send((void*) &local_histogram[0], sizeof(int64_t) * (max_degree + 1), MPI_BYTE, PartitionStatistics::my_machine_id() + 1, 0, MPI_COMM_WORLD);
                    //fprintf(stdout, "\tSend from %lld to %lld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id() + 1);
                }
                MPI_Bcast((void*) &newvid_offset_table[0], sizeof(int64_t) * (max_degree + 1), MPI_BYTE, PartitionStatistics::num_machines() - 1, MPI_COMM_WORLD);
                MPI_Bcast((void*) &local_histogram[0], sizeof(int64_t), MPI_BYTE, PartitionStatistics::num_machines() - 1, MPI_COMM_WORLD);
            } else {
                int64_t temp = 0;
                for(auto i = max_degree; i>= 0; i--) {
                    newvid_offset_table[i] = temp;
                    temp += local_histogram[i];
                }
            }

            for(auto i = 0; i < max_degree + 1; i++) {
                newvid_offset_table[i] += accumulated_histogram[i];
            }
            accumulated_histogram.clear();

            PartitionStatistics::wait_for_all();
            timer.stop_timer(14);
            timer.stop_timer(8);

            timer.start_timer(10);
            int64_t zerovertices = local_histogram[0];
            int64_t real_vertices = total_vertices - zerovertices;
            col_size = ((real_vertices + PartitionStatistics::num_machines() - 1) / PartitionStatistics::num_machines());
            total_vertices_updated = col_size * PartitionStatistics::num_machines();

            local_histogram.clear();

			vector_for_pull.Open(RDONLY, RDONLY, 1);
			vector_for_pull.OpenWindow(RDONLY, RDONLY, 1);
            vector_for_pull.AllocateMemory();
            LocalStatistics::print_mem_alloc_info();
			PartitionStatistics::wait_for_all();
			
            //TODO dump mapping table async
            //Build OldtoNewVid Mapping
			int64_t start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS;
            int part_id, next_part_id;
            for (int64_t chunk_id = 0; chunk_id < UserArguments::VECTOR_PARTITIONS; chunk_id++) {
#ifdef PRINT_LOG
                system_fprintf(0, stdout, "\tChunk idx = %lld\n", chunk_id);
#endif
                PartitionStatistics::wait_for_all();
                part_id = (start_part_id + chunk_id) % num_chunks;
                next_part_id = (part_id + 1) % num_chunks;
                if(next_part_id == start_part_id) next_part_id = -1;
                vector_for_push.ReadSequentialVector(start_part_id, part_id, next_part_id);
                vector_for_pull.ReadSequentialVector(start_part_id, part_id, next_part_id);
                Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + chunk_id);

#ifdef PRINT_LOG
                fprintf(stdout, "\t[%lld] Range start = %lld, end = %lld\n", PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + chunk_id, vid_range.GetBegin(), vid_range.GetEnd());
#endif

                int64_t idx, order;
                for (node_t vid = vid_range.GetBegin();  vid <= vid_range.GetEnd(); vid++) {	
                    idx = vector_for_push.Read<1>(vid); //idx = degree value
                    order = newvid_offset_table[idx];
                    if(idx > 0) {
                        vector_for_pull.Read<1>(vid) = (order / PartitionStatistics::num_machines()) + col_size * (order % PartitionStatistics::num_machines());
                        INVARIANT(vector_for_pull.Read<1>(vid) >= 0 && vector_for_pull.Read<1>(vid) < total_vertices_updated);
                    } else {
                        vector_for_pull.Read<1>(vid) = -1;
                    }
                    newvid_offset_table[idx]++;
                }
            }
            
            timer.stop_timer(10);
            newvid_offset_table.clear();
			CloseVectors();
			clear_vector_list();
#ifdef PRINT_LOG
            system_fprintf(0, stdout, "\tBuild OldVid to NewVid mapping table phase END, elapsed = %.3f\n", timer.get_timer(10) + timer.get_timer(14));
#endif
        }

        void RenumberingGraphUsingMappingTable() {
			register_vector(vector_for_pull);
			
            LocalStatistics::print_mem_alloc_info();
			PartitionStatistics::wait_for_all();

#ifdef PRINT_LOG
			system_fprintf(0, stdout, "BEGIN\tStart renumbering\n");
#endif
			PartitionStatistics::wait_for_all();
			timer.start_timer(9);
			//Start renumbering

			turbo_timer relabel_timer;
			turbo_timer relabel_superstep_timer;
			for(int i = 0; i < UserArguments::VECTOR_PARTITIONS; i++) {
				relabel_timer.reset_timer(0);
				relabel_timer.reset_timer(1);
				relabel_timer.reset_timer(2);
				relabel_timer.start_timer(0);
				if(i % 2 == 0) {
#ifdef PRINT_LOG
					system_fprintf(0, stdout, "\tRenumbering superstep %lld start\n", i / 2);
#endif
					relabel_superstep_timer.reset_timer(0);
					relabel_superstep_timer.start_timer(0);
					if(i == UserArguments::VECTOR_PARTITIONS - 1)
						vector_for_pull.lockSequentialVector(i, i);
					else
						vector_for_pull.lockSequentialVector(i, i + 1);
					PartitionStatistics::wait_for_all();
				}
				int64_t start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + i; 
				for(int j = 0; j < PartitionStatistics::num_machines(); j++) {
					int part_id = (start_part_id + j * UserArguments::VECTOR_PARTITIONS) % num_chunks;
					int next_part_id = (part_id + UserArguments::VECTOR_PARTITIONS) % num_chunks;
                    if(next_part_id == start_part_id) next_part_id = -1;

					timer.start_timer(11);
					relabel_timer.start_timer(1);
					
					vector_for_pull.InitializeRandomVector(start_part_id, part_id, next_part_id);
					vector_for_pull.ReadRandomVector(start_part_id, part_id, next_part_id);
					
                    timer.stop_timer(11);
					relabel_timer.stop_timer(1);
					timer.start_timer(12);
					relabel_timer.start_timer(2);
                    
                    edge_partitioned->relabelingEdges_byVector2(num_threads, part_id, vector_for_pull);
				
                    timer.stop_timer(12);
					relabel_timer.stop_timer(2);
				}
				relabel_timer.stop_timer(0);
				//fprintf(stdout, "\t[%lld] Relabeling superstep %lld end, elapsed time = %.3f, readrandom elapsed = %.3f, respondvectorpull(send) elapsed = %.3f, requestvectorpull(recv) elapsed = %.3f, relabel elapsed = %.3f, diskread elapsed = %.3f, diskwrite elapsed = %.3f\n", PartitionStatistics::my_machine_id(), i, relabel_timer.get_timer(0), relabel_timer.get_timer(1), vector_for_pull.tslee_timer.get_timer(1), vector_for_pull.tslee_timer.get_timer(2), relabel_timer.get_timer(2), edge_partitioned->total_read_timer.get_timer(0), edge_partitioned->total_write_timer.get_timer(0));
                
                //TODO remove useless timer
                edge_partitioned->total_read_timer.reset_timer(0);
				edge_partitioned->total_write_timer.reset_timer(0);
				vector_for_pull.tslee_timer.reset_timer(1);
				vector_for_pull.tslee_timer.reset_timer(2);
				if(i % 2 == 1 || (i % 2 == 0 && i == UserArguments::VECTOR_PARTITIONS - 1)) {
					PartitionStatistics::wait_for_all();
					vector_for_pull.unlockSequentialVector();
					relabel_superstep_timer.stop_timer(0);
					system_fprintf(0, stdout, "\tRenumbering superstep %lld end, elapsed time = %.3f\n", i / 2, relabel_superstep_timer.get_timer(0));
				}
			}

			fprintf(stdout, "END\t[%lld] End renumbering, elapsed time = %.3f\n", PartitionStatistics::my_machine_id(), timer.stop_timer(9));
			timer.start_timer(9);
			PartitionStatistics::wait_for_all();
			timer.stop_timer(9);
		}


        void RenumberingGraphWrapper(std::function<int(node_t)> partition_func) {
			int64_t max_partition_count;
			
			partition_histogram.resize(PartitionStatistics::num_machines(), 0);
			accumulated_histogram.resize(PartitionStatistics::num_machines(), 0);
			newvid_offset_table.resize(PartitionStatistics::num_machines(), 0);

			int64_t start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS;
            int part_id, next_part_id;
			vector_for_pull.Open(RDONLY, RDONLY, 1);
			vector_for_pull.OpenWindow(RDONLY, RDONLY, 1);
            vector_for_pull.AllocateMemory();
			register_vector(vector_for_pull);
			PartitionStatistics::wait_for_all();
			
			for (int64_t chunk_id = 0; chunk_id < UserArguments::VECTOR_PARTITIONS; chunk_id++) {
                part_id = (start_part_id + chunk_id) % num_chunks;
                next_part_id = (part_id + 1) % num_chunks;
                if (next_part_id == start_part_id) next_part_id = -1;
				vector_for_pull.ReadSequentialVector(start_part_id, part_id, next_part_id);
				Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + chunk_id);
				for (node_t vid = vid_range.GetBegin(); vid <= vid_range.GetEnd(); vid++) {
					vector_for_pull.Read(vid) = partition_func(vid) ; //Random function
					partition_histogram.at(vector_for_pull.Read(vid)) += 1;
				}
			}
			PartitionStatistics::wait_for_all();
			
			MPI_Status stat;

            if (PartitionStatistics::num_machines() > 1) {
                if (PartitionStatistics::my_machine_id() == 0) {
                    MPI_Send((void*) &partition_histogram[0], sizeof(int64_t) * PartitionStatistics::num_machines(), MPI_BYTE, PartitionStatistics::my_machine_id() + 1, 0, MPI_COMM_WORLD);
                    fprintf(stdout, "\tSend from %lld to %lld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id() + 1);

                } else if (PartitionStatistics::my_machine_id() == PartitionStatistics::num_machines() - 1) {
                    MPI_Recv((void*) &accumulated_histogram[0], sizeof(int64_t) * PartitionStatistics::num_machines(), MPI_BYTE, PartitionStatistics::my_machine_id() - 1, 0, MPI_COMM_WORLD, &stat);
                    fprintf(stdout, "\tRecv from %lld\n", PartitionStatistics::my_machine_id() - 1);
                    for (auto i = 0; i < PartitionStatistics::num_machines(); i++) {
                        partition_histogram[i] += accumulated_histogram[i];
                    }
                    int64_t temp = 0;

                    max_partition_count = *std::max_element(partition_histogram.begin(), partition_histogram.end());
                    fprintf(stdout, "\tMax partition count = %lld\n", max_partition_count);
                    for (int i = 0; i < PartitionStatistics::num_machines(); i++) {
                        newvid_offset_table[i] = i * max_partition_count;
                    }
                } else {
                    MPI_Recv((void*) &accumulated_histogram[0], sizeof(int64_t) * PartitionStatistics::num_machines(), MPI_BYTE, PartitionStatistics::my_machine_id() - 1, 0, MPI_COMM_WORLD, &stat);
                    fprintf(stdout, "\tRecv from %lld\n", PartitionStatistics::my_machine_id() - 1);
                    for(auto i = 0; i < PartitionStatistics::num_machines(); i++) {
                        partition_histogram[i] += accumulated_histogram[i];
                    }
                    MPI_Send((void*) &partition_histogram[0], sizeof(int64_t) * PartitionStatistics::num_machines(), MPI_BYTE, PartitionStatistics::my_machine_id() + 1, 0, MPI_COMM_WORLD);
                    fprintf(stdout, "\tSend from %lld to %lld\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id() + 1);
                }
                MPI_Bcast((void*) &newvid_offset_table[0], sizeof(int64_t) * PartitionStatistics::num_machines(), MPI_BYTE, PartitionStatistics::num_machines() - 1, MPI_COMM_WORLD);
                MPI_Bcast((void*) &max_partition_count, sizeof(int64_t), MPI_BYTE, PartitionStatistics::num_machines() - 1, MPI_COMM_WORLD);
            } else {
                max_partition_count = *std::max_element(partition_histogram.begin(), partition_histogram.end());
                //fprintf(stdout, "\tMax partition count = %lld\n", max_partition_count);
                for (int i = 0; i < PartitionStatistics::num_machines(); i++) {
                    newvid_offset_table[i] = i * max_partition_count;
                }
            }

			for(int64_t i = 0; i < PartitionStatistics::num_machines(); i++) {
				newvid_offset_table[i] += accumulated_histogram[i];
			}
			
			for(int64_t chunk_id = 0; chunk_id < UserArguments::VECTOR_PARTITIONS; chunk_id++) {
                part_id = (start_part_id + chunk_id) % num_chunks;
                next_part_id = (part_id + 1) % num_chunks;
                if(next_part_id == start_part_id) next_part_id = -1;
				vector_for_pull.ReadSequentialVector(start_part_id, part_id, next_part_id);
				Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + chunk_id);
				for (node_t vid = vid_range.GetBegin();  vid <= vid_range.GetEnd(); vid++) {
					int part_num = vector_for_pull.Read(vid);
					ALWAYS_ASSERT(part_num >= 0 && part_num < PartitionStatistics::num_machines());
					vector_for_pull.Read(vid) = newvid_offset_table[part_num]++;
				}
				std::shuffle(&vector_for_pull.Read(vid_range.GetBegin()), &vector_for_pull.Read(vid_range.GetEnd()), std::default_random_engine(0));
			}

			for(int i = 0; i < UserArguments::VECTOR_PARTITIONS; i++) {
				if(i % 2 == 0) {
					system_fprintf(0, stdout, "\tRelabeling superstep %lld start\n", i / 2);
					if(i == UserArguments::VECTOR_PARTITIONS - 1)
						vector_for_pull.lockSequentialVector(i, i);
					else
						vector_for_pull.lockSequentialVector(i, i + 1);
					PartitionStatistics::wait_for_all();
				}

				start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + i; 
				for(int j = 0; j < PartitionStatistics::num_machines(); j++) {
					int part_id = (start_part_id + j * UserArguments::VECTOR_PARTITIONS) % num_chunks;
					int next_part_id = (part_id + UserArguments::VECTOR_PARTITIONS) % num_chunks;
                    if(next_part_id == start_part_id) next_part_id = -1;

					vector_for_pull.InitializeRandomVector(start_part_id, part_id, next_part_id);
					vector_for_pull.ReadRandomVector(start_part_id, part_id, next_part_id);

					edge_partitioned->relabelingEdges_byVector2(num_threads, part_id, vector_for_pull);
				}
				if(i % 2 == 1 || (i % 2 == 0 && i == UserArguments::VECTOR_PARTITIONS - 1)) {
					PartitionStatistics::wait_for_all();
					vector_for_pull.unlockSequentialVector();
					system_fprintf(0, stdout, "\tRelabeling superstep %lld end\n", i / 2);
				}
				
			}
			total_vertices_updated = max_partition_count * PartitionStatistics::num_machines(); 
			fprintf(stdout, "END\t[%lld] End relabeling\n", PartitionStatistics::my_machine_id());
			PartitionStatistics::wait_for_all();
        }

		void init() {}
		virtual void Superstep(int argc, char** argv) {}

		void bijective_test(int64_t num_total_vertices) {
			std::vector<uint64_t> bit_vector;
			bit_vector.resize((total_vertices + 63) / 64, 0);
			int64_t relabeled_vertices = 0;
			int64_t zero_vertices = 0;
			int x = 5;

			fprintf(stdout, "BEGIN\tVector pull test whether Old vid to New vid mapping is bijective\n");
			int64_t start_part_id = PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS;
			int64_t part_id = 0;
			for(part_id = 0; part_id < num_chunks; part_id++) {
				int next_part_id = (part_id + UserArguments::VECTOR_PARTITIONS) % num_chunks;
                if(next_part_id == start_part_id) next_part_id = -1;
				vector_for_pull.InitializeRandomVector(start_part_id, part_id, next_part_id);
				vector_for_pull.ReadRandomVector(start_part_id, part_id, next_part_id);
				fprintf(stdout, "\tVector pull from machine %lld, %lldth chunk\n", part_id/UserArguments::VECTOR_PARTITIONS, part_id % UserArguments::VECTOR_PARTITIONS);

				Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(part_id);

				for (node_t vid = vid_range.GetBegin();  vid <= vid_range.GetEnd(); vid++) {
					int64_t idx;	
					vector_for_pull.Read<1>(vid, idx);
					if(idx != -1) {
						ALWAYS_ASSERT(idx < num_total_vertices && idx >= 0); 
						if(idx >= num_total_vertices || idx < 0)
							fprintf(stdout, "[%lld] wrong mapping = %lld\n", PartitionStatistics::my_machine_id(), idx);
						int64_t idx2 = idx / 64;
						uint64_t bit_block = bit_vector[idx2];
						uint64_t shift = 63 - (idx - (64 * idx2));
						uint64_t mask = 1L << shift;
						if((bit_block & mask) == 0) {
							bit_vector[idx2] = bit_block | mask;
							relabeled_vertices++;
						} else {
							fprintf(stdout, "\tDuplicate index : %lld at %lld\n", idx, vid);
							throw unsupported_exception();
						}
					}
					else {
						zero_vertices++;
					}
				}
			}

			fprintf(stdout, "END\tVector pull test End, Total relabeled vertices = %lld, Total zero vertices = %lld\n", relabeled_vertices, zero_vertices);

		}

		void print_elapsed_time() {
#ifdef PRINT_LOG
			if(PartitionStatistics::my_machine_id() == 0) {
				fprintf(stdout, "\t[%lld] Read vector elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(1));
				fprintf(stdout, "\t[%lld] Count degree elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(2));
				//		fprintf(stdout, "\t\t[%lld] Mmap elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(7));
				//		fprintf(stdout, "\t\t[%lld] Write vector elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(8));
				fprintf(stdout, "\t[%lld] Writeback vector elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(3));
				fprintf(stdout, "\t[%lld] Wait vector io elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(4));
				fprintf(stdout, "\t[%lld] RenumberingGraph - make local histogram elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(13));
				fprintf(stdout, "\t[%lld] RenumberingGraph - make vid offset table elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(14));
				fprintf(stdout, "\t[%lld] RenumberingGraph - make local histogram and vid offset table total elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(8));
				fprintf(stdout, "\t[%lld] RenumberingGraph - make oldtonew vid mapping table elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(10));
				fprintf(stdout, "\t[%lld] RenumberingGraph - relabeling total elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(9));
				fprintf(stdout, "\t[%lld] RenumberingGraph elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(7));
				fprintf(stdout, "\t[%lld] Vector reduce total elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(6));
			}
			fprintf(stdout, "\t[%lld] RenumberingGraph - relabeling readrandom elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(11));
			fprintf(stdout, "\t[%lld] RenumberingGraph - relabeling readrandom in lock elapsed = %.3f\n", PartitionStatistics::my_machine_id(), vector_for_pull.tslee_timer.get_timer(0));
			fprintf(stdout, "\t[%lld] RenumberingGraph - relabeling by vector elapsed = %.3f\n", PartitionStatistics::my_machine_id(), timer.get_timer(12));
#endif
		}

	private:
		TG_DistributedVector<degree_t, PLUS> vector_for_push;
		TG_DistributedVector<node_t, PLUS> vector_for_pull;
		int num_chunks;
		PartitionedEdges<FileNodeType, degree_t>* edge_partitioned;
		std::vector<int64_t> local_histogram;
        std::vector<int64_t> partition_histogram;
		std::vector<int64_t> accumulated_histogram;
		std::vector<int64_t> newvid_offset_table;
		turbo_timer timer;
		int num_threads;
		int64_t total_vertices;
		int64_t max_vid;
		int64_t count_processed_edge = 0;
        int total_vertices_updated;
		int64_t max_degree = 0;
};
